//! COPY FROM STDIN write path.
//!
//! Streams Arrow `RecordBatch` data to `PostgreSQL` via the COPY binary protocol.
//! Flushes at configurable byte thresholds to bound memory usage.

use bytes::Bytes;
use futures_util::SinkExt;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::decode::{write_binary_field, WriteTarget};
use crate::types::downcast_columns;

/// Default COPY flush buffer size (4 MB).
const DEFAULT_FLUSH_BYTES: usize = 4 * 1024 * 1024;

/// `PostgreSQL` binary COPY signature (11 bytes).
const PGCOPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\x00";
/// Flags field (4 bytes): no OIDs.
const PGCOPY_FLAGS: [u8; 4] = 0_i32.to_be_bytes();
/// Header extension area length (4 bytes): none.
const PGCOPY_EXT_LEN: [u8; 4] = 0_i32.to_be_bytes();
/// File trailer: field count = -1 signals end of data.
const PGCOPY_TRAILER: [u8; 2] = (-1_i16).to_be_bytes();

/// Write batches via COPY FROM STDIN. Returns `(rows_written, bytes_sent)`.
///
/// Parameters are all pre-computed by the session layer:
/// - `target`: pre-computed column metadata (table, active columns, schema, type-null flags)
/// - `flush_bytes`: optional flush threshold override
pub(crate) async fn write(
    ctx: &Context,
    client: &Client,
    target: &WriteTarget<'_>,
    batches: &[RecordBatch],
    flush_bytes: Option<usize>,
) -> Result<(u64, u64), String> {
    if batches.is_empty() || target.active_cols.is_empty() {
        return Ok((0, 0));
    }

    let col_list = target.quoted_col_list();
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
        target.table, col_list
    );

    let sink = client
        .copy_in(&copy_stmt)
        .await
        .map_err(|e| format!("COPY start failed: {e}"))?;
    let mut sink = Box::pin(sink);

    let flush_threshold = flush_bytes.unwrap_or(DEFAULT_FLUSH_BYTES).max(1);
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut buf = Vec::with_capacity(flush_threshold);

    // Write binary header (19 bytes)
    buf.extend_from_slice(&PGCOPY_SIGNATURE);
    buf.extend_from_slice(&PGCOPY_FLAGS);
    buf.extend_from_slice(&PGCOPY_EXT_LEN);

    // Safety: PG COPY binary tuple header uses i16 for field count;
    // tables with >32,767 columns are not supported.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let num_fields = target.active_cols.len() as i16;

    for batch in batches {
        let typed_cols = downcast_columns(batch, target.active_cols)?;

        for row_idx in 0..batch.num_rows() {
            // Tuple header: field count
            buf.extend_from_slice(&num_fields.to_be_bytes());

            // Fields
            for (pos, typed_col) in typed_cols.iter().enumerate() {
                if target.type_null_flags[pos] {
                    buf.extend_from_slice(&(-1_i32).to_be_bytes());
                } else {
                    write_binary_field(&mut buf, typed_col, row_idx);
                }
            }

            total_rows += 1;

            if buf.len() >= flush_threshold {
                total_bytes += buf.len() as u64;
                sink.send(Bytes::from(std::mem::take(&mut buf)))
                    .await
                    .map_err(|e| format!("COPY send failed: {e}"))?;
                buf = Vec::with_capacity(flush_threshold);
            }
        }
    }

    // Write trailer
    buf.extend_from_slice(&PGCOPY_TRAILER);

    // Send final buffer (always non-empty due to trailer)
    total_bytes += buf.len() as u64;
    sink.send(Bytes::from(buf))
        .await
        .map_err(|e| format!("COPY send failed: {e}"))?;

    let _rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| format!("COPY finish failed: {e}"))?;

    ctx.log(
        LogLevel::Info,
        &format!(
            "dest-postgres: COPY wrote {} rows ({} bytes) to {}",
            total_rows, total_bytes, target.table
        ),
    );

    Ok((total_rows, total_bytes))
}
