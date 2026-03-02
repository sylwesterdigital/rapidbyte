//! Change Data Capture via `PostgreSQL` logical replication slots.
//!
//! Reads WAL changes through `pg_logical_slot_get_binary_changes` with the
//! `pgoutput` plugin, decodes binary messages, emits typed Arrow IPC batches,
//! and checkpoints by LSN.

pub(crate) mod encode;
pub(crate) mod pgoutput;

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::config::Config;
use crate::metrics::{emit_read_metrics, EmitState, BATCH_SIZE};

use encode::{encode_cdc_batch, CdcRow, RelationInfo};
use pgoutput::{CdcOp, PgOutputMessage, TupleData};

/// Maximum WAL changes consumed per CDC invocation to avoid unbounded memory use.
/// Type is i32 because `pg_logical_slot_get_binary_changes()` expects int4.
const CDC_MAX_CHANGES_DEFAULT: i32 = 10_000;

/// Default replication slot prefix. Full slot names are `rapidbyte_{stream_name}`.
const SLOT_PREFIX: &str = "rapidbyte_";

/// Default publication prefix. Full publication names are `rapidbyte_{stream_name}`.
const PUB_PREFIX: &str = "rapidbyte_";

/// Read max changes per CDC query from env, with sane defaults and validation.
fn cdc_max_changes() -> i32 {
    static CACHED: OnceLock<i32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("RAPIDBYTE_CDC_MAX_CHANGES")
            .ok()
            .and_then(|raw| raw.trim().parse::<i32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(CDC_MAX_CHANGES_DEFAULT)
    })
}

fn emit_batch(
    rows: &mut Vec<CdcRow>,
    relation: &RelationInfo,
    ctx: &Context,
    state: &mut EmitState,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = encode_cdc_batch(rows, relation)?;
    // Safety: encode timing in nanos will not exceed u64::MAX for any realistic duration.
    #[allow(clippy::cast_possible_truncation)]
    {
        state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
    }

    state.total_records += rows.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    ctx.emit_batch(&batch)
        .map_err(|e| format!("emit_batch failed: {}", e.message))?;
    emit_read_metrics(ctx, state.total_records, state.total_bytes);

    rows.clear();
    Ok(())
}

/// Read CDC changes from a logical replication slot using `pg_logical_slot_get_binary_changes()`.
#[allow(clippy::too_many_lines)]
pub async fn read_cdc_changes(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    config: &Config,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    ctx.log(
        LogLevel::Info,
        &format!("CDC reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // 1. Derive slot name
    let slot_name = config
        .replication_slot
        .clone()
        .unwrap_or_else(|| format!("{SLOT_PREFIX}{}", stream.stream_name));

    // 2. Derive publication name
    let publication_name = config
        .publication
        .clone()
        .unwrap_or_else(|| format!("{PUB_PREFIX}{}", stream.stream_name));

    // 3. Ensure replication slot exists (idempotent)
    ensure_replication_slot(client, ctx, &slot_name).await?;

    // 4. Read binary changes from the slot (this CONSUMES them)
    // Uses pgoutput plugin with proto_version 1 and the configured publication.
    let changes_query = "SELECT lsn::text, data \
                         FROM pg_logical_slot_get_binary_changes(\
                             $1, NULL, $2, \
                             'proto_version', '1', \
                             'publication_names', $3\
                         )";
    let max_changes = cdc_max_changes();
    let change_rows = client
        .query(changes_query, &[&slot_name, &max_changes, &publication_name])
        .await
        .map_err(|e| {
            format!(
                "pg_logical_slot_get_binary_changes failed for slot '{slot_name}' \
                 with publication '{publication_name}'. Ensure the publication exists \
                 (CREATE PUBLICATION {publication_name} FOR TABLE ...): {e}"
            )
        })?;

    let query_secs = query_start.elapsed().as_secs_f64();

    let fetch_start = Instant::now();
    let mut state = EmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };
    let mut max_lsn: Option<u64> = None;

    // 5. Decode messages, filter to our table, accumulate into batches
    let mut relations: HashMap<u32, RelationInfo> = HashMap::new();
    let mut target_oid: Option<u32> = None;
    let mut accumulated_rows: Vec<CdcRow> = Vec::new();

    for row in &change_rows {
        let lsn_str: String = row.get(0);
        let data: &[u8] = row.get(1);

        // Track max LSN using numeric comparison
        if let Some(lsn) = pgoutput::parse_lsn(&lsn_str) {
            if max_lsn.is_none_or(|current| lsn > current) {
                max_lsn = Some(lsn);
            }
        }

        // Decode binary pgoutput message
        let msg = match pgoutput::decode(data) {
            Ok(m) => m,
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("pgoutput decode error (skipping): {e}"),
                );
                continue;
            }
        };

        match msg {
            PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                columns,
                ..
            } => {
                // Track whether this relation matches the target stream.
                // Compare both unqualified name and schema-qualified name to
                // handle publications that span multiple schemas.
                if name == stream.stream_name || format!("{namespace}.{name}") == stream.stream_name
                {
                    target_oid = Some(oid);
                }
                let info = RelationInfo::new(oid, namespace, name, columns);
                relations.insert(oid, info);
            }
            PgOutputMessage::Insert {
                relation_oid,
                new_tuple,
            } if target_oid == Some(relation_oid) => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Insert,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Update {
                relation_oid,
                new_tuple,
                ..
            } if target_oid == Some(relation_oid) => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Update,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Delete {
                relation_oid,
                key_tuple,
                old_tuple,
            } if target_oid == Some(relation_oid) => {
                let tuple = old_tuple
                    .or(key_tuple)
                    .unwrap_or_else(|| TupleData { columns: vec![] });
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Delete,
                    tuple,
                });
            }
            // Begin, Commit, Truncate, Origin, Type, Message, and non-matching DML — skip
            _ => {}
        }

        // Flush batch if accumulated enough
        if accumulated_rows.len() >= BATCH_SIZE {
            let rel = target_oid
                .and_then(|oid| relations.get(&oid))
                .ok_or_else(|| {
                    "CDC batch ready but no Relation message received for target stream".to_string()
                })?;
            emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
        }
    }

    // Flush remaining rows
    if !accumulated_rows.is_empty() {
        let rel = target_oid
            .and_then(|oid| relations.get(&oid))
            .ok_or_else(|| {
                "CDC rows accumulated but no Relation message received for target stream"
                    .to_string()
            })?;
        emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // 6. Emit checkpoint with max LSN
    let checkpoint_count = if let Some(lsn) = max_lsn {
        let lsn_string = pgoutput::lsn_to_string(lsn);
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: stream.stream_name.clone(),
            cursor_field: Some("lsn".to_string()),
            cursor_value: Some(CursorValue::Lsn {
                value: lsn_string.clone(),
            }),
            records_processed: state.total_records,
            bytes_processed: state.total_bytes,
        };
        // CDC uses get_binary_changes (destructive) so checkpoint MUST succeed to avoid data loss.
        ctx.checkpoint(&cp).map_err(|e| {
            format!(
                "CDC checkpoint failed (WAL already consumed): {}",
                e.message
            )
        })?;
        ctx.log(
            LogLevel::Info,
            &format!(
                "CDC checkpoint: stream={} lsn={}",
                stream.stream_name, lsn_string
            ),
        );
        1u64
    } else {
        ctx.log(
            LogLevel::Info,
            &format!("CDC: no new changes for stream '{}'", stream.stream_name),
        );
        0u64
    };

    ctx.log(
        LogLevel::Info,
        &format!(
            "CDC stream '{}' complete: {} records, {} bytes, {} batches",
            stream.stream_name, state.total_records, state.total_bytes, state.batches_emitted
        ),
    );

    // Safety: nanosecond timing precision loss beyond 52 bits is acceptable for metrics.
    #[allow(clippy::cast_precision_loss)]
    let arrow_encode_secs = state.arrow_encode_nanos as f64 / 1e9;

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped: 0,
        perf: Some(ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs,
        }),
    })
}

/// Ensure the logical replication slot exists, creating it if necessary.
/// Uses try-create to avoid TOCTOU race between check and create.
async fn ensure_replication_slot(
    client: &Client,
    ctx: &Context,
    slot_name: &str,
) -> Result<(), String> {
    ctx.log(
        LogLevel::Debug,
        &format!("Ensuring replication slot '{slot_name}' exists"),
    );

    // Try to create; if it already exists, PG raises duplicate_object (42710).
    let result = client
        .query_one(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot_name],
        )
        .await;

    match result {
        Ok(_) => {
            ctx.log(
                LogLevel::Info,
                &format!("Created replication slot '{slot_name}' with pgoutput"),
            );
        }
        Err(e) => {
            // Check for duplicate_object error (SQLSTATE 42710)
            let is_duplicate = e
                .as_db_error()
                .is_some_and(|db| db.code().code() == "42710");

            if is_duplicate {
                ctx.log(
                    LogLevel::Debug,
                    &format!("Replication slot '{slot_name}' already exists"),
                );
            } else {
                return Err(format!(
                    "Failed to create logical replication slot '{slot_name}'. \
                     Ensure wal_level=logical in postgresql.conf: {e}"
                ));
            }
        }
    }

    Ok(())
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::encode::CdcRow;
    use super::encode::RelationInfo;
    use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};
    use rapidbyte_sdk::arrow::datatypes::DataType;

    #[test]
    fn relation_info_schema_includes_rb_op() {
        let rel = RelationInfo::new(
            16385,
            "public".to_string(),
            "users".to_string(),
            vec![
                ColumnDef {
                    flags: 1,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "name".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        );

        let schema = &rel.arrow_schema;
        // 2 data columns + _rb_op
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(schema.field(2).name(), "_rb_op");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn cdc_row_with_insert_op() {
        let row = CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![
                    ColumnValue::Text("42".to_string()),
                    ColumnValue::Text("Alice".to_string()),
                ],
            },
        };
        assert_eq!(row.op.as_str(), "insert");
        assert_eq!(row.tuple.columns.len(), 2);
    }

    #[test]
    fn delete_row_with_empty_tuple() {
        // Simulates a DELETE where neither old_tuple nor key_tuple is available.
        let tuple = TupleData { columns: vec![] };
        let row = CdcRow {
            op: CdcOp::Delete,
            tuple,
        };
        assert_eq!(row.op.as_str(), "delete");
        assert!(row.tuple.columns.is_empty());
    }

    #[test]
    fn publication_name_default() {
        let prefix = super::PUB_PREFIX;
        let pub_name = format!("{prefix}{}", "users");
        assert_eq!(pub_name, "rapidbyte_users");
    }

    #[test]
    fn slot_name_default() {
        let prefix = super::SLOT_PREFIX;
        let slot_name = format!("{prefix}{}", "orders");
        assert_eq!(slot_name, "rapidbyte_orders");
    }
}
