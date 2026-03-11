//! DataFusion-powered SQL transform execution.
//!
//! For each incoming Arrow batch:
//! 1. Register it as a DataFusion MemTable named after the current stream
//! 2. Re-plan the cached SQL statement against the current stream-named table
//! 3. Forward result batches downstream via `ctx.emit_batch()`

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use futures::StreamExt;
use rapidbyte_sdk::prelude::*;

use crate::config::Config;

/// Run the SQL transform for a single stream.
///
/// # Errors
///
/// Returns `Err` if SQL execution fails or batch emission encounters an error.
pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    _config: &Config,
    statement: &Statement,
) -> Result<TransformSummary, PluginError> {
    let session = SessionContext::new();
    let stream_name = ctx.stream_name();

    let mut records_in: u64 = 0;
    let mut records_out: u64 = 0;
    let mut bytes_in: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut batches_processed: u64 = 0;

    while let Some((schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            continue;
        }

        let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        records_in += batch_rows;

        let batch_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        bytes_in += batch_bytes;

        // Register as MemTable named after the current stream.
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(
            |e| PluginError::internal("SQL_MEMTABLE", format!("Failed to create MemTable: {e}")),
        )?;

        // Deregister previous table (no-op on first iteration).
        let _ = session.deregister_table(stream_name);
        session
            .register_table(stream_name, Arc::new(mem_table))
            .map_err(|e| {
                PluginError::internal(
                    "SQL_REGISTER",
                    format!("Failed to register table: {e}"),
                )
            })?;

        // Re-plan against the current stream table and stream results instead of buffering them.
        let logical_plan = session
            .state()
            .statement_to_plan(statement.clone())
            .await
            .map_err(|e| PluginError::internal("SQL_PLAN", format!("Query planning failed: {e}")))?;
        let df = DataFrame::new(session.state(), logical_plan);
        let mut result_stream = df.execute_stream().await.map_err(|e| {
            PluginError::internal("SQL_EXEC", format!("Query execution failed: {e}"))
        })?;

        while let Some(batch) = result_stream.next().await {
            let batch = batch.map_err(|e| {
                PluginError::internal("SQL_EXEC", format!("Query execution failed: {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            records_out += batch.num_rows() as u64;
            bytes_out += batch.get_array_memory_size() as u64;
            ctx.emit_batch(&batch)?;
        }

        batches_processed += 1;
    }

    ctx.log(
        LogLevel::Info,
        &format!(
            "SQL transform complete: {records_in} rows in, {records_out} rows out, {batches_processed} batches"
        ),
    );

    Ok(TransformSummary {
        records_in,
        records_out,
        bytes_in,
        bytes_out,
        batches_processed,
    })
}
