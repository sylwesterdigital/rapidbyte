//! Full-refresh and incremental stream reading.
//!
//! Orchestrates: schema resolution -> query building -> server-side cursor ->
//! fetch loop -> Arrow batch emission -> checkpoint.

use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDateTime;
use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::{DataErrorPolicy, PartitionStrategy};

use crate::cursor::CursorTracker;
use crate::encode;
use crate::metrics::{emit_read_metrics, emit_read_perf_metrics, EmitState, BATCH_SIZE};
use crate::query;
use crate::types::Column;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 10_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Initial fixed overhead estimate for an empty batch payload.
const BATCH_OVERHEAD_BYTES: usize = 256;

fn partition_strategy_from_env() -> PartitionStrategy {
    match std::env::var("RAPIDBYTE_SOURCE_PARTITION_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("range") => PartitionStrategy::Range,
        _ => PartitionStrategy::Mod,
    }
}

fn effective_partition_strategy(stream: &StreamContext) -> PartitionStrategy {
    stream
        .partition_strategy
        .unwrap_or_else(partition_strategy_from_env)
}

fn build_range_bounds_sql(source_table_name: &str) -> String {
    format!(
        "SELECT MIN({id_col})::bigint, MAX({id_col})::bigint FROM {table_name}",
        id_col = quote_identifier("id"),
        table_name = query::quote_table_name(source_table_name),
    )
}

async fn compute_range_bounds(
    client: &Client,
    source_table_name: &str,
    partition_count: u32,
    partition_index: u32,
) -> Result<Option<(i64, i64)>, String> {
    if partition_count == 0 || partition_index >= partition_count {
        return Ok(None);
    }

    let sql = build_range_bounds_sql(source_table_name);
    let row = client
        .query_one(&sql, &[])
        .await
        .map_err(|e| format!("range partition min/max query failed: {e}"))?;

    let min_id: Option<i64> = row.get(0);
    let max_id: Option<i64> = row.get(1);
    let (min_id, max_id) = match (min_id, max_id) {
        (Some(min_id), Some(max_id)) if min_id <= max_id => (min_id, max_id),
        _ => return Ok(None),
    };

    let span = i128::from(max_id) - i128::from(min_id) + 1;
    let chunk = (span + i128::from(partition_count) - 1) / i128::from(partition_count);
    let start = i128::from(min_id) + i128::from(partition_index) * chunk;
    let mut end = start + chunk - 1;
    if partition_index == partition_count - 1 {
        end = i128::from(max_id);
    }

    let start_i64 = i64::try_from(start).map_err(|e| format!("range start overflow: {e}"))?;
    let end_i64 = i64::try_from(end).map_err(|e| format!("range end overflow: {e}"))?;
    Ok(Some((start_i64, end_i64)))
}

/// Estimate byte size of a single row for `max_record_bytes` checking.
pub(crate) fn estimate_row_bytes(columns: &[Column]) -> usize {
    let mut total = 0usize;
    for col in columns {
        total += match col.arrow_type {
            ArrowDataType::Int16 => 2,
            ArrowDataType::Int32 | ArrowDataType::Float32 | ArrowDataType::Date32 => 4,
            ArrowDataType::Int64 | ArrowDataType::Float64 | ArrowDataType::TimestampMicros => 8,
            ArrowDataType::Boolean => 1,
            _ => 64,
        };
        // 1-byte null bitmap overhead per column.
        total += 1;
    }
    total
}

/// Encode and emit all currently accumulated rows as one Arrow IPC batch.
fn emit_accumulated_rows(
    rows: &mut Vec<tokio_postgres::Row>,
    columns: &[Column],
    schema: &Arc<Schema>,
    ctx: &Context,
    state: &mut EmitState,
    estimated_bytes: &mut usize,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = encode::rows_to_record_batch(rows, columns, schema)?;
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
    *estimated_bytes = BATCH_OVERHEAD_BYTES;
    Ok(())
}

/// Read a single stream using server-side cursors.
#[allow(clippy::too_many_lines)]
pub async fn read_stream(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    let source_table_name = stream.source_stream_or_stream_name();
    ctx.log(
        LogLevel::Info,
        &format!("Reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // ── 1. Schema resolution ──────────────────────────────────────────
    let all_columns = crate::discovery::query_table_columns(client, source_table_name).await?;

    // ── 2. Projection pushdown ────────────────────────────────────────
    let columns: Vec<Column> = match &stream.selected_columns {
        Some(selected) if !selected.is_empty() => {
            let unknown: Vec<&str> = selected
                .iter()
                .filter(|name| !all_columns.iter().any(|c| c.name == **name))
                .map(std::string::String::as_str)
                .collect();
            if !unknown.is_empty() {
                return Err(format!(
                    "Selected columns {:?} not found in table '{}'",
                    unknown, source_table_name
                ));
            }

            let filtered: Vec<Column> = all_columns
                .into_iter()
                .filter(|c| selected.iter().any(|s| s == &c.name))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "None of the selected columns {:?} found in table '{}'",
                    selected, source_table_name
                ));
            }
            if let (SyncMode::Incremental, Some(ci)) = (&stream.sync_mode, &stream.cursor_info) {
                if !filtered.iter().any(|c| c.name == ci.cursor_field) {
                    return Err(format!(
                        "Cursor field '{}' must be included in selected columns for incremental stream '{}'",
                        ci.cursor_field,
                        stream.stream_name
                    ));
                }
            }
            filtered
        }
        _ => all_columns,
    };

    // ── 3. Arrow schema ───────────────────────────────────────────────
    let arrow_schema = encode::arrow_schema(&columns);

    // ── 4. Transaction + server-side cursor ───────────────────────────
    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| format!("BEGIN failed: {e}"))?;

    let partition_range_bounds = if effective_partition_strategy(stream)
        == PartitionStrategy::Range
    {
        match (stream.partition_count, stream.partition_index) {
            (Some(count), Some(index)) => {
                match compute_range_bounds(client, source_table_name, count, index).await {
                    Ok(bounds) => bounds,
                    Err(e) => {
                        ctx.log(
                            LogLevel::Warn,
                            &format!(
                                "Range partitioning disabled for stream '{}': {e}; falling back to modulo",
                                stream.stream_name
                            ),
                        );
                        None
                    }
                }
            }
            _ => None,
        }
    } else {
        None
    };

    let cursor_query = query::build_base_query(ctx, stream, &columns, partition_range_bounds)?;

    let declare = format!(
        "DECLARE {} NO SCROLL CURSOR FOR {}",
        CURSOR_NAME, cursor_query.sql
    );
    match cursor_query.bind.as_ref() {
        Some(bind) => {
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [bind.as_tosql()];
            client
                .execute(&declare, &params)
                .await
                .map_err(|e| format!("DECLARE CURSOR failed: {e}"))?;
        }
        None => {
            client
                .execute(&declare, &[])
                .await
                .map_err(|e| format!("DECLARE CURSOR failed: {e}"))?;
        }
    }

    let query_secs = query_start.elapsed().as_secs_f64();

    // ── 5. Limits + cursor tracker setup ──────────────────────────────
    // Safety: wasm32 target has 32-bit pointers; these u64 limits are always
    // within practical memory bounds so truncation is acceptable.
    #[allow(clippy::cast_possible_truncation)]
    let max_batch_bytes = if stream.limits.max_batch_bytes > 0 {
        stream.limits.max_batch_bytes as usize
    } else {
        StreamLimits::DEFAULT_MAX_BATCH_BYTES as usize
    };

    #[allow(clippy::cast_possible_truncation)]
    let max_record_bytes = if stream.limits.max_record_bytes > 0 {
        stream.limits.max_record_bytes as usize
    } else {
        StreamLimits::DEFAULT_MAX_RECORD_BYTES as usize
    };
    let estimated_row_bytes = estimate_row_bytes(&columns);
    let mut records_skipped: u64 = 0;

    let mut tracker: Option<CursorTracker> = match stream.cursor_info.as_ref() {
        Some(ci) => Some(CursorTracker::new(ci, &columns)?),
        None => None,
    };

    let fetch_query = format!("FETCH {FETCH_CHUNK} FROM {CURSOR_NAME}");
    let mut accumulated_rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut estimated_bytes: usize = BATCH_OVERHEAD_BYTES;
    let mut state = EmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };

    let mut loop_error: Option<String> = None;

    // ── 6. Fetch loop ─────────────────────────────────────────────────
    let fetch_start = Instant::now();
    loop {
        let rows = match client.query(&fetch_query, &[]).await {
            Ok(r) => r,
            Err(e) => {
                loop_error = Some(format!("FETCH failed for {}: {}", stream.stream_name, e));
                break;
            }
        };

        let exhausted = rows.is_empty();

        if !exhausted {
            if estimated_row_bytes > max_record_bytes {
                if stream.policies.on_data_error == DataErrorPolicy::Fail {
                    loop_error = Some(format!(
                        "Record exceeds max_record_bytes ({estimated_row_bytes} > {max_record_bytes})",
                    ));
                    break;
                }
                records_skipped += rows.len() as u64;
                ctx.log(
                    LogLevel::Warn,
                    &format!(
                        "Skipping {} oversized records: {estimated_row_bytes} bytes > max_record_bytes {max_record_bytes}",
                        rows.len(),
                    ),
                );
            } else {
                for row in rows {
                    if !accumulated_rows.is_empty()
                        && estimated_bytes + estimated_row_bytes >= max_batch_bytes
                    {
                        if let Err(e) = emit_accumulated_rows(
                            &mut accumulated_rows,
                            &columns,
                            &arrow_schema,
                            ctx,
                            &mut state,
                            &mut estimated_bytes,
                        ) {
                            loop_error = Some(e);
                            break;
                        }
                    }

                    if loop_error.is_some() {
                        break;
                    }

                    estimated_bytes += estimated_row_bytes;

                    // ── Cursor extraction ─────────────────────────────────
                    // IMPORTANT: PostgreSQL SERIAL is INT4 (i32), not INT8 (i64).
                    // tokio-postgres `try_get()` requires exact type matches, so we
                    // must chain i64 -> i32 fallbacks. The host orchestrator currently
                    // hardcodes CursorType::Utf8 for incremental state, so the catch-all
                    // arm is common. Do not remove the i32/i16 fallbacks or incremental
                    // tracking can silently stop advancing on SERIAL/SMALLSERIAL columns.
                    if let Some(ref mut t) = tracker {
                        let col_idx = t.col_idx();
                        if t.is_int_strategy() {
                            let val = row
                                .try_get::<_, i64>(col_idx)
                                .ok()
                                .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(i64::from))
                                .or_else(|| row.try_get::<_, i16>(col_idx).ok().map(i64::from));
                            if let Some(val) = val {
                                t.observe_int(val);
                            }
                        } else {
                            let val = row
                                .try_get::<_, String>(col_idx)
                                .ok()
                                .or_else(|| {
                                    row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string())
                                })
                                .or_else(|| {
                                    row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string())
                                })
                                .or_else(|| {
                                    row.try_get::<_, NaiveDateTime>(col_idx)
                                        .ok()
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                                })
                                .or_else(|| {
                                    row.try_get::<_, chrono::DateTime<chrono::Utc>>(col_idx)
                                        .ok()
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                                })
                                .or_else(|| {
                                    row.try_get::<_, chrono::NaiveDate>(col_idx)
                                        .ok()
                                        .map(|d| d.to_string())
                                })
                                .or_else(|| {
                                    row.try_get::<_, serde_json::Value>(col_idx)
                                        .ok()
                                        .map(|v| v.to_string())
                                });
                            if let Some(val) = val {
                                t.observe_text(&val);
                            }
                        }
                    }

                    accumulated_rows.push(row);
                }
            }
        }

        let should_emit = !accumulated_rows.is_empty()
            && (estimated_bytes >= max_batch_bytes
                || accumulated_rows.len() >= BATCH_SIZE
                || exhausted);

        if should_emit {
            if let Err(e) = emit_accumulated_rows(
                &mut accumulated_rows,
                &columns,
                &arrow_schema,
                ctx,
                &mut state,
                &mut estimated_bytes,
            ) {
                loop_error = Some(e);
                break;
            }
        }

        if exhausted {
            break;
        }

        // Stop early if max_records limit reached
        if let Some(max) = stream.limits.max_records {
            if state.total_records >= max {
                ctx.log(
                    LogLevel::Info,
                    &format!(
                        "Reached max_records limit ({}) for stream '{}'",
                        max, stream.stream_name
                    ),
                );
                break;
            }
        }
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // ── 7. Cleanup ────────────────────────────────────────────────────
    let close_query = format!("CLOSE {CURSOR_NAME}");
    if let Err(e) = client.execute(&close_query, &[]).await {
        ctx.log(
            LogLevel::Warn,
            &format!(
                "Warning: cursor CLOSE failed for stream '{}': {} (non-fatal, transaction cleanup will close it)",
                stream.stream_name, e
            ),
        );
    }
    if loop_error.is_some() {
        let _ = client.execute("ROLLBACK", &[]).await;
    } else {
        client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {e}"))?;
    }

    if let Some(e) = loop_error {
        return Err(e);
    }

    // ── 8. Checkpoint ─────────────────────────────────────────────────
    let checkpoint_count = if let Some(t) = tracker {
        if let Some(cp) =
            t.into_checkpoint(&stream.stream_name, state.total_records, state.total_bytes)
        {
            let cursor_field = cp.cursor_field.as_deref().unwrap_or("");
            let cursor_value = cp
                .cursor_value
                .as_ref()
                .map(|v| match v {
                    CursorValue::Utf8 { value: s } => s.clone(),
                    _ => format!("{v:?}"),
                })
                .unwrap_or_default();
            let _ = ctx.checkpoint(&cp);
            ctx.log(
                LogLevel::Info,
                &format!(
                    "Source checkpoint: stream={} cursor_field={} cursor_value={}",
                    stream.stream_name, cursor_field, cursor_value
                ),
            );
            1u64
        } else {
            0u64
        }
    } else {
        0u64
    };

    // ── 9. Summary ────────────────────────────────────────────────────
    ctx.log(
        LogLevel::Info,
        &format!(
            "Stream '{}' complete: {} records, {} bytes, {} batches",
            stream.stream_name, state.total_records, state.total_bytes, state.batches_emitted
        ),
    );

    // Safety: nanosecond timing precision loss beyond 52 bits is acceptable for metrics.
    #[allow(clippy::cast_precision_loss)]
    let arrow_encode_secs = state.arrow_encode_nanos as f64 / 1e9;

    let perf = ReadPerf {
        connect_secs,
        query_secs,
        fetch_secs,
        arrow_encode_secs,
    };
    emit_read_perf_metrics(ctx, &perf);

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped,
        perf: Some(perf),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::catalog::SchemaHint;
    use rapidbyte_sdk::stream::StreamPolicies;

    #[test]
    fn estimate_row_bytes_matches_expected_mix() {
        let columns = vec![
            Column::new("id", "bigint", false),
            Column::new("name", "text", true),
            Column::new("active", "boolean", false),
        ];
        // Int64(8)+Utf8(64)+Boolean(1) plus 1-byte null bitmap overhead per column.
        assert_eq!(estimate_row_bytes(&columns), 8 + 64 + 1 + 3);
    }

    #[test]
    fn stream_partition_strategy_override_wins_over_env() {
        let original = std::env::var("RAPIDBYTE_SOURCE_PARTITION_MODE").ok();
        std::env::set_var("RAPIDBYTE_SOURCE_PARTITION_MODE", "mod");

        let stream = StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: Some(PartitionStrategy::Range),
            copy_flush_bytes_override: None,
        };

        assert_eq!(effective_partition_strategy(&stream), PartitionStrategy::Range);

        if let Some(value) = original {
            std::env::set_var("RAPIDBYTE_SOURCE_PARTITION_MODE", value);
        } else {
            std::env::remove_var("RAPIDBYTE_SOURCE_PARTITION_MODE");
        }
    }

    #[test]
    fn falls_back_to_env_when_stream_strategy_not_set() {
        let original = std::env::var("RAPIDBYTE_SOURCE_PARTITION_MODE").ok();
        std::env::set_var("RAPIDBYTE_SOURCE_PARTITION_MODE", "range");

        let stream = StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        };

        assert_eq!(effective_partition_strategy(&stream), PartitionStrategy::Range);

        if let Some(value) = original {
            std::env::set_var("RAPIDBYTE_SOURCE_PARTITION_MODE", value);
        } else {
            std::env::remove_var("RAPIDBYTE_SOURCE_PARTITION_MODE");
        }
    }

    #[test]
    fn build_range_bounds_sql_supports_schema_qualified_table_name() {
        let sql = build_range_bounds_sql("public.users");
        assert_eq!(
            sql,
            "SELECT MIN(id)::bigint, MAX(id)::bigint FROM public.users"
        );
    }
}
