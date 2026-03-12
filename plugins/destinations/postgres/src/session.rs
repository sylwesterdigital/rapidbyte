use std::sync::Arc;
use std::time::Instant;

use tokio_postgres::Client;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::contract::{CheckpointConfig, WriteContract};
use crate::ddl::swap_staging_table;
use crate::decode;

const COPY_FLUSH_1MB: usize = 1024 * 1024;
const COPY_FLUSH_4MB: usize = 4 * 1024 * 1024;
const COPY_FLUSH_16MB: usize = 16 * 1024 * 1024;
pub(crate) const COPY_FLUSH_MAX: usize = 32 * 1024 * 1024;

pub(crate) fn clamp_copy_flush_bytes(bytes: usize) -> usize {
    bytes.clamp(COPY_FLUSH_1MB, COPY_FLUSH_MAX)
}

pub(crate) fn adaptive_copy_flush_bytes(
    configured: Option<usize>,
    avg_row_bytes: Option<usize>,
) -> usize {
    if let Some(bytes) = configured {
        return clamp_copy_flush_bytes(bytes);
    }

    match avg_row_bytes {
        Some(bytes) if bytes >= 64 * 1024 => COPY_FLUSH_16MB,
        Some(bytes) if bytes >= 8 * 1024 => COPY_FLUSH_4MB,
        _ => COPY_FLUSH_1MB,
    }
}

pub(crate) fn loop_error_commit_state(checkpoint_count: u64) -> CommitState {
    if checkpoint_count > 0 {
        CommitState::AfterCommitConfirmed
    } else {
        CommitState::BeforeCommit
    }
}

pub(crate) fn loop_error_commit_state_with_commits(
    checkpoint_count: u64,
    commits_completed: u64,
) -> CommitState {
    if commits_completed > checkpoint_count {
        CommitState::AfterCommitUnknown
    } else {
        loop_error_commit_state(checkpoint_count)
    }
}

/// Result of a completed write session, used to build `WriteSummary`.
pub struct SessionResult {
    pub total_rows: u64,
    pub total_bytes: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    pub flush_secs: f64,
    pub commit_secs: f64,
}

struct WriteStats {
    total_rows: u64,
    total_bytes: u64,
    batches_written: u64,
    pub(crate) checkpoint_count: u64,
    pub(crate) commits_completed: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
}

/// Manages lifecycle of writing a single stream to `PostgreSQL`.
pub struct WriteSession<'a> {
    ctx: &'a Context,
    client: &'a Client,
    target_schema: &'a str,

    stream_name: String,
    effective_stream: String,
    qualified_table: String,
    effective_write_mode: Option<WriteMode>,

    load_method: LoadMethod,
    schema_policy: SchemaEvolutionPolicy,
    needs_schema_ensure: bool,
    use_watermarks: bool,
    checkpoint_config: CheckpointConfig,
    copy_flush_bytes: Option<usize>,

    is_replace: bool,

    flush_start: Instant,
    last_checkpoint_time: Instant,
    stats: WriteStats,

    schema_state: crate::ddl::SchemaState,
}

impl<'a> WriteSession<'a> {
    /// Open a write session and BEGIN the first transaction.
    pub async fn begin(
        ctx: &'a Context,
        client: &'a Client,
        target_schema: &'a str,
        config: WriteContract,
    ) -> Result<WriteSession<'a>, String> {
        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("BEGIN failed: {e}"))?;

        let now = Instant::now();
        let mut schema_state = crate::ddl::SchemaState::new();
        schema_state.ignored_columns = config.ignored_columns;
        schema_state.type_null_columns = config.type_null_columns;
        if !config.needs_schema_ensure {
            schema_state.created_tables.insert(config.qualified_table.clone());
        }

        Ok(WriteSession {
            ctx,
            client,
            target_schema,
            stream_name: config.stream_name,
            effective_stream: config.effective_stream,
            qualified_table: config.qualified_table,
            effective_write_mode: config.effective_write_mode,
            load_method: config.load_method,
            schema_policy: config.schema_policy,
            needs_schema_ensure: config.needs_schema_ensure,
            use_watermarks: config.use_watermarks,
            checkpoint_config: config.checkpoint,
            copy_flush_bytes: config.copy_flush_bytes,
            is_replace: config.is_replace,
            flush_start: now,
            last_checkpoint_time: now,
            stats: WriteStats {
                total_rows: 0,
                total_bytes: 0,
                batches_written: 0,
                checkpoint_count: 0,
                commits_completed: 0,
                bytes_since_commit: 0,
                rows_since_commit: 0,
            },
            schema_state,
        })
    }

    /// Process a decoded Arrow batch.
    pub async fn process_batch(
        &mut self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<(), String> {
        let n: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        if self.needs_schema_ensure {
            self.schema_state
                .ensure_table(
                    self.ctx,
                    self.client,
                    self.target_schema,
                    &self.effective_stream,
                    self.effective_write_mode.as_ref(),
                    Some(&self.schema_policy),
                    schema,
                )
                .await?;
            self.needs_schema_ensure = false;
        }

        let active_cols = decode::active_column_indices(schema, &self.schema_state.ignored_columns);
        if active_cols.is_empty() {
            self.ctx.log(
                LogLevel::Warn,
                "dest-postgres: all columns ignored, skipping batch",
            );
            return Ok(());
        }
        let type_null_flags = decode::type_null_flags(
            &active_cols,
            schema,
            &self.schema_state.type_null_columns,
        );

        let target = decode::WriteTarget {
            table: &self.qualified_table,
            active_cols: &active_cols,
            schema,
            type_null_flags: &type_null_flags,
        };

        let use_copy = self.load_method == LoadMethod::Copy
            && !matches!(self.effective_write_mode, Some(WriteMode::Upsert { .. }));

        let (rows_written, bytes_written) = if use_copy {
            if self.copy_flush_bytes.is_none() {
                let avg_row_bytes = (batch_rows > 0).then(|| n / batch_rows as usize);
                let chosen = adaptive_copy_flush_bytes(None, avg_row_bytes);
                self.copy_flush_bytes = Some(chosen);
                self.ctx.log(
                    LogLevel::Debug,
                    &format!(
                        "dest-postgres: adaptive copy_flush_bytes={} (avg_row_bytes={})",
                        chosen,
                        avg_row_bytes.unwrap_or_default()
                    ),
                );
            }

            crate::copy::write(
                self.ctx,
                self.client,
                &target,
                batches,
                self.copy_flush_bytes,
            )
            .await?
        } else {
            let upsert_clause = decode::build_upsert_clause(
                self.effective_write_mode.as_ref(),
                schema,
                &active_cols,
            );
            crate::insert::write(
                self.ctx,
                self.client,
                &target,
                batches,
                upsert_clause.as_deref(),
            )
            .await?
        };

        self.stats.total_rows += rows_written;
        self.stats.total_bytes += bytes_written;
        self.stats.bytes_since_commit += bytes_written;
        self.stats.rows_since_commit += rows_written;
        self.stats.batches_written += 1;

        let _ = self.ctx.metric(&Metric {
            name: "records_written".to_string(),
            value: MetricValue::Counter(self.stats.total_rows),
            labels: vec![],
        });
        let _ = self.ctx.metric(&Metric {
            name: "bytes_written".to_string(),
            value: MetricValue::Counter(self.stats.total_bytes),
            labels: vec![],
        });

        self.maybe_checkpoint().await?;

        Ok(())
    }

    fn build_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            id: self.stats.checkpoint_count + 1,
            kind: CheckpointKind::Dest,
            stream: self.stream_name.clone(),
            cursor_field: None,
            cursor_value: None,
            records_processed: self.stats.total_rows,
            bytes_processed: self.stats.total_bytes,
        }
    }

    async fn maybe_checkpoint(&mut self) -> Result<(), String> {
        let cfg = &self.checkpoint_config;
        let should_checkpoint = (cfg.bytes > 0 && self.stats.bytes_since_commit >= cfg.bytes)
            || (cfg.rows > 0 && self.stats.rows_since_commit >= cfg.rows)
            || (cfg.seconds > 0 && self.last_checkpoint_time.elapsed().as_secs() >= cfg.seconds);

        if !should_checkpoint {
            return Ok(());
        }

        if self.use_watermarks {
            crate::watermark::set(
                self.client,
                self.target_schema,
                &self.stream_name,
                self.stats.total_rows,
                self.stats.total_bytes,
            )
            .await
            .map_err(|e| format!("Watermark update failed: {e}"))?;
        }

        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("Checkpoint COMMIT failed: {e}"))?;
        self.stats.commits_completed += 1;

        self.ctx
            .checkpoint(&self.build_checkpoint())
            .map_err(|e| format!("Destination checkpoint failed: {}", e.message))?;
        self.stats.checkpoint_count += 1;
        self.stats.bytes_since_commit = 0;
        self.stats.rows_since_commit = 0;
        self.last_checkpoint_time = Instant::now();

        self.ctx.log(
            LogLevel::Debug,
            &format!(
                "dest-postgres: checkpoint {} — committed {} rows, {} bytes so far",
                self.stats.checkpoint_count, self.stats.total_rows, self.stats.total_bytes
            ),
        );

        self.client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("Post-checkpoint BEGIN failed: {e}"))?;

        Ok(())
    }

    pub async fn commit(mut self) -> Result<SessionResult, String> {
        let flush_secs = self.flush_start.elapsed().as_secs_f64();

        if self.use_watermarks {
            crate::watermark::set(
                self.client,
                self.target_schema,
                &self.stream_name,
                self.stats.total_rows,
                self.stats.total_bytes,
            )
            .await
            .map_err(|e| format!("Watermark update failed: {e}"))?;
        }

        let commit_start = Instant::now();
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {e}"))?;
        self.stats.commits_completed += 1;
        let commit_secs = commit_start.elapsed().as_secs_f64();

        if self.is_replace {
            swap_staging_table(self.ctx, self.client, self.target_schema, &self.stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
        }

        self.ctx
            .checkpoint(&self.build_checkpoint())
            .map_err(|e| format!("Destination checkpoint failed: {}", e.message))?;
        self.stats.checkpoint_count += 1;

        if self.use_watermarks {
            let _ =
                crate::watermark::clear(self.client, self.target_schema, &self.stream_name).await;
        }

        self.ctx.log(
            LogLevel::Info,
            &format!(
                "dest-postgres: flushed {} rows in {} batches via {} (flush={:.3}s commit={:.3}s)",
                self.stats.total_rows,
                self.stats.batches_written,
                self.load_method,
                flush_secs,
                commit_secs
            ),
        );

        Ok(SessionResult {
            total_rows: self.stats.total_rows,
            total_bytes: self.stats.total_bytes,
            batches_written: self.stats.batches_written,
            checkpoint_count: self.stats.checkpoint_count,
            flush_secs,
            commit_secs,
        })
    }

    pub async fn rollback(self) {
        let _ = self.client.execute("ROLLBACK", &[]).await;
    }

    pub fn loop_error_commit_state(&self) -> CommitState {
        loop_error_commit_state_with_commits(
            self.stats.checkpoint_count,
            self.stats.commits_completed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_flush_uses_user_override_when_set() {
        let chosen = adaptive_copy_flush_bytes(Some(2 * 1024 * 1024), Some(80_000));
        assert_eq!(chosen, 2 * 1024 * 1024);
    }

    #[test]
    fn adaptive_flush_chooses_medium_row_bucket() {
        let chosen = adaptive_copy_flush_bytes(None, Some(10 * 1024));
        assert_eq!(chosen, COPY_FLUSH_4MB);
    }

    #[test]
    fn adaptive_flush_chooses_small_bucket() {
        let chosen = adaptive_copy_flush_bytes(None, Some(400));
        assert_eq!(chosen, 1024 * 1024);
    }

    #[test]
    fn adaptive_flush_chooses_large_row_bucket() {
        let chosen = adaptive_copy_flush_bytes(None, Some(70 * 1024));
        assert_eq!(chosen, 16 * 1024 * 1024);
    }

    #[test]
    fn loop_error_before_any_checkpoint_is_before_commit() {
        assert_eq!(loop_error_commit_state(0), CommitState::BeforeCommit);
    }

    #[test]
    fn loop_error_after_checkpoint_is_after_commit_confirmed() {
        assert_eq!(
            loop_error_commit_state(1),
            CommitState::AfterCommitConfirmed,
        );
    }

    #[test]
    fn loop_error_after_uncheckpointed_commit_is_after_commit_unknown() {
        assert_eq!(
            loop_error_commit_state_with_commits(0, 1),
            CommitState::AfterCommitUnknown,
        );
    }
}
