//! Stream write orchestration for destination `PostgreSQL` plugin.

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

use crate::contract::{
    async_prepare_stream_once, prepare_stream_once, schema_hint_has_shape, CheckpointConfig,
};
use crate::metrics::emit_write_perf_metrics;
use crate::session::{clamp_copy_flush_bytes, WriteSession};

fn resolve_copy_flush_bytes(
    stream_override: Option<u64>,
    configured: Option<usize>,
) -> Option<usize> {
    if let Some(bytes) = stream_override {
        if bytes == 0 {
            return configured.map(clamp_copy_flush_bytes);
        }
        let override_bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
        return Some(clamp_copy_flush_bytes(override_bytes));
    }

    configured.map(clamp_copy_flush_bytes)
}

/// Entry point for writing a single stream.
pub async fn write_stream(
    config: &crate::config::Config,
    ctx: &Context,
    stream: &StreamContext,
) -> Result<WriteSummary, PluginError> {
    let connect_start = Instant::now();
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    let connect_secs = connect_start.elapsed().as_secs_f64();

    let setup = prepare_stream_once(
        &config.schema,
        &stream.stream_name,
        stream.write_mode.clone(),
        &stream.schema,
        stream.partition_count.unwrap_or(1) <= 1,
        stream.policies.schema_evolution,
        CheckpointConfig {
            bytes: stream.limits.checkpoint_interval_bytes,
            rows: stream.limits.checkpoint_interval_rows,
            seconds: stream.limits.checkpoint_interval_seconds,
        },
        resolve_copy_flush_bytes(stream.copy_flush_bytes_override, config.copy_flush_bytes),
        config.load_method,
    )
    .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
    let skip_mutable_setup = stream.partition_count.unwrap_or(1) > 1
        && !setup.is_replace
        && schema_hint_has_shape(&stream.schema);
    let setup = if skip_mutable_setup {
        setup
    } else {
        async_prepare_stream_once(ctx, &client, &stream.schema, setup)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
    };

    let mut session = WriteSession::begin(ctx, &client, &config.schema, setup)
        .await
        .map_err(|e| PluginError::transient_db("SESSION_BEGIN_FAILED", e))?;

    let mut loop_error: Option<String> = None;
    let mut arrow_decode_secs = 0.0;

    loop {
        match rapidbyte_sdk::host_ffi::next_batch_with_decode_timing(stream.limits.max_batch_bytes)
        {
            Ok(None) => break,
            Ok(Some(decoded)) => {
                arrow_decode_secs += decoded.decode_secs;
                let _ = ctx.metric(&Metric {
                    name: "dest_arrow_decode_secs".to_string(),
                    value: MetricValue::Gauge(decoded.decode_secs),
                    labels: vec![],
                });

                if let Err(e) = session.process_batch(&decoded.schema, &decoded.batches).await {
                    loop_error = Some(e);
                    break;
                }
            }
            Err(e) => {
                loop_error = Some(format!("next_batch failed: {e}"));
                break;
            }
        }
    }

    if let Some(err) = loop_error {
        let commit_state = session.loop_error_commit_state();
        session.rollback().await;
        return Err(PluginError::transient_db("WRITE_FAILED", err).with_commit_state(commit_state));
    }

    let result = session.commit().await.map_err(|e| {
        PluginError::transient_db("COMMIT_FAILED", e)
            .with_commit_state(CommitState::AfterCommitUnknown)
    })?;

    let perf = WritePerf {
        connect_secs,
        flush_secs: result.flush_secs,
        commit_secs: result.commit_secs,
        arrow_decode_secs,
    };
    emit_write_perf_metrics(ctx, &perf);

    Ok(WriteSummary {
        records_written: result.total_rows,
        bytes_written: result.total_bytes,
        batches_written: result.batches_written,
        checkpoint_count: result.checkpoint_count,
        records_failed: 0,
        perf: Some(perf),
    })
}

#[cfg(test)]
mod tests {
    use super::resolve_copy_flush_bytes;
    use crate::session::COPY_FLUSH_MAX;

    #[test]
    fn runtime_override_takes_precedence_over_configured_flush_bytes() {
        let resolved = resolve_copy_flush_bytes(Some(16 * 1024 * 1024), Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(16 * 1024 * 1024));
    }

    #[test]
    fn zero_runtime_override_falls_back_to_configured_flush_bytes() {
        let resolved = resolve_copy_flush_bytes(Some(0), Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(2 * 1024 * 1024));
    }

    #[test]
    fn configured_flush_bytes_used_when_no_runtime_override() {
        let resolved = resolve_copy_flush_bytes(None, Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(2 * 1024 * 1024));
    }

    #[test]
    fn resolved_flush_bytes_are_clamped_to_guardrails() {
        let clamped_override = resolve_copy_flush_bytes(Some(u64::MAX), None);
        assert_eq!(clamped_override, Some(COPY_FLUSH_MAX));

        let clamped_config = resolve_copy_flush_bytes(None, Some(256 * 1024));
        assert_eq!(clamped_config, Some(1024 * 1024));
    }
}
