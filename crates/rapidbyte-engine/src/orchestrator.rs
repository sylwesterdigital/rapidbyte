//! Pipeline orchestrator: resolves plugins, loads modules, executes streams, and finalizes state.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use rapidbyte_runtime::{
    parse_plugin_ref, Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::StateBackend;
use rapidbyte_types::catalog::{Catalog, SchemaHint};
use rapidbyte_types::cursor::{CursorInfo, CursorType, CursorValue};
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::{Permissions, PluginManifest, ResourceLimits};
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::stream::{PartitionStrategy, StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::Feature;
use rapidbyte_types::wire::{PluginKind, SyncMode, WriteMode};

use crate::arrow::ipc_to_record_batches;
use crate::checkpoint::correlate_and_persist_cursors;
use crate::config::types::{parse_byte_size, PipelineConfig, PipelineParallelism};
use crate::error::{compute_backoff, PipelineError};
use crate::execution::{DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome};
use crate::progress::{Phase, ProgressEvent};
use crate::resolve::{
    build_sandbox_overrides, check_state_backend, create_state_backend, load_and_validate_manifest,
    resolve_plugins, validate_config_against_schema, ResolvedPlugins,
};
use crate::result::{
    CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric,
};
use crate::runner::{
    run_destination_stream, run_discover, run_source_stream, run_transform_stream, validate_plugin,
    TransformRunResult,
};

struct StreamResult {
    stream_name: String,
    partition_index: Option<u32>,
    partition_count: Option<u32>,
    read_summary: ReadSummary,
    write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    src_host_timings: HostTimings,
    dst_host_timings: HostTimings,
    src_duration: f64,
    dst_duration: f64,
    vm_setup_secs: f64,
    recv_secs: f64,
    transform_durations: Vec<f64>,
    dry_run_result: Option<DryRunStreamResult>,
}

#[derive(Clone)]
struct LoadedTransformModule {
    module: LoadedComponent,
    plugin_id: String,
    plugin_version: String,
    config: serde_json::Value,
    load_ms: u64,
    permissions: Option<Permissions>,
    manifest_limits: ResourceLimits,
}

struct LoadedModules {
    source_module: LoadedComponent,
    dest_module: LoadedComponent,
    source_module_load_ms: u64,
    dest_module_load_ms: u64,
    transform_modules: Vec<LoadedTransformModule>,
}

struct StreamBuild {
    limits: StreamLimits,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
    stream_ctxs: Vec<StreamContext>,
}

struct StreamParams {
    pipeline_name: String,
    source_config: serde_json::Value,
    dest_config: serde_json::Value,
    source_plugin_id: String,
    source_plugin_version: String,
    dest_plugin_id: String,
    dest_plugin_version: String,
    source_permissions: Option<Permissions>,
    dest_permissions: Option<Permissions>,
    source_overrides: Option<SandboxOverrides>,
    dest_overrides: Option<SandboxOverrides>,
    transform_overrides: Vec<Option<SandboxOverrides>>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
    channel_capacity: usize,
}

struct AggregatedStreamResults {
    total_read_summary: ReadSummary,
    total_write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    src_timings: HostTimings,
    dst_timings: HostTimings,
    src_timing_maxima: SourceTimingMaxima,
    dst_timing_maxima: DestTimingMaxima,
    transform_durations: Vec<f64>,
    dlq_records: Vec<DlqRecord>,
    final_stats: RunStats,
    first_error: Option<PipelineError>,
    dry_run_streams: Vec<DryRunStreamResult>,
    stream_metrics: Vec<StreamShardMetric>,
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, Default)]
struct SourceTimingMaxima {
    duration_secs: f64,
    connect_secs: f64,
    query_secs: f64,
    fetch_secs: f64,
    arrow_encode_secs: f64,
}

fn observe_source_timing(maxima: &mut SourceTimingMaxima, stream: &StreamResult) {
    maxima.duration_secs = maxima.duration_secs.max(stream.src_duration);
    maxima.connect_secs = maxima
        .connect_secs
        .max(stream.src_host_timings.source_connect_secs);
    maxima.query_secs = maxima
        .query_secs
        .max(stream.src_host_timings.source_query_secs);
    maxima.fetch_secs = maxima
        .fetch_secs
        .max(stream.src_host_timings.source_fetch_secs);
    maxima.arrow_encode_secs = maxima
        .arrow_encode_secs
        .max(stream.src_host_timings.source_arrow_encode_secs);
}

struct StreamTaskCollection {
    successes: Vec<StreamResult>,
    first_error: Option<PipelineError>,
}

const SYSTEM_CORE_RESERVE_DIVISOR: u32 = 8;
const MIN_PIPELINE_COORDINATION_CORES: u32 = 1;
const MAX_PIPELINE_COORDINATION_CORES: u32 = 2;
const TRANSFORM_PENALTY_SLOPE: f64 = 0.05;
const MIN_TRANSFORM_FACTOR: f64 = 0.75;

fn resolve_effective_parallelism(config: &PipelineConfig, supports_partitioned_read: bool) -> u32 {
    match config.resources.parallelism {
        PipelineParallelism::Manual(value) => value.max(1),
        PipelineParallelism::Auto => resolve_auto_parallelism(config, supports_partitioned_read),
    }
}

fn resolve_auto_parallelism(config: &PipelineConfig, supports_partitioned_read: bool) -> u32 {
    let available_cores = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let available_cores = u32::try_from(available_cores).unwrap_or(u32::MAX);
    resolve_auto_parallelism_for_cores(config, available_cores, supports_partitioned_read)
}

fn resolve_auto_parallelism_for_cores(
    config: &PipelineConfig,
    available_cores: u32,
    supports_partitioned_read: bool,
) -> u32 {
    let cap = auto_worker_core_budget(available_cores);

    let eligible_streams = if supports_partitioned_read {
        u32::try_from(
            config
                .source
                .streams
                .iter()
                .filter(|stream| stream.sync_mode == SyncMode::FullRefresh)
                .count(),
        )
        .unwrap_or(u32::MAX)
    } else {
        0
    };

    let base_target = if eligible_streams == 0 {
        1
    } else {
        let per_stream_budget = (cap / eligible_streams).max(1);
        (eligible_streams * per_stream_budget).min(cap)
    };

    #[allow(clippy::cast_precision_loss)]
    let transform_count = config.transforms.len() as f64;
    let transform_factor = if transform_count == 0.0 {
        1.0
    } else {
        (1.0 / (1.0 + (TRANSFORM_PENALTY_SLOPE * transform_count))).max(MIN_TRANSFORM_FACTOR)
    };

    let scaled = (f64::from(base_target) * transform_factor).round();
    if scaled <= 1.0 {
        1
    } else if scaled >= f64::from(cap) {
        cap
    } else {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let scaled_u32 = scaled as u32;
        scaled_u32.clamp(1, cap)
    }
}

fn auto_worker_core_budget(available_cores: u32) -> u32 {
    let available_cores = available_cores.max(1);

    let system_reserve = if available_cores <= 2 {
        0
    } else {
        (available_cores / SYSTEM_CORE_RESERVE_DIVISOR).max(1)
    };
    let coordination_reserve = (available_cores / 4).clamp(
        MIN_PIPELINE_COORDINATION_CORES,
        MAX_PIPELINE_COORDINATION_CORES,
    );

    available_cores
        .saturating_sub(system_reserve)
        .saturating_sub(coordination_reserve)
        .max(1)
}

fn decode_incremental_last_value(
    raw: String,
    tie_breaker_field: Option<&str>,
) -> Result<CursorValue, PipelineError> {
    if tie_breaker_field.is_some() {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) {
            if value
                .as_object()
                .is_some_and(|object| object.contains_key("cursor"))
            {
                return Ok(CursorValue::Json { value });
            }
        }

        return Ok(CursorValue::Json {
            value: serde_json::json!({
                "cursor": {
                    "type": "utf8",
                    "value": raw,
                },
                "tie_breaker": {
                    "type": "null",
                }
            }),
        });
    }

    Ok(CursorValue::Utf8 { value: raw })
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, Default)]
struct DestTimingMaxima {
    duration_secs: f64,
    vm_setup_secs: f64,
    recv_secs: f64,
    connect_secs: f64,
    flush_secs: f64,
    commit_secs: f64,
    arrow_decode_secs: f64,
}

fn build_source_timing(
    src_timing_maxima: &SourceTimingMaxima,
    src_timings: &HostTimings,
    src_perf: Option<&rapidbyte_types::metric::ReadPerf>,
    source_module_load_ms: u64,
) -> SourceTiming {
    SourceTiming {
        duration_secs: src_timing_maxima.duration_secs,
        module_load_ms: source_module_load_ms,
        connect_secs: src_perf.map_or(src_timing_maxima.connect_secs, |p| p.connect_secs),
        query_secs: src_perf.map_or(src_timing_maxima.query_secs, |p| p.query_secs),
        fetch_secs: src_perf.map_or(src_timing_maxima.fetch_secs, |p| p.fetch_secs),
        arrow_encode_secs: src_perf
            .map_or(src_timing_maxima.arrow_encode_secs, |p| p.arrow_encode_secs),
        emit_nanos: src_timings.emit_batch_nanos,
        compress_nanos: src_timings.compress_nanos,
        emit_count: src_timings.emit_batch_count,
    }
}

fn observe_dest_timing(maxima: &mut DestTimingMaxima, stream: &StreamResult) {
    maxima.duration_secs = maxima.duration_secs.max(stream.dst_duration);
    maxima.vm_setup_secs = maxima.vm_setup_secs.max(stream.vm_setup_secs);
    maxima.recv_secs = maxima.recv_secs.max(stream.recv_secs);
    maxima.connect_secs = maxima
        .connect_secs
        .max(stream.dst_host_timings.dest_connect_secs);
    maxima.flush_secs = maxima
        .flush_secs
        .max(stream.dst_host_timings.dest_flush_secs);
    maxima.commit_secs = maxima
        .commit_secs
        .max(stream.dst_host_timings.dest_commit_secs);
    maxima.arrow_decode_secs = maxima
        .arrow_decode_secs
        .max(stream.dst_host_timings.dest_arrow_decode_secs);
}

/// Type alias for the progress channel sender used throughout the orchestrator.
type ProgressTx = Option<tokio::sync::mpsc::UnboundedSender<ProgressEvent>>;

fn send_progress(tx: &ProgressTx, event: ProgressEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event);
    }
}

async fn collect_stream_task_results(
    mut stream_join_set: JoinSet<Result<StreamResult, PipelineError>>,
    progress_tx: &ProgressTx,
) -> Result<StreamTaskCollection, PipelineError> {
    let mut successes = Vec::new();
    let mut first_error: Option<PipelineError> = None;

    while let Some(joined) = stream_join_set.join_next().await {
        match joined {
            Ok(Ok(sr)) if first_error.is_none() => {
                send_progress(
                    progress_tx,
                    ProgressEvent::StreamCompleted {
                        stream: sr.stream_name.clone(),
                    },
                );
                successes.push(sr);
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::error!("Stream failed: {}", error);
                if first_error.is_none() {
                    first_error = Some(error);
                    stream_join_set.abort_all();
                }
            }
            Err(join_err) if join_err.is_cancelled() && first_error.is_some() => {
                // Expected: sibling tasks cancelled after first stream failure.
            }
            Err(join_err) => {
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "Stream task panicked: {join_err}"
                )));
            }
        }
    }

    Ok(StreamTaskCollection {
        successes,
        first_error,
    })
}

/// Run a full pipeline: source -> destination with state tracking.
/// Retries on retryable plugin errors up to `config.resources.max_retries` times.
///
/// # Errors
///
/// Returns a `PipelineError` if the pipeline fails after exhausting retries
/// or encounters a non-retryable error.
pub async fn run_pipeline(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    progress_tx: Option<tokio::sync::mpsc::UnboundedSender<ProgressEvent>>,
) -> Result<PipelineOutcome, PipelineError> {
    let max_retries = config.resources.max_retries;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let result = execute_pipeline_once(config, options, attempt, progress_tx.clone()).await;

        match result {
            Ok(outcome) => return Ok(outcome),
            Err(ref err) if err.is_retryable() && attempt <= max_retries => {
                if let Some(plugin_err) = err.as_plugin_error() {
                    let delay = compute_backoff(plugin_err, attempt);
                    let commit_state_str =
                        plugin_err.commit_state.as_ref().map(|cs| format!("{cs:?}"));
                    #[allow(clippy::cast_possible_truncation)]
                    // Safety: delay.as_millis() is always well under u64::MAX
                    let delay_ms = delay.as_millis() as u64;
                    tracing::warn!(
                        attempt,
                        max_retries,
                        delay_ms,
                        category = %plugin_err.category,
                        code = %plugin_err.code,
                        commit_state = commit_state_str.as_deref(),
                        safe_to_retry = plugin_err.safe_to_retry,
                        "Retryable error, will retry"
                    );
                    send_progress(
                        &progress_tx,
                        ProgressEvent::Retry {
                            attempt,
                            max_retries,
                            message: format!(
                                "[{}] {}: {}",
                                plugin_err.category, plugin_err.code, plugin_err.message
                            ),
                            delay_secs: delay.as_secs_f64(),
                        },
                    );
                    tokio::time::sleep(delay).await;
                }
            }
            Err(err) => {
                if let Some(plugin_err) = err.as_plugin_error() {
                    let commit_state_str =
                        plugin_err.commit_state.as_ref().map(|cs| format!("{cs:?}"));
                    if err.is_retryable() {
                        tracing::error!(
                            attempt,
                            max_retries,
                            category = %plugin_err.category,
                            code = %plugin_err.code,
                            commit_state = commit_state_str.as_deref(),
                            safe_to_retry = plugin_err.safe_to_retry,
                            "Max retries exhausted, failing pipeline"
                        );
                    } else {
                        tracing::error!(
                            category = %plugin_err.category,
                            code = %plugin_err.code,
                            commit_state = commit_state_str.as_deref(),
                            "Non-retryable plugin error, failing pipeline"
                        );
                    }
                } else {
                    tracing::error!("Infrastructure error, failing pipeline: {}", err);
                }
                return Err(err);
            }
        }
    }
}

async fn execute_pipeline_once(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    attempt: u32,
    progress_tx: ProgressTx,
) -> Result<PipelineOutcome, PipelineError> {
    let start = Instant::now();
    let pipeline_id = PipelineId::new(config.pipeline.clone());
    tracing::info!(
        pipeline = config.pipeline,
        dry_run = options.dry_run,
        "Starting pipeline run"
    );

    send_progress(
        &progress_tx,
        ProgressEvent::PhaseChange {
            phase: Phase::Resolving,
        },
    );
    let plugins = resolve_plugins(config)?;
    let state = create_state_backend(config).map_err(PipelineError::Infrastructure)?;

    // Skip run tracking in dry-run mode to avoid orphaned run records.
    let run_id = if options.dry_run {
        0
    } else {
        let state_for_run = state.clone();
        let pipeline_id_for_run = pipeline_id.clone();
        tokio::task::spawn_blocking(move || {
            state_for_run.start_run(&pipeline_id_for_run, &StreamName::new("all"))
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("start_run task panicked: {e}"))
        })?
        .map_err(|e| PipelineError::Infrastructure(e.into()))?
    };

    send_progress(
        &progress_tx,
        ProgressEvent::PhaseChange {
            phase: Phase::Loading,
        },
    );
    let modules = load_modules(config, &plugins).await?;
    let config_for_build = config.clone();
    let state_for_build = state.clone();
    let max_records = options.limit;
    let source_manifest_for_build = plugins.source_manifest.clone();
    let stream_build = tokio::task::spawn_blocking(move || {
        build_stream_contexts(
            &config_for_build,
            state_for_build.as_ref(),
            max_records,
            source_manifest_for_build.as_ref(),
        )
    })
    .await
    .map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("build_stream_contexts task panicked: {e}"))
    })??;
    send_progress(
        &progress_tx,
        ProgressEvent::PhaseChange {
            phase: Phase::Running,
        },
    );
    let aggregated = execute_streams(
        config,
        &plugins,
        &modules,
        &stream_build,
        state.clone(),
        options,
        &progress_tx,
    )
    .await?;

    send_progress(
        &progress_tx,
        ProgressEvent::PhaseChange {
            phase: Phase::Finished,
        },
    );

    if options.dry_run {
        let duration_secs = start.elapsed().as_secs_f64();
        let src_perf = aggregated.total_read_summary.perf.as_ref();
        return Ok(PipelineOutcome::DryRun(DryRunResult {
            streams: aggregated.dry_run_streams,
            source: build_source_timing(
                &aggregated.src_timing_maxima,
                &aggregated.src_timings,
                src_perf,
                modules.source_module_load_ms,
            ),
            transform_count: config.transforms.len(),
            transform_duration_secs: aggregated.transform_durations.iter().sum(),
            duration_secs,
        }));
    }

    let result = finalize_run(
        config,
        &pipeline_id,
        state.clone(),
        run_id,
        attempt,
        start,
        &modules,
        aggregated,
    )
    .await?;
    Ok(PipelineOutcome::Run(result))
}

async fn load_modules(
    config: &PipelineConfig,
    plugins: &ResolvedPlugins,
) -> Result<LoadedModules, PipelineError> {
    let runtime = Arc::new(WasmRuntime::new().map_err(PipelineError::Infrastructure)?);
    tracing::info!(
        source = %plugins.source_wasm.display(),
        destination = %plugins.dest_wasm.display(),
        "Loading plugin modules"
    );

    let source_wasm_for_load = plugins.source_wasm.clone();
    let runtime_for_source = runtime.clone();
    #[allow(clippy::cast_possible_truncation)]
    let source_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_source
            .load_module(&source_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let dest_wasm_for_load = plugins.dest_wasm.clone();
    let runtime_for_dest = runtime.clone();
    #[allow(clippy::cast_possible_truncation)]
    let dest_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_dest
            .load_module(&dest_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let (source_module, source_module_load_ms) = source_load_task.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("Source module load task panicked: {e}"))
    })??;
    let (dest_module, dest_module_load_ms) = dest_load_task.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "Destination module load task panicked: {e}"
        ))
    })??;

    tracing::info!(
        source_ms = source_module_load_ms,
        dest_ms = dest_module_load_ms,
        "Plugin modules loaded"
    );

    let mut transform_modules = Vec::with_capacity(config.transforms.len());
    for tc in &config.transforms {
        let wasm_path = rapidbyte_runtime::resolve_plugin_path(&tc.use_ref, PluginKind::Transform)
            .map_err(PipelineError::Infrastructure)?;
        let manifest = load_and_validate_manifest(&wasm_path, &tc.use_ref, PluginKind::Transform)
            .map_err(PipelineError::Infrastructure)?;
        if let Some(ref m) = manifest {
            validate_config_against_schema(&tc.use_ref, &tc.config, m)
                .map_err(PipelineError::Infrastructure)?;
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let transform_manifest_limits = manifest
            .as_ref()
            .map(|m| m.limits.clone())
            .unwrap_or_default();
        let load_start = Instant::now();
        let module = runtime
            .load_module(&wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        #[allow(clippy::cast_possible_truncation)]
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        let (id, ver) = parse_plugin_ref(&tc.use_ref);
        transform_modules.push(LoadedTransformModule {
            module,
            plugin_id: id,
            plugin_version: ver,
            config: tc.config.clone(),
            load_ms,
            permissions: transform_perms,
            manifest_limits: transform_manifest_limits,
        });
    }

    Ok(LoadedModules {
        source_module,
        dest_module,
        source_module_load_ms,
        dest_module_load_ms,
        transform_modules,
    })
}

#[allow(clippy::too_many_lines)]
fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
    max_records: Option<u64>,
    source_manifest: Option<&PluginManifest>,
) -> Result<StreamBuild, PipelineError> {
    let supports_partitioned_read = source_manifest
        .is_some_and(|m: &PluginManifest| m.has_source_feature(Feature::PartitionedRead));
    let baseline_parallelism = resolve_effective_parallelism(config, supports_partitioned_read);
    let autotune_decision = crate::autotune::resolve_stream_autotune(
        config,
        baseline_parallelism,
        supports_partitioned_read,
    );
    let configured_parallelism = autotune_decision.parallelism;
    tracing::info!(
        autotune_enabled = autotune_decision.autotune_enabled,
        baseline_parallelism,
        configured_parallelism,
        partition_strategy = ?autotune_decision.partition_strategy,
        copy_flush_bytes_override = autotune_decision.copy_flush_bytes_override,
        "Autotune decision resolved"
    );
    let max_batch = parse_byte_size(&config.resources.max_batch_bytes).map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "Invalid max_batch_bytes '{}': {}",
            config.resources.max_batch_bytes,
            e
        ))
    })?;
    let checkpoint_interval = parse_byte_size(&config.resources.checkpoint_interval_bytes)
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Invalid checkpoint_interval_bytes '{}': {}",
                config.resources.checkpoint_interval_bytes,
                e
            ))
        })?;

    let limits = StreamLimits {
        max_batch_bytes: if max_batch > 0 {
            max_batch
        } else {
            StreamLimits::default().max_batch_bytes
        },
        checkpoint_interval_bytes: checkpoint_interval,
        checkpoint_interval_rows: config.resources.checkpoint_interval_rows,
        checkpoint_interval_seconds: config.resources.checkpoint_interval_seconds,
        max_inflight_batches: config.resources.max_inflight_batches,
        max_records,
        ..StreamLimits::default()
    };

    let pipeline_id = PipelineId::new(config.pipeline.clone());
    let should_partition = supports_partitioned_read && configured_parallelism > 1;
    let mut stream_ctxs = Vec::new();

    for s in &config.source.streams {
        let cursor_info = match s.sync_mode {
            SyncMode::Incremental => {
                if let Some(cursor_field) = &s.cursor_field {
                    let last_value = state
                        .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                        .map_err(|e| PipelineError::Infrastructure(e.into()))?
                        .and_then(|cs| cs.cursor_value)
                        .map(|v| decode_incremental_last_value(v, s.tie_breaker_field.as_deref()))
                        .transpose()?;
                    Some(CursorInfo {
                        cursor_field: cursor_field.clone(),
                        tie_breaker_field: s.tie_breaker_field.clone(),
                        cursor_type: CursorType::Utf8,
                        last_value,
                    })
                } else {
                    None
                }
            }
            SyncMode::Cdc => {
                let last_value = state
                    .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                    .map_err(|e| PipelineError::Infrastructure(e.into()))?
                    .and_then(|cs| cs.cursor_value)
                    .map(|v| CursorValue::Lsn { value: v });
                Some(CursorInfo {
                    cursor_field: "lsn".to_string(),
                    tie_breaker_field: None,
                    cursor_type: CursorType::Lsn,
                    last_value,
                })
            }
            SyncMode::FullRefresh => None,
        };

        let base_ctx = StreamContext {
            stream_name: s.name.clone(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: s.sync_mode,
            cursor_info,
            limits: limits.clone(),
            policies: StreamPolicies {
                on_data_error: config.destination.on_data_error,
                schema_evolution: config.destination.schema_evolution.unwrap_or_default(),
            },
            write_mode: Some(
                config
                    .destination
                    .write_mode
                    .to_protocol(config.destination.primary_key.clone()),
            ),
            selected_columns: s.columns.clone(),
            partition_key: s.partition_key.clone(),
            partition_count: None,
            partition_index: None,
            effective_parallelism: Some(configured_parallelism),
            partition_strategy: None,
            copy_flush_bytes_override: autotune_decision.copy_flush_bytes_override,
        };

        if should_partition
            && s.sync_mode == SyncMode::FullRefresh
            && !matches!(base_ctx.write_mode, Some(WriteMode::Replace))
        {
            for shard in 0..configured_parallelism {
                let mut shard_ctx = base_ctx.clone();
                shard_ctx.source_stream_name = Some(s.name.clone());
                shard_ctx.partition_count = Some(configured_parallelism);
                shard_ctx.partition_index = Some(shard);
                shard_ctx.partition_strategy = Some(
                    autotune_decision
                        .partition_strategy
                        .unwrap_or(PartitionStrategy::Mod),
                );
                stream_ctxs.push(shard_ctx);
            }
        } else {
            stream_ctxs.push(base_ctx);
        }
    }

    Ok(StreamBuild {
        limits,
        compression: config.resources.compression,
        stream_ctxs,
    })
}

fn destination_preflight_streams(stream_ctxs: &[StreamContext]) -> Vec<StreamContext> {
    let mut seen = HashSet::new();
    let mut preflight = Vec::new();
    for stream_ctx in stream_ctxs {
        if matches!(stream_ctx.write_mode, Some(WriteMode::Replace)) {
            continue;
        }
        if seen.insert(stream_ctx.stream_name.clone()) {
            let mut preflight_ctx = stream_ctx.clone();
            // Preflight runs once per logical stream and should not carry shard identity.
            preflight_ctx.partition_count = None;
            preflight_ctx.partition_index = None;
            preflight.push(preflight_ctx);
        }
    }
    preflight
}

fn execution_parallelism(config: &PipelineConfig, stream_ctxs: &[StreamContext]) -> usize {
    let resolved = stream_ctxs
        .iter()
        .filter_map(|ctx| ctx.effective_parallelism)
        .max()
        .unwrap_or_else(|| resolve_effective_parallelism(config, false));

    usize::try_from(resolved.max(1)).unwrap_or(usize::MAX)
}

struct CollectedTransforms {
    durations: Vec<f64>,
    first_error: Option<PipelineError>,
}

async fn collect_transform_results(
    transform_handles: Vec<(
        usize,
        tokio::task::JoinHandle<Result<TransformRunResult, PipelineError>>,
    )>,
    stream_name: &str,
) -> Result<CollectedTransforms, PipelineError> {
    let mut durations = Vec::new();
    let mut first_error: Option<PipelineError> = None;
    for (i, t_handle) in transform_handles {
        let result = t_handle.await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Transform {i} task panicked for stream '{stream_name}': {e}"
            ))
        })?;
        match result {
            Ok(tr) => {
                tracing::info!(
                    transform_index = i,
                    stream = stream_name,
                    duration_secs = tr.duration_secs,
                    records_in = tr.summary.records_in,
                    records_out = tr.summary.records_out,
                    "Transform stage completed for stream"
                );
                durations.push(tr.duration_secs);
            }
            Err(e) => {
                tracing::error!(
                    transform_index = i,
                    stream = stream_name,
                    "Transform failed: {e}",
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    Ok(CollectedTransforms {
        durations,
        first_error,
    })
}

#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn execute_streams(
    config: &PipelineConfig,
    plugins: &ResolvedPlugins,
    modules: &LoadedModules,
    stream_build: &StreamBuild,
    state: Arc<dyn StateBackend>,
    options: &ExecutionOptions,
    progress_tx: &ProgressTx,
) -> Result<AggregatedStreamResults, PipelineError> {
    let (source_plugin_id, source_plugin_version) = parse_plugin_ref(&config.source.use_ref);
    let (dest_plugin_id, dest_plugin_version) = parse_plugin_ref(&config.destination.use_ref);
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let num_transforms = config.transforms.len();
    let parallelism = execution_parallelism(config, &stream_build.stream_ctxs);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));

    tracing::info!(
        pipeline = config.pipeline,
        parallelism,
        num_streams = stream_build.stream_ctxs.len(),
        num_transforms,
        "Starting per-stream pipeline execution"
    );

    let source_manifest_limits = plugins
        .source_manifest
        .as_ref()
        .map(|m| m.limits.clone())
        .unwrap_or_default();
    let dest_manifest_limits = plugins
        .dest_manifest
        .as_ref()
        .map(|m| m.limits.clone())
        .unwrap_or_default();
    let transform_overrides: Vec<Option<SandboxOverrides>> = config
        .transforms
        .iter()
        .zip(modules.transform_modules.iter())
        .map(|(tc, tm)| {
            build_sandbox_overrides(
                tc.permissions.as_ref(),
                tc.limits.as_ref(),
                &tm.manifest_limits,
            )
        })
        .collect();

    let params = Arc::new(StreamParams {
        pipeline_name: config.pipeline.clone(),
        source_config: config.source.config.clone(),
        dest_config: config.destination.config.clone(),
        source_plugin_id,
        source_plugin_version,
        dest_plugin_id,
        dest_plugin_version,
        source_permissions: plugins.source_permissions.clone(),
        dest_permissions: plugins.dest_permissions.clone(),
        source_overrides: build_sandbox_overrides(
            config.source.permissions.as_ref(),
            config.source.limits.as_ref(),
            &source_manifest_limits,
        ),
        dest_overrides: build_sandbox_overrides(
            config.destination.permissions.as_ref(),
            config.destination.limits.as_ref(),
            &dest_manifest_limits,
        ),
        transform_overrides,
        compression: stream_build.compression,
        channel_capacity: (stream_build.limits.max_inflight_batches as usize).max(1),
    });

    let mut stream_join_set: JoinSet<Result<StreamResult, PipelineError>> = JoinSet::new();
    let run_dlq_records: Arc<Mutex<Vec<DlqRecord>>> = Arc::new(Mutex::new(Vec::new()));

    if !options.dry_run {
        let preflight_streams = destination_preflight_streams(&stream_build.stream_ctxs);
        let preflight_parallelism = parallelism.min(preflight_streams.len()).max(1);
        tracing::info!(
            unique_streams = preflight_streams.len(),
            preflight_parallelism,
            "Running destination DDL preflight before shard workers"
        );

        let mut preflight_join_set: JoinSet<Result<(), PipelineError>> = JoinSet::new();
        let preflight_semaphore = Arc::new(tokio::sync::Semaphore::new(preflight_parallelism));

        for stream_ctx in preflight_streams {
            let permit = preflight_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Preflight semaphore closed: {e}"
                    ))
                })?;

            let stream_name = stream_ctx.stream_name.clone();
            let state_dst = state.clone();
            let dest_module = modules.dest_module.clone();
            let params = params.clone();

            preflight_join_set.spawn(async move {
                let _permit = permit;
                let (tx, rx) = mpsc::channel::<Frame>(1);
                tx.send(Frame::EndStream).await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Failed to prime destination preflight channel for stream '{stream_name}': {e}",
                    ))
                })?;
                drop(tx);

                let preflight_result = tokio::task::spawn_blocking(move || {
                    run_destination_stream(
                        &dest_module,
                        rx,
                        Arc::new(Mutex::new(Vec::new())),
                        state_dst,
                        &params.pipeline_name,
                        &params.dest_plugin_id,
                        &params.dest_plugin_version,
                        &params.dest_config,
                        &stream_ctx,
                        Arc::new(Mutex::new(RunStats::default())),
                        params.dest_permissions.as_ref(),
                        params.compression,
                        params.dest_overrides.as_ref(),
                    )
                })
                .await
                .map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination preflight task panicked for stream '{stream_name}': {e}",
                    ))
                })??;

                tracing::info!(
                    stream = stream_name,
                    duration_secs = preflight_result.duration_secs,
                    "Destination preflight completed"
                );

                Ok(())
            });
        }

        while let Some(joined) = preflight_join_set.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    preflight_join_set.abort_all();
                    return Err(err);
                }
                Err(join_err) => {
                    preflight_join_set.abort_all();
                    return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination preflight join error: {join_err}"
                    )));
                }
            }
        }
    }

    for stream_ctx in &stream_build.stream_ctxs {
        let permit =
            semaphore.clone().acquire_owned().await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!("Semaphore closed: {e}"))
            })?;

        let params = params.clone();
        let source_module = modules.source_module.clone();
        let dest_module = modules.dest_module.clone();
        let state_src = state.clone();
        let state_dst = state.clone();
        let stats_src = stats.clone();
        let stats_dst = stats.clone();
        let stream_ctx = stream_ctx.clone();
        let run_dlq_records = run_dlq_records.clone();
        let transforms = modules.transform_modules.clone();
        let is_dry_run = options.dry_run;
        let dry_run_limit = options.limit;
        let progress_tx_for_stream = progress_tx.clone();

        let handle = tokio::spawn(async move {
            let num_t = transforms.len();
            let mut channels = Vec::with_capacity(num_t + 1);
            for _ in 0..=num_t {
                channels.push(mpsc::channel::<Frame>(params.channel_capacity));
            }

            let (mut senders, mut receivers): (
                Vec<mpsc::Sender<Frame>>,
                Vec<mpsc::Receiver<Frame>>,
            ) = channels.into_iter().unzip();

            let source_tx = senders.remove(0);
            let dest_rx = receivers.pop().ok_or_else(|| {
                PipelineError::Infrastructure(anyhow::anyhow!("Missing destination receiver"))
            })?;

            let stream_ctx_for_src = stream_ctx.clone();
            let stream_ctx_for_dst = stream_ctx.clone();

            // Build per-batch progress callback for the source runner
            let on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>> =
                progress_tx_for_stream.as_ref().map(|tx| {
                    let tx = tx.clone();
                    Arc::new(move |bytes: u64| {
                        let _ = tx.send(ProgressEvent::BatchEmitted { bytes });
                    }) as Arc<dyn Fn(u64) + Send + Sync>
                });

            let params_src = params.clone();
            let src_handle = tokio::task::spawn_blocking(move || {
                run_source_stream(
                    &source_module,
                    source_tx,
                    state_src,
                    &params_src.pipeline_name,
                    &params_src.source_plugin_id,
                    &params_src.source_plugin_version,
                    &params_src.source_config,
                    &stream_ctx_for_src,
                    stats_src,
                    params_src.source_permissions.as_ref(),
                    params_src.compression,
                    params_src.source_overrides.as_ref(),
                    on_emit,
                )
            });

            let mut transform_handles = Vec::with_capacity(num_t);
            for (i, t) in transforms.into_iter().enumerate() {
                let rx = receivers.remove(0);
                let tx = senders.remove(0);
                let state_t = state_dst.clone();
                let dlq_records_t = run_dlq_records.clone();
                let stream_ctx_t = stream_ctx.clone();
                let params_t = params.clone();
                let t_handle = tokio::task::spawn_blocking(move || {
                    run_transform_stream(
                        &t.module,
                        rx,
                        tx,
                        dlq_records_t,
                        state_t,
                        &params_t.pipeline_name,
                        &t.plugin_id,
                        &t.plugin_version,
                        &t.config,
                        &stream_ctx_t,
                        t.permissions.as_ref(),
                        params_t.compression,
                        params_t.transform_overrides.get(i).and_then(Option::as_ref),
                    )
                });
                transform_handles.push((i, t_handle));
            }

            if is_dry_run {
                // Dry-run: collect frames instead of running destination plugin
                let compression = params.compression;
                let collector_handle =
                    tokio::spawn(collect_dry_run_frames(dest_rx, dry_run_limit, compression));

                let src_result = src_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Source task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                let transforms =
                    collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

                let mut collected = collector_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Dry-run collector task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })??;

                drop(permit);

                if let Some(transform_err) = transforms.first_error {
                    return Err(transform_err);
                }

                let src = src_result?;
                collected
                    .stream_name
                    .clone_from(&stream_ctx_for_dst.stream_name);

                Ok(StreamResult {
                    stream_name: stream_ctx.stream_name.clone(),
                    partition_index: stream_ctx.partition_index,
                    partition_count: stream_ctx.partition_count,
                    read_summary: src.summary,
                    write_summary: WriteSummary {
                        records_written: 0,
                        bytes_written: 0,
                        batches_written: 0,
                        checkpoint_count: 0,
                        records_failed: 0,
                        perf: None,
                    },
                    source_checkpoints: src.checkpoints,
                    dest_checkpoints: Vec::new(),
                    src_host_timings: src.host_timings,
                    dst_host_timings: HostTimings::default(),
                    src_duration: src.duration_secs,
                    dst_duration: 0.0,
                    vm_setup_secs: 0.0,
                    recv_secs: 0.0,
                    transform_durations: transforms.durations,
                    dry_run_result: Some(collected),
                })
            } else {
                // Normal mode: run destination plugin
                let dst_handle = tokio::task::spawn_blocking(move || {
                    run_destination_stream(
                        &dest_module,
                        dest_rx,
                        run_dlq_records,
                        state_dst,
                        &params.pipeline_name,
                        &params.dest_plugin_id,
                        &params.dest_plugin_version,
                        &params.dest_config,
                        &stream_ctx_for_dst,
                        stats_dst,
                        params.dest_permissions.as_ref(),
                        params.compression,
                        params.dest_overrides.as_ref(),
                    )
                });

                let src_result = src_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Source task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                let transforms =
                    collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

                let dst_result = dst_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                drop(permit);

                if let Some(transform_err) = transforms.first_error {
                    return Err(transform_err);
                }

                let src = src_result?;

                let dst = dst_result?;

                Ok(StreamResult {
                    stream_name: stream_ctx.stream_name.clone(),
                    partition_index: stream_ctx.partition_index,
                    partition_count: stream_ctx.partition_count,
                    read_summary: src.summary,
                    write_summary: dst.summary,
                    source_checkpoints: src.checkpoints,
                    dest_checkpoints: dst.checkpoints,
                    src_host_timings: src.host_timings,
                    dst_host_timings: dst.host_timings,
                    src_duration: src.duration_secs,
                    dst_duration: dst.duration_secs,
                    vm_setup_secs: dst.vm_setup_secs,
                    recv_secs: dst.recv_secs,
                    transform_durations: transforms.durations,
                    dry_run_result: None,
                })
            }
        });

        stream_join_set.spawn(async move {
            handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!("Stream task panicked: {e}"))
            })?
        });
    }

    let mut source_checkpoints = Vec::new();
    let mut dest_checkpoints = Vec::new();
    let mut total_read_summary = ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    };
    let mut total_write_summary = WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    };
    let mut src_timings = HostTimings::default();
    let mut dst_timings = HostTimings::default();
    let mut src_timing_maxima = SourceTimingMaxima::default();
    let mut dst_timing_maxima = DestTimingMaxima::default();
    let mut transform_durations = Vec::new();
    let mut dry_run_streams: Vec<DryRunStreamResult> = Vec::new();
    let mut stream_metrics: Vec<StreamShardMetric> = Vec::new();
    let stream_collection = collect_stream_task_results(stream_join_set, progress_tx).await?;

    for sr in stream_collection.successes {
        observe_source_timing(&mut src_timing_maxima, &sr);
        observe_dest_timing(&mut dst_timing_maxima, &sr);

        total_read_summary.records_read += sr.read_summary.records_read;
        total_read_summary.bytes_read += sr.read_summary.bytes_read;
        total_read_summary.batches_emitted += sr.read_summary.batches_emitted;
        total_read_summary.checkpoint_count += sr.read_summary.checkpoint_count;
        total_read_summary.records_skipped += sr.read_summary.records_skipped;

        total_write_summary.records_written += sr.write_summary.records_written;
        total_write_summary.bytes_written += sr.write_summary.bytes_written;
        total_write_summary.batches_written += sr.write_summary.batches_written;
        total_write_summary.checkpoint_count += sr.write_summary.checkpoint_count;
        total_write_summary.records_failed += sr.write_summary.records_failed;

        source_checkpoints.extend(sr.source_checkpoints);
        dest_checkpoints.extend(sr.dest_checkpoints);

        src_timings.emit_batch_nanos += sr.src_host_timings.emit_batch_nanos;
        src_timings.compress_nanos += sr.src_host_timings.compress_nanos;
        src_timings.emit_batch_count += sr.src_host_timings.emit_batch_count;
        src_timings.source_connect_secs += sr.src_host_timings.source_connect_secs;
        src_timings.source_query_secs += sr.src_host_timings.source_query_secs;
        src_timings.source_fetch_secs += sr.src_host_timings.source_fetch_secs;
        src_timings.source_arrow_encode_secs += sr.src_host_timings.source_arrow_encode_secs;
        dst_timings.next_batch_nanos += sr.dst_host_timings.next_batch_nanos;
        dst_timings.next_batch_wait_nanos += sr.dst_host_timings.next_batch_wait_nanos;
        dst_timings.next_batch_process_nanos += sr.dst_host_timings.next_batch_process_nanos;
        dst_timings.decompress_nanos += sr.dst_host_timings.decompress_nanos;
        dst_timings.next_batch_count += sr.dst_host_timings.next_batch_count;
        dst_timings.dest_connect_secs += sr.dst_host_timings.dest_connect_secs;
        dst_timings.dest_flush_secs += sr.dst_host_timings.dest_flush_secs;
        dst_timings.dest_commit_secs += sr.dst_host_timings.dest_commit_secs;
        dst_timings.dest_arrow_decode_secs += sr.dst_host_timings.dest_arrow_decode_secs;

        transform_durations.extend(sr.transform_durations);

        stream_metrics.push(StreamShardMetric {
            stream_name: sr.stream_name,
            partition_index: sr.partition_index,
            partition_count: sr.partition_count,
            records_read: sr.read_summary.records_read,
            records_written: sr.write_summary.records_written,
            bytes_read: sr.read_summary.bytes_read,
            bytes_written: sr.write_summary.bytes_written,
            source_duration_secs: sr.src_duration,
            dest_duration_secs: sr.dst_duration,
            dest_vm_setup_secs: sr.vm_setup_secs,
            dest_recv_secs: sr.recv_secs,
        });

        if let Some(dr) = sr.dry_run_result {
            dry_run_streams.push(dr);
        }
    }

    // Sanity-check partitioned read results
    {
        let shard_row_counts: Vec<u64> = stream_metrics
            .iter()
            .filter(|m| m.partition_index.is_some())
            .map(|m| m.records_read)
            .collect();

        if !shard_row_counts.is_empty() {
            if shard_row_counts.contains(&0) {
                tracing::warn!(
                    "PartitionedRead sanity check: one or more shards returned 0 rows \
                     — the source may not be honoring partition coordinates"
                );
            }
            if shard_row_counts.len() > 1
                && shard_row_counts.iter().all(|&c| c == shard_row_counts[0])
                && shard_row_counts[0] > 100
            {
                tracing::warn!(
                    shard_count = shard_row_counts.len(),
                    rows_per_shard = shard_row_counts[0],
                    "PartitionedRead sanity check: all shards returned identical row counts \
                     — the source may be ignoring partition coordinates"
                );
            }
        }
    }

    let first_error = stream_collection.first_error;

    let dlq_records = run_dlq_records
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("DLQ collection mutex poisoned"))
        })?
        .drain(..)
        .collect();

    let final_stats = stats
        .lock()
        .map_err(|_| PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned")))?
        .clone();

    Ok(AggregatedStreamResults {
        total_read_summary,
        total_write_summary,
        source_checkpoints,
        dest_checkpoints,
        src_timings,
        dst_timings,
        src_timing_maxima,
        dst_timing_maxima,
        transform_durations,
        dlq_records,
        final_stats,
        first_error,
        dry_run_streams,
        stream_metrics,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn finalize_run(
    config: &PipelineConfig,
    pipeline_id: &PipelineId,
    state: Arc<dyn StateBackend>,
    run_id: i64,
    attempt: u32,
    start: Instant,
    modules: &LoadedModules,
    mut aggregated: AggregatedStreamResults,
) -> Result<PipelineResult, PipelineError> {
    if let Some(err) = aggregated.first_error {
        let state_for_complete = state.clone();
        let run_stats = RunStats {
            records_read: aggregated.final_stats.records_read,
            records_written: aggregated.final_stats.records_written,
            bytes_read: aggregated.final_stats.bytes_read,
            error_message: Some(format!("Stream error: {err}")),
        };
        tokio::task::spawn_blocking(move || {
            state_for_complete.complete_run(run_id, RunStatus::Failed, &run_stats)
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("complete_run task panicked: {e}"))
        })?
        .map_err(|e| PipelineError::Infrastructure(e.into()))?;

        let state_for_dlq = state.clone();
        let pipeline_id_for_dlq = pipeline_id.clone();
        let dlq_records = std::mem::take(&mut aggregated.dlq_records);
        tokio::task::spawn_blocking(move || {
            crate::dlq::persist_dlq_records(
                state_for_dlq.as_ref(),
                &pipeline_id_for_dlq,
                run_id,
                &dlq_records,
            );
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("persist_dlq_records task panicked: {e}"))
        })?;

        return Err(err);
    }

    let plugin_internal_secs = aggregated.dst_timing_maxima.connect_secs
        + aggregated.dst_timing_maxima.flush_secs
        + aggregated.dst_timing_maxima.commit_secs;
    let wasm_overhead_secs = (aggregated.dst_timing_maxima.duration_secs
        - aggregated.dst_timing_maxima.vm_setup_secs
        - aggregated.dst_timing_maxima.recv_secs
        - plugin_internal_secs)
        .max(0.0);

    let state_for_complete = state.clone();
    let complete_stats = RunStats {
        records_read: aggregated.total_read_summary.records_read,
        records_written: aggregated.total_write_summary.records_written,
        bytes_read: aggregated.total_read_summary.bytes_read,
        error_message: None,
    };
    tokio::task::spawn_blocking(move || {
        state_for_complete.complete_run(run_id, RunStatus::Completed, &complete_stats)
    })
    .await
    .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!("complete_run task panicked: {e}")))?
    .map_err(|e| PipelineError::Infrastructure(e.into()))?;

    tracing::debug!(
        pipeline = config.pipeline,
        source_checkpoint_count = aggregated.source_checkpoints.len(),
        dest_checkpoint_count = aggregated.dest_checkpoints.len(),
        "About to correlate checkpoints"
    );

    let state_for_cursor = state.clone();
    let pipeline_id_for_cursor = pipeline_id.clone();
    let source_checkpoints = std::mem::take(&mut aggregated.source_checkpoints);
    let dest_checkpoints = std::mem::take(&mut aggregated.dest_checkpoints);
    let cursors_advanced = tokio::task::spawn_blocking(move || {
        correlate_and_persist_cursors(
            state_for_cursor.as_ref(),
            &pipeline_id_for_cursor,
            &source_checkpoints,
            &dest_checkpoints,
        )
    })
    .await
    .map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "correlate_and_persist_cursors task panicked: {e}"
        ))
    })?
    .map_err(PipelineError::Infrastructure)?;
    if cursors_advanced > 0 {
        tracing::info!(
            pipeline = config.pipeline,
            cursors_advanced,
            "Checkpoint coordination complete"
        );
    }

    let state_for_dlq = state.clone();
    let pipeline_id_for_dlq = pipeline_id.clone();
    let dlq_records = std::mem::take(&mut aggregated.dlq_records);
    tokio::task::spawn_blocking(move || {
        crate::dlq::persist_dlq_records(
            state_for_dlq.as_ref(),
            &pipeline_id_for_dlq,
            run_id,
            &dlq_records,
        );
    })
    .await
    .map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("persist_dlq_records task panicked: {e}"))
    })?;

    let duration = start.elapsed();
    let src_perf = aggregated.total_read_summary.perf.as_ref();
    let perf = aggregated.total_write_summary.perf.as_ref();
    let transform_module_load_ms = modules
        .transform_modules
        .iter()
        .map(|m| m.load_ms)
        .collect::<Vec<_>>();

    tracing::info!(
        pipeline = config.pipeline,
        records_read = aggregated.total_read_summary.records_read,
        records_written = aggregated.total_write_summary.records_written,
        duration_secs = duration.as_secs_f64(),
        "Pipeline run completed"
    );

    Ok(PipelineResult {
        counts: PipelineCounts {
            records_read: aggregated.total_read_summary.records_read,
            records_written: aggregated.total_write_summary.records_written,
            bytes_read: aggregated.total_read_summary.bytes_read,
            bytes_written: aggregated.total_write_summary.bytes_written,
        },
        source: build_source_timing(
            &aggregated.src_timing_maxima,
            &aggregated.src_timings,
            src_perf,
            modules.source_module_load_ms,
        ),
        dest: DestTiming {
            duration_secs: aggregated.dst_timing_maxima.duration_secs,
            module_load_ms: modules.dest_module_load_ms,
            connect_secs: perf.map_or(aggregated.dst_timing_maxima.connect_secs, |p| {
                p.connect_secs
            }),
            flush_secs: perf.map_or(aggregated.dst_timing_maxima.flush_secs, |p| p.flush_secs),
            commit_secs: perf.map_or(aggregated.dst_timing_maxima.commit_secs, |p| p.commit_secs),
            arrow_decode_secs: perf.map_or(aggregated.dst_timing_maxima.arrow_decode_secs, |p| {
                p.arrow_decode_secs
            }),
            vm_setup_secs: aggregated.dst_timing_maxima.vm_setup_secs,
            recv_secs: aggregated.dst_timing_maxima.recv_secs,
            recv_nanos: aggregated.dst_timings.next_batch_nanos,
            recv_wait_nanos: aggregated.dst_timings.next_batch_wait_nanos,
            recv_process_nanos: aggregated.dst_timings.next_batch_process_nanos,
            decompress_nanos: aggregated.dst_timings.decompress_nanos,
            recv_count: aggregated.dst_timings.next_batch_count,
        },
        transform_count: aggregated.transform_durations.len(),
        transform_duration_secs: aggregated.transform_durations.iter().sum(),
        transform_module_load_ms,
        duration_secs: duration.as_secs_f64(),
        wasm_overhead_secs,
        retry_count: attempt.saturating_sub(1),
        parallelism: resolve_effective_parallelism(config, false),
        stream_metrics: aggregated.stream_metrics,
    })
}

/// Collect frames from a channel, decode IPC, enforce row limit.
/// Used in dry-run mode instead of the destination runner.
async fn collect_dry_run_frames(
    mut receiver: mpsc::Receiver<Frame>,
    limit: Option<u64>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
) -> Result<DryRunStreamResult, PipelineError> {
    let mut batches = Vec::new();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;

    'recv: while let Some(frame) = receiver.recv().await {
        let Frame::Data { payload: data, .. } = frame else {
            break 'recv;
        };
        let ipc_bytes = match compression {
            Some(codec) => {
                rapidbyte_runtime::compression::decompress(codec, &data).map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Dry-run decompression failed: {e}"
                    ))
                })?
            }
            None => data.to_vec(),
        };

        let decoded = ipc_to_record_batches(&ipc_bytes).map_err(PipelineError::Infrastructure)?;

        for batch in decoded {
            let rows = batch.num_rows() as u64;
            total_bytes += batch.get_array_memory_size() as u64;

            if let Some(max) = limit {
                let remaining = max.saturating_sub(total_rows);
                if remaining == 0 {
                    break 'recv;
                }
                if rows > remaining {
                    #[allow(clippy::cast_possible_truncation)]
                    batches.push(batch.slice(0, remaining as usize));
                    total_rows += remaining;
                    break 'recv;
                }
            }

            total_rows += rows;
            batches.push(batch);
        }
    }

    Ok(DryRunStreamResult {
        stream_name: String::new(),
        batches,
        total_rows,
        total_bytes,
    })
}

/// Check a pipeline: validate configuration and connectivity without running.
///
/// # Errors
///
/// Returns an error if plugin resolution, module loading, or validation fails.
#[allow(clippy::too_many_lines)]
pub async fn check_pipeline(config: &PipelineConfig) -> Result<CheckResult> {
    tracing::info!(
        pipeline = config.pipeline,
        "Checking pipeline configuration"
    );

    let plugins = resolve_plugins(config).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if plugins.source_manifest.is_some() {
        println!("Source manifest:    OK");
    }
    if plugins.dest_manifest.is_some() {
        println!("Dest manifest:      OK");
    }

    if let Some(ref sm) = plugins.source_manifest {
        match validate_config_against_schema(&config.source.use_ref, &config.source.config, sm) {
            Ok(()) => println!("Source config:      OK"),
            Err(e) => println!("Source config:      FAILED\n  {e}"),
        }
    }
    if let Some(ref dm) = plugins.dest_manifest {
        match validate_config_against_schema(
            &config.destination.use_ref,
            &config.destination.config,
            dm,
        ) {
            Ok(()) => println!("Dest config:        OK"),
            Err(e) => println!("Dest config:        FAILED\n  {e}"),
        }
    }

    let state_ok = check_state_backend(config);

    let source_config = config.source.config.clone();
    let source_permissions = plugins.source_permissions.clone();
    let (src_id, src_ver) = parse_plugin_ref(&config.source.use_ref);
    let source_wasm = plugins.source_wasm.clone();
    let source_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_plugin(
                &source_wasm,
                PluginKind::Source,
                &src_id,
                &src_ver,
                &source_config,
                source_permissions.as_ref(),
            )
        });

    let dest_config = config.destination.config.clone();
    let dest_permissions = plugins.dest_permissions.clone();
    let (dst_id, dst_ver) = parse_plugin_ref(&config.destination.use_ref);
    let dest_wasm = plugins.dest_wasm.clone();
    let dest_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_plugin(
                &dest_wasm,
                PluginKind::Destination,
                &dst_id,
                &dst_ver,
                &dest_config,
                dest_permissions.as_ref(),
            )
        });

    let source_validation = source_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Source validation task panicked: {e}"))??;
    let dest_validation = dest_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Destination validation task panicked: {e}"))??;

    let mut transform_tasks = Vec::with_capacity(config.transforms.len());
    for (index, tc) in config.transforms.iter().enumerate() {
        let wasm_path = rapidbyte_runtime::resolve_plugin_path(&tc.use_ref, PluginKind::Transform)?;
        let manifest = load_and_validate_manifest(&wasm_path, &tc.use_ref, PluginKind::Transform)?;
        if let Some(ref m) = manifest {
            match validate_config_against_schema(&tc.use_ref, &tc.config, m) {
                Ok(()) => println!("Transform config ({}): OK", tc.use_ref),
                Err(e) => println!("Transform config ({}): FAILED\n  {}", tc.use_ref, e),
            }
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let config_val = tc.config.clone();
        let plugin_ref = tc.use_ref.clone();
        let (tc_id, tc_ver) = parse_plugin_ref(&tc.use_ref);
        let handle = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_plugin(
                &wasm_path,
                PluginKind::Transform,
                &tc_id,
                &tc_ver,
                &config_val,
                transform_perms.as_ref(),
            )
        });
        transform_tasks.push((index, plugin_ref, handle));
    }

    let mut transform_validations = Vec::with_capacity(transform_tasks.len());
    for (index, plugin_ref, handle) in transform_tasks {
        let result = handle.await.map_err(|e| {
            anyhow::anyhow!("Transform validation task panicked (index {index}, {plugin_ref}): {e}")
        })??;
        transform_validations.push(result);
    }

    Ok(CheckResult {
        source_validation,
        destination_validation: dest_validation,
        transform_validations,
        state_ok,
    })
}

/// Discover available streams from a source plugin.
///
/// # Errors
///
/// Returns an error if the plugin cannot be loaded, opened, or discovery fails.
pub async fn discover_plugin(plugin_ref: &str, config: &serde_json::Value) -> Result<Catalog> {
    let wasm_path = rapidbyte_runtime::resolve_plugin_path(plugin_ref, PluginKind::Source)?;
    let manifest = load_and_validate_manifest(&wasm_path, plugin_ref, PluginKind::Source)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());
    let (plugin_id, plugin_version) = parse_plugin_ref(plugin_ref);
    let config = config.clone();

    tokio::task::spawn_blocking(move || {
        run_discover(
            &wasm_path,
            &plugin_id,
            &plugin_version,
            &config,
            permissions.as_ref(),
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("Discover task panicked: {e}"))?
}

#[cfg(test)]
mod stream_task_collection_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn success_result(stream_name: &str) -> StreamResult {
        StreamResult {
            stream_name: stream_name.to_string(),
            partition_index: None,
            partition_count: None,
            read_summary: ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            },
            write_summary: WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            },
            source_checkpoints: Vec::new(),
            dest_checkpoints: Vec::new(),
            src_host_timings: HostTimings::default(),
            dst_host_timings: HostTimings::default(),
            src_duration: 0.0,
            dst_duration: 0.0,
            vm_setup_secs: 0.0,
            recv_secs: 0.0,
            transform_durations: Vec::new(),
            dry_run_result: None,
        }
    }

    #[tokio::test]
    async fn collect_stream_tasks_fails_fast_and_cancels_siblings() {
        let mut join_set: JoinSet<Result<StreamResult, PipelineError>> = JoinSet::new();
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(success_result("slow_stream"))
        });
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Err(PipelineError::Infrastructure(anyhow::anyhow!(
                "expected failure"
            )))
        });

        let start = Instant::now();
        let collected = collect_stream_task_results(join_set, &None)
            .await
            .expect("collector should return first error, not infra panic");

        assert!(collected.first_error.is_some());
        assert!(collected.successes.is_empty());
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}

#[cfg(test)]
mod dry_run_tests {
    use super::*;
    use crate::arrow::record_batch_to_ipc;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let max = i64::try_from(n).expect("test row count must fit in i64");
        let ids: Vec<i64> = (0..max).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(names)) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn collect_dry_run_frames_basic() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        let batch = make_test_batch(5);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .await
        .unwrap();
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, None, None).await.unwrap();
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.batches.len(), 1);
    }

    #[tokio::test]
    async fn collect_dry_run_frames_with_limit() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        let batch = make_test_batch(100);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .await
        .unwrap();
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, Some(10), None).await.unwrap();
        assert_eq!(result.total_rows, 10);
        let total: usize = result.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 10);
    }

    #[tokio::test]
    async fn collect_dry_run_frames_multiple_batches() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        for _ in 0..3 {
            let batch = make_test_batch(5);
            let ipc = record_batch_to_ipc(&batch).unwrap();
            tx.send(Frame::Data {
                payload: bytes::Bytes::from(ipc),
                checkpoint_id: 1,
            })
            .await
            .unwrap();
        }
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, Some(12), None).await.unwrap();
        assert_eq!(result.total_rows, 12);
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod stream_context_partition_tests {
    use super::*;
    use crate::config::types::PipelineConfig;
    use rapidbyte_state::SqliteStateBackend;
    use rapidbyte_types::manifest::{Roles, SourceCapabilities};
    use rapidbyte_types::wire::ProtocolVersion;

    fn test_manifest_with_partitioned_read() -> PluginManifest {
        PluginManifest {
            id: "test/pg".to_string(),
            name: "Test".to_string(),
            version: "0.1.0".to_string(),
            description: String::new(),
            author: None,
            license: None,
            protocol_version: ProtocolVersion::V5,
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh],
                    features: vec![Feature::PartitionedRead],
                }),
                destination: None,
                transform: None,
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            config_schema: None,
        }
    }

    fn config_with_parallelism(parallelism: u32, sync_mode: &str) -> PipelineConfig {
        config_with_parallelism_and_write_mode(parallelism, sync_mode, "append")
    }

    fn config_with_parallelism_and_write_mode(
        parallelism: u32,
        sync_mode: &str,
        write_mode: &str,
    ) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test_partitioning
source:
  use: postgres
  config: {{}}
  streams:
    - name: bench_events
      sync_mode: {sync_mode}
destination:
  use: postgres
  config: {{}}
  write_mode: {write_mode}
resources:
  parallelism: {parallelism}
"#
        );
        serde_yaml::from_str(&yaml).expect("valid pipeline yaml")
    }

    fn config_with_parallelism_expr(
        parallelism: &str,
        source_use: &str,
        sync_mode: &str,
        write_mode: &str,
        transform_count: usize,
    ) -> PipelineConfig {
        let transforms_yaml = if transform_count == 0 {
            String::new()
        } else {
            let mut transforms = String::from("transforms:\n");
            for _ in 0..transform_count {
                transforms.push_str("  - use: sql\n    config: {}\n");
            }
            transforms
        };

        let yaml = format!(
            r#"
version: "1.0"
pipeline: test_partitioning
source:
  use: {source_use}
  config: {{}}
  streams:
    - name: bench_events
      sync_mode: {sync_mode}
{transforms_yaml}destination:
  use: postgres
  config: {{}}
  write_mode: {write_mode}
resources:
  parallelism: {parallelism}
"#
        );
        serde_yaml::from_str(&yaml).expect("valid pipeline yaml")
    }

    #[test]
    fn full_refresh_with_parallelism_fans_out_stream_contexts() {
        let config = config_with_parallelism(4, "full_refresh");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 4);
        assert_eq!(
            build
                .stream_ctxs
                .iter()
                .map(|ctx| ctx.stream_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "bench_events",
                "bench_events",
                "bench_events",
                "bench_events"
            ]
        );
        for (idx, stream_ctx) in build.stream_ctxs.iter().enumerate() {
            assert_eq!(stream_ctx.sync_mode, SyncMode::FullRefresh);
            assert_eq!(
                stream_ctx.source_stream_name.as_deref(),
                Some("bench_events")
            );
            assert_eq!(stream_ctx.partition_count, Some(4));
            assert_eq!(
                stream_ctx.partition_index,
                Some(u32::try_from(idx).expect("partition index should fit in u32"))
            );
        }
    }

    #[test]
    fn incremental_streams_remain_unpartitioned() {
        let config = config_with_parallelism(4, "incremental");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 1);
        let stream_ctx = &build.stream_ctxs[0];
        assert_eq!(stream_ctx.stream_name, "bench_events");
        assert_eq!(stream_ctx.source_stream_name, None);
        assert_eq!(stream_ctx.partition_count, None);
        assert_eq!(stream_ctx.partition_index, None);
    }

    #[test]
    fn replace_mode_streams_remain_unpartitioned() {
        let config = config_with_parallelism_and_write_mode(4, "full_refresh", "replace");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 1);
        let stream_ctx = &build.stream_ctxs[0];
        assert_eq!(stream_ctx.stream_name, "bench_events");
        assert_eq!(stream_ctx.source_stream_name, None);
        assert_eq!(stream_ctx.partition_count, None);
        assert_eq!(stream_ctx.partition_index, None);
        assert_eq!(stream_ctx.write_mode, Some(WriteMode::Replace));
    }

    #[test]
    fn decode_incremental_last_value_wraps_legacy_scalar_for_tie_breaker_streams() {
        let value = decode_incremental_last_value("42".to_string(), Some("id"))
            .expect("legacy scalar cursor should decode");

        match value {
            CursorValue::Json { value } => {
                assert_eq!(value["cursor"]["type"], "utf8");
                assert_eq!(value["cursor"]["value"], "42");
                assert_eq!(value["tie_breaker"]["type"], "null");
            }
            other => panic!("expected wrapped composite cursor, got {other:?}"),
        }
    }

    #[test]
    fn decode_incremental_last_value_preserves_composite_json() {
        let raw = r#"{"cursor":{"type":"utf8","value":"2024-01-01T00:00:00Z"},"tie_breaker":{"type":"int64","value":7}}"#;
        let value = decode_incremental_last_value(raw.to_string(), Some("id"))
            .expect("composite cursor should decode");

        match value {
            CursorValue::Json { value } => {
                assert_eq!(value["cursor"]["value"], "2024-01-01T00:00:00Z");
                assert_eq!(value["tie_breaker"]["value"], 7);
            }
            other => panic!("expected composite cursor json, got {other:?}"),
        }
    }

    #[test]
    fn auto_parallelism_without_eligible_streams_resolves_to_one() {
        let config = config_with_parallelism_expr("auto", "postgres", "incremental", "append", 0);
        assert_eq!(resolve_effective_parallelism(&config, true), 1);
    }

    #[test]
    fn auto_parallelism_uses_adaptive_core_budget() {
        let config = config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 0);

        assert_eq!(resolve_auto_parallelism_for_cores(&config, 16, true), 12);
        assert_eq!(resolve_auto_parallelism_for_cores(&config, 8, true), 5);
        assert_eq!(resolve_auto_parallelism_for_cores(&config, 4, true), 2);
    }

    #[test]
    fn manual_parallelism_override_is_honored() {
        let config = config_with_parallelism_expr("7", "postgres", "full_refresh", "append", 0);
        assert_eq!(resolve_effective_parallelism(&config, true), 7);
    }

    #[test]
    fn transform_count_reduces_auto_parallelism() {
        let without_transforms =
            config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 0);
        let with_transforms =
            config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 3);

        assert!(
            resolve_auto_parallelism_for_cores(&with_transforms, 16, true)
                <= resolve_auto_parallelism_for_cores(&without_transforms, 16, true)
        );
    }

    #[test]
    fn auto_parallelism_scales_with_multiple_streams() {
        let yaml = r#"
version: "1.0"
pipeline: bench_pg
source:
  use: postgres
  config: {}
  streams:
    - name: a
      sync_mode: full_refresh
    - name: b
      sync_mode: full_refresh
    - name: c
      sync_mode: full_refresh
destination:
  use: postgres
  config: {}
  write_mode: append
resources:
  parallelism: auto
state:
  backend: sqlite
  connection: ":memory:"
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).expect("valid pipeline yaml");

        // 16 cores => 12 worker cores after reserves; split across 3 streams => 4 shards each.
        assert_eq!(resolve_auto_parallelism_for_cores(&config, 16, true), 12);
    }

    #[test]
    fn execution_parallelism_prefers_stream_context_override() {
        let config = config_with_parallelism_expr("2", "postgres", "full_refresh", "append", 0);
        let stream_ctxs = vec![StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: Some("users".to_string()),
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
            partition_key: None,
            partition_count: Some(5),
            partition_index: Some(0),
            effective_parallelism: Some(5),
            partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
            copy_flush_bytes_override: None,
        }];

        assert_eq!(execution_parallelism(&config, &stream_ctxs), 5);
    }

    #[test]
    fn execution_parallelism_falls_back_to_pipeline_setting() {
        let config = config_with_parallelism_expr("3", "postgres", "full_refresh", "append", 0);
        let stream_ctxs = vec![StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }];

        assert_eq!(execution_parallelism(&config, &stream_ctxs), 3);
    }

    #[test]
    fn destination_preflight_deduplicates_partitioned_streams() {
        let stream_ctxs = vec![
            StreamContext {
                stream_name: "bench_events".to_string(),
                source_stream_name: Some("bench_events".to_string()),
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: Some(4),
                partition_index: Some(0),
                effective_parallelism: Some(4),
                partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "bench_events".to_string(),
                source_stream_name: Some("bench_events".to_string()),
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: Some(4),
                partition_index: Some(3),
                effective_parallelism: Some(4),
                partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "users".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::Incremental,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
        ];

        let preflight = destination_preflight_streams(&stream_ctxs);
        assert_eq!(preflight.len(), 2);
        assert_eq!(preflight[0].stream_name, "bench_events");
        assert_eq!(preflight[0].partition_count, None);
        assert_eq!(preflight[0].partition_index, None);
        assert_eq!(preflight[1].stream_name, "users");
        assert_eq!(preflight[1].partition_count, None);
        assert_eq!(preflight[1].partition_index, None);
    }

    #[test]
    fn destination_preflight_skips_replace_mode_streams() {
        let stream_ctxs = vec![
            StreamContext {
                stream_name: "users_replace".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: Some(WriteMode::Replace),
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "orders_append".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: Some(WriteMode::Append),
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
        ];

        let preflight = destination_preflight_streams(&stream_ctxs);
        assert_eq!(preflight.len(), 1);
        assert_eq!(preflight[0].stream_name, "orders_append");
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default, clippy::float_cmp)]
mod dest_timing_maxima_tests {
    use super::*;

    fn stream_result_with_dest_timing(
        duration_secs: f64,
        vm_setup_secs: f64,
        recv_secs: f64,
        connect_secs: f64,
        flush_secs: f64,
        commit_secs: f64,
        arrow_decode_secs: f64,
    ) -> StreamResult {
        let mut dst_host_timings = HostTimings::default();
        dst_host_timings.dest_connect_secs = connect_secs;
        dst_host_timings.dest_flush_secs = flush_secs;
        dst_host_timings.dest_commit_secs = commit_secs;
        dst_host_timings.dest_arrow_decode_secs = arrow_decode_secs;

        StreamResult {
            stream_name: "users".to_string(),
            partition_index: Some(0),
            partition_count: Some(2),
            read_summary: ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            },
            write_summary: WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            },
            source_checkpoints: Vec::new(),
            dest_checkpoints: Vec::new(),
            src_host_timings: HostTimings::default(),
            dst_host_timings,
            src_duration: 0.0,
            dst_duration: duration_secs,
            vm_setup_secs,
            recv_secs,
            transform_durations: Vec::new(),
            dry_run_result: None,
        }
    }

    #[test]
    fn observe_dest_timing_tracks_independent_maxima() {
        let mut maxima = DestTimingMaxima::default();

        let stream_a = stream_result_with_dest_timing(10.0, 1.0, 2.0, 0.5, 4.0, 1.0, 0.2);
        let stream_b = stream_result_with_dest_timing(8.0, 3.0, 5.0, 0.7, 2.0, 1.5, 0.4);

        observe_dest_timing(&mut maxima, &stream_a);
        observe_dest_timing(&mut maxima, &stream_b);

        assert_eq!(maxima.duration_secs, 10.0);
        assert_eq!(maxima.vm_setup_secs, 3.0);
        assert_eq!(maxima.recv_secs, 5.0);
        assert_eq!(maxima.connect_secs, 0.7);
        assert_eq!(maxima.flush_secs, 4.0);
        assert_eq!(maxima.commit_secs, 1.5);
        assert_eq!(maxima.arrow_decode_secs, 0.4);
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default, clippy::float_cmp)]
mod source_timing_maxima_tests {
    use super::*;

    fn stream_result_with_source_timing(
        duration_secs: f64,
        connect_secs: f64,
        query_secs: f64,
        fetch_secs: f64,
        arrow_encode_secs: f64,
    ) -> StreamResult {
        let mut src_host_timings = HostTimings::default();
        src_host_timings.source_connect_secs = connect_secs;
        src_host_timings.source_query_secs = query_secs;
        src_host_timings.source_fetch_secs = fetch_secs;
        src_host_timings.source_arrow_encode_secs = arrow_encode_secs;

        StreamResult {
            stream_name: "users".to_string(),
            partition_index: Some(0),
            partition_count: Some(2),
            read_summary: ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            },
            write_summary: WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            },
            source_checkpoints: Vec::new(),
            dest_checkpoints: Vec::new(),
            src_host_timings,
            dst_host_timings: HostTimings::default(),
            src_duration: duration_secs,
            dst_duration: 0.0,
            vm_setup_secs: 0.0,
            recv_secs: 0.0,
            transform_durations: Vec::new(),
            dry_run_result: None,
        }
    }

    #[test]
    fn observe_source_timing_tracks_independent_maxima() {
        let mut maxima = SourceTimingMaxima::default();

        let stream_a = stream_result_with_source_timing(10.0, 1.0, 2.0, 5.0, 0.6);
        let stream_b = stream_result_with_source_timing(8.0, 3.0, 1.0, 4.0, 0.9);

        observe_source_timing(&mut maxima, &stream_a);
        observe_source_timing(&mut maxima, &stream_b);

        assert_eq!(maxima.duration_secs, 10.0);
        assert_eq!(maxima.connect_secs, 3.0);
        assert_eq!(maxima.query_secs, 2.0);
        assert_eq!(maxima.fetch_secs, 5.0);
        assert_eq!(maxima.arrow_encode_secs, 0.9);
    }
}
