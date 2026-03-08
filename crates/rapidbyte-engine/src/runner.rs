//! Plugin runner utilities for source, destination, transform, validate, and discover flows.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{ReadSummary, TransformSummary, WriteSummary};
use rapidbyte_types::stream::StreamContext;
use rapidbyte_types::wire::PluginKind;

use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, dest_validation_to_sdk,
    source_bindings, source_error_to_sdk, source_validation_to_sdk, transform_bindings,
    transform_error_to_sdk, transform_validation_to_sdk, ComponentHostState, CompressionCodec,
    Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::state::RunStats;

use crate::error::PipelineError;

/// Result of running a source plugin for a single stream.
pub(crate) struct SourceRunResult {
    pub duration_secs: f64,
    pub summary: ReadSummary,
    pub checkpoints: Vec<Checkpoint>,
    pub host_timings: HostTimings,
}

/// Result of running a destination plugin for a single stream.
pub(crate) struct DestRunResult {
    pub duration_secs: f64,
    pub summary: WriteSummary,
    pub vm_setup_secs: f64,
    pub recv_secs: f64,
    pub checkpoints: Vec<Checkpoint>,
    pub host_timings: HostTimings,
}

/// Result of running a transform plugin for a single stream.
pub(crate) struct TransformRunResult {
    pub duration_secs: f64,
    pub summary: TransformSummary,
}

fn handle_close_result<E, F>(
    result: std::result::Result<
        std::result::Result<(), E>,
        rapidbyte_runtime::wasmtime_reexport::Error,
    >,
    role: &str,
    stream_name: &str,
    convert: F,
) where
    F: Fn(E) -> String,
{
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!(
                stream = stream_name,
                "{} close failed: {}",
                role,
                convert(err)
            );
        }
        Err(err) => {
            tracing::warn!(stream = stream_name, "{} close trap: {}", role, err);
        }
    }
}

/// Run a source plugin for a single stream.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::needless_pass_by_value
)]
pub(crate) fn run_source_stream(
    module: &LoadedComponent,
    sender: mpsc::Sender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    plugin_id: &str,
    plugin_version: &str,
    source_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
    on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
) -> Result<SourceRunResult, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let source_timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .plugin_id(plugin_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .sender(sender.clone())
        .source_checkpoints(source_checkpoints.clone())
        .timings(source_timings.clone())
        .config(source_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    if let Some(cb) = on_emit {
        builder = builder.on_emit(cb);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte source host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_plugin_source();

    let source_config_json = serde_json::to_string(source_config)
        .context("Failed to serialize source config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening source plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?
        .map_err(|err| PipelineError::Plugin(source_error_to_sdk(err)))?;

    let ctx_json = serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
    let run_request = source_bindings::rapidbyte::plugin::types::RunRequest {
        phase: source_bindings::rapidbyte::plugin::types::RunPhase::Read,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.read else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "source run summary missing read section"
                )));
            };
            ReadSummary {
                records_read: summary.records_read,
                bytes_read: summary.bytes_read,
                batches_emitted: summary.batches_emitted,
                checkpoint_count: summary.checkpoint_count,
                records_skipped: summary.records_skipped,
                perf: None,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(source_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_read,
        bytes = summary.bytes_read,
        "Source read complete for stream"
    );

    {
        let mut s = stats.lock().map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned"))
        })?;
        s.records_read += summary.records_read;
        s.bytes_read += summary.bytes_read;
    }

    let _ = sender.blocking_send(Frame::EndStream);

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing source plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Source",
        &stream_ctx.stream_name,
        |err| source_error_to_sdk(err).to_string(),
    );

    let checkpoints = source_checkpoints
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("source checkpoint mutex poisoned"))
        })?
        .drain(..)
        .collect::<Vec<_>>();
    let source_host_timings = source_timings
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("source timing mutex poisoned"))
        })?
        .clone();

    Ok(SourceRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        checkpoints,
        host_timings: source_host_timings,
    })
}

/// Run a destination plugin for a single stream.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::needless_pass_by_value
)]
pub(crate) fn run_destination_stream(
    module: &LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    plugin_id: &str,
    plugin_version: &str,
    dest_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
) -> Result<DestRunResult, PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .plugin_id(plugin_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .receiver(receiver)
        .dest_checkpoints(dest_checkpoints.clone())
        .dlq_records(dlq_records.clone())
        .timings(dest_timings.clone())
        .config(dest_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "destination", |linker| {
        dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte destination host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        dest_bindings::RapidbyteDestination::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_plugin_destination();
    let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

    let dest_config_json = serde_json::to_string(dest_config)
        .context("Failed to serialize destination config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening destination plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &dest_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?
        .map_err(|err| PipelineError::Plugin(dest_error_to_sdk(err)))?;

    let recv_start = Instant::now();
    let ctx_json = serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        stream = stream_ctx.stream_name,
        "Starting destination write"
    );
    let run_request = dest_bindings::rapidbyte::plugin::types::RunRequest {
        phase: dest_bindings::rapidbyte::plugin::types::RunPhase::Write,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.write else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "destination run summary missing write section"
                )));
            };
            WriteSummary {
                records_written: summary.records_written,
                bytes_written: summary.bytes_written,
                batches_written: summary.batches_written,
                checkpoint_count: summary.checkpoint_count,
                records_failed: summary.records_failed,
                perf: None,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(dest_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_written,
        bytes = summary.bytes_written,
        "Destination write complete for stream"
    );

    {
        let mut s = stats.lock().map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned"))
        })?;
        s.records_written += summary.records_written;
    }

    let recv_secs = recv_start.elapsed().as_secs_f64();

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing destination plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Destination",
        &stream_ctx.stream_name,
        |err| dest_error_to_sdk(err).to_string(),
    );

    let checkpoints = dest_checkpoints
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("destination checkpoint mutex poisoned"))
        })?
        .drain(..)
        .collect::<Vec<_>>();
    let dest_host_timings = dest_timings
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("destination timing mutex poisoned"))
        })?
        .clone();

    Ok(DestRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        vm_setup_secs,
        recv_secs,
        checkpoints,
        host_timings: dest_host_timings,
    })
}

/// Run a transform plugin for a single stream.
#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]
pub(crate) fn run_transform_stream(
    module: &LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    sender: mpsc::Sender<Frame>,
    dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    plugin_id: &str,
    plugin_version: &str,
    transform_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
) -> Result<TransformRunResult, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .plugin_id(plugin_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .sender(sender.clone())
        .receiver(receiver)
        .dlq_records(dlq_records)
        .source_checkpoints(source_checkpoints)
        .dest_checkpoints(dest_checkpoints)
        .timings(timings)
        .config(transform_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "transform", |linker| {
        transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(linker, |state| {
            state
        })
        .context("Failed to add rapidbyte transform host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        transform_bindings::RapidbyteTransform::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_plugin_transform();

    let transform_config_json = serde_json::to_string(transform_config)
        .context("Failed to serialize transform config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening transform plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &transform_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?
        .map_err(|err| PipelineError::Plugin(transform_error_to_sdk(err)))?;

    let ctx_json = serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting transform");
    let run_request = transform_bindings::rapidbyte::plugin::types::RunRequest {
        phase: transform_bindings::rapidbyte::plugin::types::RunPhase::Transform,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.transform else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "transform run summary missing transform section"
                )));
            };
            TransformSummary {
                records_in: summary.records_in,
                records_out: summary.records_out,
                bytes_in: summary.bytes_in,
                bytes_out: summary.bytes_out,
                batches_processed: summary.batches_processed,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(transform_error_to_sdk(err)));
        }
    };

    let _ = sender.blocking_send(Frame::EndStream);

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing transform plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Transform",
        &stream_ctx.stream_name,
        |err| transform_error_to_sdk(err).to_string(),
    );

    Ok(TransformRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
    })
}

pub(crate) fn validate_plugin(
    wasm_path: &std::path::Path,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<ValidationResult> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, "Validating plugin");

    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;
    let state = Arc::new(SqliteStateBackend::in_memory()?);

    let mut builder = ComponentHostState::builder()
        .pipeline("check")
        .plugin_id(plugin_id)
        .stream("check")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(source_validation_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(dest_validation_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            let linker = create_component_linker(&module.engine, "transform", |linker| {
                transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = transform_bindings::RapidbyteTransform::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_transform();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Transform open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(transform_validation_to_sdk)
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
    }
}

/// Discover available streams from a source plugin.
pub(crate) fn run_discover(
    wasm_path: &std::path::Path,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<Catalog> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let mut builder = ComponentHostState::builder()
        .pipeline("discover")
        .plugin_id(plugin_id)
        .stream("discover")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)?;
        Ok(())
    })?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)?;
    let iface = bindings.rapidbyte_plugin_source();
    let config_json = serde_json::to_string(config)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Opening source plugin for discover"
    );
    let session = iface
        .call_open(&mut store, &config_json)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!("Source open failed for discover: {e}"))?;

    let discover_json = iface
        .call_discover(&mut store, session)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let catalog = serde_json::from_str::<Catalog>(&discover_json)
        .context("Failed to parse discover catalog JSON")?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Closing source plugin after discover"
    );
    if let Err(err) = iface.call_close(&mut store, session)? {
        tracing::warn!(
            "Source close failed after discover: {}",
            source_error_to_sdk(err)
        );
    }

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::handle_close_result;

    #[test]
    fn test_handle_close_result_ok() {
        handle_close_result::<String, _>(Ok(Ok(())), "Source", "users", |e| e);
    }
}
