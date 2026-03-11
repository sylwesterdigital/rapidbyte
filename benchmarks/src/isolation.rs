use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use rapidbyte_engine::arrow::record_batch_to_ipc;
use rapidbyte_engine::runner::{run_destination_stream, run_source_stream, run_transform_stream};
use rapidbyte_runtime::{
    load_plugin_manifest, parse_plugin_ref, resolve_plugin_path, Frame, WasmRuntime,
};
use rapidbyte_state::SqliteStateBackend;
use rapidbyte_types::catalog::SchemaHint;
use rapidbyte_types::state::RunStats;
use rapidbyte_types::stream::{DataErrorPolicy, StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{PluginKind, SyncMode, WriteMode};
use serde_yaml::{Mapping, Value as YamlValue};

use crate::adapters::resolve_scenario_adapters;
use crate::artifact::ArtifactCorrectness;
use crate::runner::{ArtifactContext, RunResult};
use crate::scenario::{
    BenchmarkBuildMode, BenchmarkKind, PostgresBenchmarkEnvironment, ScenarioManifest,
};
use crate::workload::ResolvedWorkloadPlan;

pub fn execute_source_benchmark(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
    artifact_context: &ArtifactContext,
) -> Result<RunResult> {
    ensure_build_mode_matches_current_binary(scenario)?;
    let env = environment.with_context(|| {
        format!(
            "scenario {} requires a resolved benchmark environment for source execution",
            scenario.id
        )
    })?;
    let resolved = resolve_scenario_adapters(scenario)?;
    let source = resolved.source.context("missing source adapter")?;
    source.prepare_fixtures(scenario, workload, env)?;
    let mapping = source.prepare_pipeline_mapping(scenario, env)?;
    let config = mapping_config_json(&mapping)?;
    let plugin_ref = scenario
        .connectors
        .iter()
        .find(|connector| connector.kind == "source")
        .map(|connector| connector.plugin.clone())
        .context("missing source connector")?;
    let stream_ctx = source_stream_context(env, scenario);
    let _aot = AotEnvGuard::new(scenario.benchmark.aot);
    let runtime = WasmRuntime::new()?;
    let wasm_path = resolve_plugin_path(&plugin_ref, PluginKind::Source)?;
    let module = runtime.load_module(&wasm_path)?;
    let manifest = load_plugin_manifest(&wasm_path)?;
    let permissions = manifest
        .as_ref()
        .map(|manifest| manifest.permissions.clone());
    let (plugin_id, plugin_version) = parse_plugin_ref(&plugin_ref);
    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let (sender, receiver) = mpsc::sync_channel(32);
    let drain = std::thread::spawn(move || {
        while let Ok(frame) = receiver.recv() {
            if matches!(frame, Frame::EndStream) {
                break;
            }
        }
    });
    let result = run_source_stream(
        &module,
        sender,
        state,
        "benchmark-source",
        &plugin_id,
        &plugin_version,
        &config,
        &stream_ctx,
        stats,
        permissions.as_ref(),
        None,
        None,
        None,
    )?;
    let _ = drain.join();

    Ok(RunResult {
        suite_id: scenario.suite.clone(),
        scenario_id: scenario.id.clone(),
        benchmark_kind: BenchmarkKind::Source,
        git_sha: artifact_context.git_sha.clone(),
        hardware_class: artifact_context.hardware_class.clone(),
        scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
        build_mode: benchmark_build_mode_name(scenario.benchmark.build_mode).to_string(),
        connector_metrics: serde_json::json!({
            "source": {
                "duration_secs": result.duration_secs,
                "connect_secs": result.host_timings.source_connect_secs,
                "query_secs": result.host_timings.source_query_secs,
                "fetch_secs": result.host_timings.source_fetch_secs,
                "arrow_encode_secs": result.host_timings.source_arrow_encode_secs,
                "emit_count": result.host_timings.emit_batch_count,
            },
            "checkpoint_count": result.checkpoints.len(),
        }),
        canonical_metrics: serde_json::json!({
            "duration_secs": result.duration_secs,
            "records_per_sec": rate(result.summary.records_read, result.duration_secs),
            "mb_per_sec": rate(result.summary.bytes_read, result.duration_secs) / 1024.0 / 1024.0,
            "batch_count": result.summary.batches_emitted,
        }),
        execution_flags: serde_json::json!({
            "synthetic": false,
            "aot": scenario.benchmark.aot,
        }),
        correctness: row_count_correctness(
            "row_count",
            workload.expected_records_read,
            result.summary.records_read,
        ),
    })
}

pub fn execute_destination_benchmark(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
    artifact_context: &ArtifactContext,
) -> Result<RunResult> {
    ensure_build_mode_matches_current_binary(scenario)?;
    let env = environment.with_context(|| {
        format!(
            "scenario {} requires a resolved benchmark environment for destination execution",
            scenario.id
        )
    })?;
    let resolved = resolve_scenario_adapters(scenario)?;
    let destination = resolved
        .destination
        .context("missing destination adapter")?;
    destination.prepare_fixtures(scenario, workload, env)?;
    let mapping = destination.prepare_pipeline_mapping(scenario, env)?;
    let config = mapping_config_json(&mapping)?;
    let plugin_ref = scenario
        .connectors
        .iter()
        .find(|connector| connector.kind == "destination")
        .map(|connector| connector.plugin.clone())
        .context("missing destination connector")?;
    let batches = generate_input_batches(workload.rows)?;
    let schema_hint = first_batch_schema_hint(&batches)?;
    let stream_ctx = destination_stream_context(env, scenario, schema_hint);
    let _aot = AotEnvGuard::new(scenario.benchmark.aot);
    let runtime = WasmRuntime::new()?;
    let wasm_path = resolve_plugin_path(&plugin_ref, PluginKind::Destination)?;
    let module = runtime.load_module(&wasm_path)?;
    let manifest = load_plugin_manifest(&wasm_path)?;
    let permissions = manifest
        .as_ref()
        .map(|manifest| manifest.permissions.clone());
    let (plugin_id, plugin_version) = parse_plugin_ref(&plugin_ref);
    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let dlq_records = Arc::new(Mutex::new(Vec::new()));
    let (sender, receiver) = mpsc::sync_channel(64);
    send_input_batches(sender, &batches)?;
    let result = run_destination_stream(
        &module,
        receiver,
        dlq_records,
        state,
        "benchmark-destination",
        &plugin_id,
        &plugin_version,
        &config,
        &stream_ctx,
        stats,
        permissions.as_ref(),
        None,
        None,
    )?;

    Ok(RunResult {
        suite_id: scenario.suite.clone(),
        scenario_id: scenario.id.clone(),
        benchmark_kind: BenchmarkKind::Destination,
        git_sha: artifact_context.git_sha.clone(),
        hardware_class: artifact_context.hardware_class.clone(),
        scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
        build_mode: benchmark_build_mode_name(scenario.benchmark.build_mode).to_string(),
        connector_metrics: serde_json::json!({
            "destination": {
                "duration_secs": result.duration_secs,
                "vm_setup_secs": result.vm_setup_secs,
                "recv_secs": result.recv_secs,
                "connect_secs": result.host_timings.dest_connect_secs,
                "flush_secs": result.host_timings.dest_flush_secs,
                "commit_secs": result.host_timings.dest_commit_secs,
                "arrow_decode_secs": result.host_timings.dest_arrow_decode_secs,
            },
            "checkpoint_count": result.checkpoints.len(),
        }),
        canonical_metrics: serde_json::json!({
            "duration_secs": result.duration_secs,
            "records_per_sec": rate(result.summary.records_written, result.duration_secs),
            "mb_per_sec": rate(result.summary.bytes_written, result.duration_secs) / 1024.0 / 1024.0,
            "batch_count": result.summary.batches_written,
        }),
        execution_flags: serde_json::json!({
            "synthetic": false,
            "aot": scenario.benchmark.aot,
        }),
        correctness: row_count_correctness(
            "row_count",
            workload.expected_records_written,
            result.summary.records_written,
        ),
    })
}

pub fn execute_transform_benchmark(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    artifact_context: &ArtifactContext,
) -> Result<RunResult> {
    ensure_build_mode_matches_current_binary(scenario)?;
    let resolved = resolve_scenario_adapters(scenario)?;
    if resolved.transforms.is_empty() {
        bail!(
            "scenario {} requires at least one transform connector",
            scenario.id
        );
    }

    let batches = generate_input_batches(workload.rows)?;
    let stream_ctx = transform_stream_context(first_batch_schema_hint(&batches)?);
    let _aot = AotEnvGuard::new(scenario.benchmark.aot);
    let runtime = WasmRuntime::new()?;
    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let dlq_records = Arc::new(Mutex::new(Vec::new()));
    let channel_capacity = batches.len().saturating_mul(4).max(8);
    let (first_sender, first_receiver) = mpsc::sync_channel(channel_capacity);
    send_input_batches(first_sender, &batches)?;

    let mut incoming = first_receiver;
    let mut transform_duration_secs = 0.0;
    let mut transform_module_load_ms = Vec::new();
    let mut last_summary = None;

    for (index, connector) in scenario
        .connectors
        .iter()
        .filter(|connector| connector.kind == "transform")
        .enumerate()
    {
        let (sender, receiver) = mpsc::sync_channel(channel_capacity);
        let config = scenario
            .connector_options
            .transforms
            .get(index)
            .cloned()
            .unwrap_or_default();
        let wasm_path = resolve_plugin_path(&connector.plugin, PluginKind::Transform)?;
        let manifest = load_plugin_manifest(&wasm_path)?;
        let permissions = manifest
            .as_ref()
            .map(|manifest| manifest.permissions.clone());
        let load_start = Instant::now();
        let module = runtime.load_module(&wasm_path)?;
        transform_module_load_ms.push(load_start.elapsed().as_millis() as u64);
        let (plugin_id, plugin_version) = parse_plugin_ref(&connector.plugin);
        let summary = run_transform_stream(
            &module,
            incoming,
            sender,
            dlq_records.clone(),
            state.clone(),
            "benchmark-transform",
            &plugin_id,
            &plugin_version,
            index,
            &serde_json::to_value(config.config)?,
            &stream_ctx,
            permissions.as_ref(),
            None,
            None,
        )?;
        transform_duration_secs += summary.duration_secs;
        last_summary = Some(summary.summary);
        incoming = receiver;
    }

    let drain = std::thread::spawn(move || {
        while let Ok(frame) = incoming.recv() {
            if matches!(frame, Frame::EndStream) {
                break;
            }
        }
    });
    let _ = drain.join();
    let summary = last_summary.context("transform benchmark produced no summary")?;

    Ok(RunResult {
        suite_id: scenario.suite.clone(),
        scenario_id: scenario.id.clone(),
        benchmark_kind: BenchmarkKind::Transform,
        git_sha: artifact_context.git_sha.clone(),
        hardware_class: artifact_context.hardware_class.clone(),
        scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
        build_mode: benchmark_build_mode_name(scenario.benchmark.build_mode).to_string(),
        connector_metrics: serde_json::json!({
            "transform": {
                "count": resolved.transforms.len(),
                "duration_secs": transform_duration_secs,
                "module_load_ms": transform_module_load_ms,
            }
        }),
        canonical_metrics: serde_json::json!({
            "duration_secs": transform_duration_secs,
            "records_per_sec": rate(summary.records_out, transform_duration_secs),
            "mb_per_sec": rate(summary.bytes_out, transform_duration_secs) / 1024.0 / 1024.0,
            "batch_count": summary.batches_processed,
        }),
        execution_flags: serde_json::json!({
            "synthetic": false,
            "aot": scenario.benchmark.aot,
        }),
        correctness: row_count_correctness(
            "row_count",
            workload.expected_records_written,
            summary.records_out,
        ),
    })
}

fn ensure_build_mode_matches_current_binary(scenario: &ScenarioManifest) -> Result<()> {
    let current = if cfg!(debug_assertions) {
        BenchmarkBuildMode::Debug
    } else {
        BenchmarkBuildMode::Release
    };
    if scenario.benchmark.build_mode == current {
        return Ok(());
    }
    bail!(
        "scenario {} requires {:?} benchmark binary but rapidbyte-benchmarks is running as {:?}",
        scenario.id,
        scenario.benchmark.build_mode,
        current
    );
}

fn benchmark_build_mode_name(build_mode: BenchmarkBuildMode) -> &'static str {
    match build_mode {
        BenchmarkBuildMode::Debug => "debug",
        BenchmarkBuildMode::Release => "release",
    }
}

fn mapping_config_json(mapping: &Mapping) -> Result<serde_json::Value> {
    let config = mapping
        .get(YamlValue::String("config".to_string()))
        .cloned()
        .context("benchmark adapter mapping is missing config")?;
    serde_json::to_value(config).context("failed to serialize benchmark config as json")
}

fn source_stream_context(
    env: &PostgresBenchmarkEnvironment,
    scenario: &ScenarioManifest,
) -> StreamContext {
    StreamContext {
        stream_name: env.stream_name.clone(),
        source_stream_name: Some(env.stream_name.clone()),
        schema: SchemaHint::Columns(Vec::new()),
        sync_mode: scenario
            .connector_options
            .source
            .sync_mode
            .unwrap_or(SyncMode::FullRefresh),
        cursor_info: None,
        limits: StreamLimits::default(),
        policies: StreamPolicies::default(),
        write_mode: None,
        selected_columns: None,
        partition_key: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    }
}

fn destination_stream_context(
    env: &PostgresBenchmarkEnvironment,
    scenario: &ScenarioManifest,
    schema: SchemaHint,
) -> StreamContext {
    StreamContext {
        stream_name: env.stream_name.clone(),
        source_stream_name: Some(env.stream_name.clone()),
        schema,
        sync_mode: SyncMode::FullRefresh,
        cursor_info: None,
        limits: StreamLimits::default(),
        policies: StreamPolicies {
            on_data_error: DataErrorPolicy::Fail,
            ..StreamPolicies::default()
        },
        write_mode: Some(parse_write_mode(
            scenario
                .connector_options
                .destination
                .write_mode
                .as_deref()
                .unwrap_or("append"),
        )),
        selected_columns: None,
        partition_key: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    }
}

fn transform_stream_context(schema: SchemaHint) -> StreamContext {
    StreamContext {
        stream_name: "bench_transform".to_string(),
        source_stream_name: None,
        schema,
        sync_mode: SyncMode::FullRefresh,
        cursor_info: None,
        limits: StreamLimits::default(),
        policies: StreamPolicies::default(),
        write_mode: None,
        selected_columns: None,
        partition_key: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    }
}

fn parse_write_mode(value: &str) -> WriteMode {
    match value {
        "replace" => WriteMode::Replace,
        _ => WriteMode::Append,
    }
}

fn generate_input_batches(rows: u64) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant_id", DataType::Int32, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let chunk_size = 100_000_u64;
    let mut start = 0_u64;
    while start < rows {
        let end = (start + chunk_size).min(rows);
        let ids = (start + 1..=end).map(|row| row as i64).collect::<Vec<_>>();
        let tenant_ids = (start + 1..=end)
            .map(|row| (row % 128) as i32)
            .collect::<Vec<_>>();
        let event_names = (start + 1..=end)
            .map(|row| {
                if row % 2 == 0 {
                    "signup".to_string()
                } else {
                    "purchase".to_string()
                }
            })
            .collect::<Vec<_>>();
        let payloads = (start + 1..=end)
            .map(|row| format!("payload-{row:010}"))
            .collect::<Vec<_>>();

        batches.push(RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int32Array::from(tenant_ids)),
                Arc::new(StringArray::from(event_names)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?);
        start = end;
    }
    Ok(batches)
}

fn send_input_batches(sender: mpsc::SyncSender<Frame>, batches: &[RecordBatch]) -> Result<()> {
    for batch in batches {
        sender
            .send(Frame::Data {
                payload: Bytes::from(record_batch_to_ipc(batch)?),
                checkpoint_id: 0,
            })
            .context("failed to send benchmark input batch")?;
    }
    sender
        .send(Frame::EndStream)
        .context("failed to send benchmark end-of-stream")?;
    Ok(())
}

fn first_batch_schema_hint(batches: &[RecordBatch]) -> Result<SchemaHint> {
    let first = batches
        .first()
        .context("benchmark input batches are empty")?;
    Ok(SchemaHint::ArrowIpc(record_batch_to_ipc(first)?))
}

fn row_count_correctness(validator: &str, expected: u64, actual: u64) -> ArtifactCorrectness {
    ArtifactCorrectness {
        passed: expected == actual,
        validator: validator.to_string(),
        details: Some(serde_json::json!({
            "expected": expected,
            "actual": actual,
        })),
    }
}

fn rate(value: u64, duration_secs: f64) -> f64 {
    if duration_secs <= 0.0 {
        0.0
    } else {
        value as f64 / duration_secs
    }
}

struct AotEnvGuard {
    previous: Option<String>,
}

impl AotEnvGuard {
    fn new(enabled: bool) -> Self {
        let previous = std::env::var("RAPIDBYTE_WASMTIME_AOT").ok();
        std::env::set_var("RAPIDBYTE_WASMTIME_AOT", if enabled { "1" } else { "0" });
        Self { previous }
    }
}

impl Drop for AotEnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            std::env::set_var("RAPIDBYTE_WASMTIME_AOT", value);
        } else {
            std::env::remove_var("RAPIDBYTE_WASMTIME_AOT");
        }
    }
}
