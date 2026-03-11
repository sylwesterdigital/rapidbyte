use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::adapters::prepare_pipeline_fixtures;
use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};
use crate::environment::{
    resolve_environment_profile, resolve_postgres_environment_from_profile, EnvironmentSession,
    SystemEnvironmentCommandExecutor,
};
use crate::isolation::{
    execute_destination_benchmark, execute_source_benchmark, execute_transform_benchmark,
};
use crate::output::write_artifact_json;
use crate::pipeline::write_rendered_pipeline;
use crate::scenario::{
    filter_scenarios, BenchmarkBuildMode, BenchmarkKind, PostgresBenchmarkEnvironment,
    ScenarioManifest,
};
use crate::workload::{resolve_workload_plan_with_environment, ResolvedWorkloadPlan};

const BENCH_JSON_MARKER: &str = "@@BENCH_JSON@@";

#[derive(Debug, Clone)]
pub struct ArtifactContext {
    pub git_sha: String,
    pub hardware_class: String,
    pub scenario_fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub(crate) suite_id: String,
    pub(crate) scenario_id: String,
    pub(crate) benchmark_kind: BenchmarkKind,
    pub(crate) git_sha: String,
    pub(crate) hardware_class: String,
    pub(crate) scenario_fingerprint: String,
    pub(crate) build_mode: String,
    pub(crate) connector_metrics: JsonValue,
    pub(crate) canonical_metrics: JsonValue,
    pub(crate) execution_flags: JsonValue,
    pub(crate) correctness: ArtifactCorrectness,
}

#[derive(Debug, Clone)]
pub struct SyntheticRunSpec {
    pub benchmark_kind: BenchmarkKind,
    pub build_mode: String,
    pub connector_metrics: JsonValue,
    pub records_written: u64,
    pub aot: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct EmitArtifactsOptions<'a> {
    pub suite: Option<&'a str>,
    pub scenario_ids: &'a [String],
    pub env_profile: Option<&'a str>,
    pub hardware_class: Option<&'a str>,
    pub rapidbyte_bin: Option<&'a Path>,
    pub output_path: &'a Path,
}

impl RunResult {
    pub fn success(
        suite_id: impl Into<String>,
        scenario_id: impl Into<String>,
        artifact_context: &ArtifactContext,
        spec: SyntheticRunSpec,
    ) -> Self {
        let duration_secs = 1.0;
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            benchmark_kind: spec.benchmark_kind,
            git_sha: artifact_context.git_sha.clone(),
            hardware_class: artifact_context.hardware_class.clone(),
            scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
            build_mode: spec.build_mode,
            connector_metrics: spec.connector_metrics,
            canonical_metrics: json!({
                "duration_secs": duration_secs,
                "records_per_sec": spec.records_written as f64 / duration_secs,
                "mb_per_sec": (spec.records_written as f64 * 128.0) / 1024.0 / 1024.0 / duration_secs,
                "cpu_secs": 0.5,
                "peak_rss_mb": 64.0,
                "batch_count": 1,
            }),
            execution_flags: json!({
                "synthetic": true,
                "aot": spec.aot,
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "synthetic_row_count".to_string(),
                details: Some(json!({
                    "records_written": spec.records_written,
                })),
            },
        }
    }

    #[cfg(test)]
    pub fn without_assertions(
        suite_id: impl Into<String>,
        scenario_id: impl Into<String>,
        artifact_context: &ArtifactContext,
    ) -> Self {
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: artifact_context.git_sha.clone(),
            hardware_class: artifact_context.hardware_class.clone(),
            scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
            build_mode: build_mode_name(BenchmarkBuildMode::Debug).to_string(),
            connector_metrics: JsonValue::Object(serde_json::Map::new()),
            canonical_metrics: JsonValue::Object(serde_json::Map::new()),
            execution_flags: json!({
                "synthetic": true,
                "aot": false,
            }),
            correctness: ArtifactCorrectness {
                passed: false,
                validator: "missing_assertions".to_string(),
                details: None,
            },
        }
    }
}

pub fn materialize_artifact(result: RunResult) -> Result<BenchmarkArtifact> {
    if result.correctness.validator == "missing_assertions" {
        bail!(
            "benchmark run for scenario {} is missing correctness assertions",
            result.scenario_id
        );
    }

    Ok(BenchmarkArtifact {
        schema_version: 1,
        suite_id: result.suite_id,
        scenario_id: result.scenario_id,
        benchmark_kind: result.benchmark_kind,
        git_sha: result.git_sha,
        hardware_class: result.hardware_class,
        scenario_fingerprint: result.scenario_fingerprint,
        build_mode: result.build_mode,
        execution_flags: result.execution_flags,
        canonical_metrics: result.canonical_metrics,
        connector_metrics: result.connector_metrics,
        correctness: result.correctness,
    })
}

pub fn build_artifact_context(
    root: &Path,
    scenario: &ScenarioManifest,
    hardware_class: Option<&str>,
) -> ArtifactContext {
    ArtifactContext {
        git_sha: resolve_git_sha(root).unwrap_or_else(|_| "unknown".to_string()),
        hardware_class: hardware_class
            .map(ToOwned::to_owned)
            .or_else(|| std::env::var("RAPIDBYTE_BENCH_HARDWARE_CLASS").ok())
            .unwrap_or_else(|| "unknown".to_string()),
        scenario_fingerprint: scenario_fingerprint(scenario)
            .unwrap_or_else(|_| "unknown".to_string()),
    }
}

pub fn emit_scenario_artifacts(
    root: &Path,
    scenarios: &[ScenarioManifest],
    options: EmitArtifactsOptions<'_>,
) -> Result<usize> {
    let filtered = select_scenarios(scenarios, options.suite, options.scenario_ids);
    if filtered.is_empty() {
        return emit_scenario_artifacts_with_dependencies(
            root,
            &filtered,
            options,
            Box::new(SessionBackedEnvironmentHandle { session: None }),
            execute_scenario_once,
        );
    }
    let environment = acquire_benchmark_environment(root, options.env_profile)?;
    emit_scenario_artifacts_with_dependencies(
        root,
        &filtered,
        options,
        environment,
        execute_scenario_once,
    )
}

fn emit_scenario_artifacts_with_dependencies<F>(
    root: &Path,
    filtered: &[&ScenarioManifest],
    options: EmitArtifactsOptions<'_>,
    environment: Box<dyn BenchmarkEnvironmentHandle>,
    execute_once: F,
) -> Result<usize>
where
    F: Fn(
        &Path,
        &ScenarioManifest,
        &ResolvedWorkloadPlan,
        Option<&PostgresBenchmarkEnvironment>,
        &ArtifactContext,
        Option<&Path>,
    ) -> Result<RunResult>,
{
    let run_result = (|| {
        if let Some(parent) = options.output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(options.output_path)?;
        let mut artifact_count = 0;
        for scenario in filtered {
            let artifact_context = build_artifact_context(root, scenario, options.hardware_class);
            let resolved_env = environment.resolve_for_scenario(scenario)?;
            let workload = resolve_workload_plan_with_environment(scenario, resolved_env.as_ref())?;

            for _ in 0..scenario.execution.warmups {
                let _ = execute_once(
                    root,
                    scenario,
                    &workload,
                    resolved_env.as_ref(),
                    &artifact_context,
                    options.rapidbyte_bin,
                )?;
            }

            let measured_iterations = scenario.execution.iterations.max(1);
            for _ in 0..measured_iterations {
                let artifact = materialize_artifact(execute_once(
                    root,
                    scenario,
                    &workload,
                    resolved_env.as_ref(),
                    &artifact_context,
                    options.rapidbyte_bin,
                )?)?;
                write_artifact_json(&mut file, &artifact)?;
                artifact_count += 1;
            }
        }

        Ok(artifact_count)
    })();

    let teardown_result = environment.finish();
    match (run_result, teardown_result) {
        (Ok(written), Ok(())) => Ok(written),
        (Ok(_), Err(teardown_err)) => Err(teardown_err),
        (Err(run_err), Ok(())) => Err(run_err),
        (Err(run_err), Err(teardown_err)) => Err(anyhow::anyhow!(
            "benchmark execution failed: {run_err}; teardown also failed: {teardown_err}"
        )),
    }
}

fn select_scenarios<'a>(
    scenarios: &'a [ScenarioManifest],
    suite: Option<&str>,
    scenario_ids: &[String],
) -> Vec<&'a ScenarioManifest> {
    filter_scenarios(scenarios, suite, &[])
        .into_iter()
        .filter(|scenario| {
            scenario_ids.is_empty() || scenario_ids.iter().any(|selected| selected == &scenario.id)
        })
        .collect()
}

trait BenchmarkEnvironmentHandle {
    fn resolve_for_scenario(
        &self,
        scenario: &ScenarioManifest,
    ) -> Result<Option<PostgresBenchmarkEnvironment>>;
    fn finish(self: Box<Self>) -> Result<()>;
}

struct SessionBackedEnvironmentHandle {
    session: Option<EnvironmentSession>,
}

impl BenchmarkEnvironmentHandle for SessionBackedEnvironmentHandle {
    fn resolve_for_scenario(
        &self,
        scenario: &ScenarioManifest,
    ) -> Result<Option<PostgresBenchmarkEnvironment>> {
        resolve_postgres_environment_from_profile(
            scenario,
            self.session.as_ref().map(EnvironmentSession::profile),
        )
    }

    fn finish(self: Box<Self>) -> Result<()> {
        match self.session {
            Some(session) => session.finish(&SystemEnvironmentCommandExecutor),
            None => Ok(()),
        }
    }
}

fn acquire_benchmark_environment(
    root: &Path,
    env_profile: Option<&str>,
) -> Result<Box<dyn BenchmarkEnvironmentHandle>> {
    let session = match env_profile {
        Some(profile_id) => {
            let mut profile = resolve_environment_profile(root, profile_id)?;
            crate::environment::apply_environment_overrides(&mut profile)?;
            Some(EnvironmentSession::provision(
                root,
                profile,
                &SystemEnvironmentCommandExecutor,
            )?)
        }
        None => None,
    };
    Ok(Box::new(SessionBackedEnvironmentHandle { session }))
}

fn execute_scenario_once(
    root: &Path,
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
    artifact_context: &ArtifactContext,
    rapidbyte_bin: Option<&Path>,
) -> Result<RunResult> {
    match scenario.kind {
        crate::scenario::BenchmarkKind::Pipeline => {
            if workload.seed.is_some() && !workload.synthetic {
                return execute_real_postgres_pipeline(
                    root,
                    scenario,
                    workload,
                    environment,
                    artifact_context,
                    rapidbyte_bin,
                );
            }

            Ok(RunResult::success(
                scenario.suite.clone(),
                scenario.id.clone(),
                artifact_context,
                SyntheticRunSpec {
                    benchmark_kind: scenario.kind,
                    build_mode: build_mode_name(scenario.benchmark.build_mode).to_string(),
                    connector_metrics: json!({
                        "workload_family": format!("{:?}", scenario.workload.family),
                    }),
                    records_written: workload.rows,
                    aot: scenario.benchmark.aot,
                },
            ))
        }
        crate::scenario::BenchmarkKind::Source => {
            execute_source_benchmark(scenario, workload, environment, artifact_context)
        }
        crate::scenario::BenchmarkKind::Destination => {
            execute_destination_benchmark(scenario, workload, environment, artifact_context)
        }
        crate::scenario::BenchmarkKind::Transform => {
            execute_transform_benchmark(scenario, workload, artifact_context)
        }
    }
}

fn execute_real_postgres_pipeline(
    root: &Path,
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
    artifact_context: &ArtifactContext,
    rapidbyte_bin: Option<&Path>,
) -> Result<RunResult> {
    let temp_root = benchmark_temp_root(&scenario.id)?;
    let env = environment.with_context(|| {
        format!(
            "scenario {} requires a resolved postgres benchmark environment",
            scenario.id
        )
    })?;
    prepare_pipeline_fixtures(scenario, workload, env)?;
    let rendered = write_rendered_pipeline(scenario, env, &temp_root)?;
    let bench_json = invoke_rapidbyte_run(root, scenario, &rendered.path, rapidbyte_bin)?;
    let correctness = enforce_row_count_assertions(&scenario.id, workload, &bench_json)?;
    run_result_from_bench_json(scenario, artifact_context, bench_json, correctness)
}

fn invoke_rapidbyte_run(
    repo_root: &Path,
    scenario: &ScenarioManifest,
    pipeline_path: &Path,
    rapidbyte_bin: Option<&Path>,
) -> Result<JsonValue> {
    let binary_path = rapidbyte_bin
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_rapidbyte_binary(repo_root, scenario));
    if !binary_path.exists() {
        bail!(
            "rapidbyte benchmark binary not found at {}; build it first or pass --rapidbyte-bin",
            binary_path.display()
        );
    }
    let output = rapidbyte_run_command(repo_root, pipeline_path, scenario, Some(&binary_path))
        .output()
        .context("failed to invoke rapidbyte CLI")?;

    if !output.status.success() {
        bail!(
            "rapidbyte run failed for {}:\nstdout:\n{}\nstderr:\n{}",
            pipeline_path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    parse_bench_json_from_stdout(&String::from_utf8_lossy(&output.stdout))
}

fn rapidbyte_run_command(
    repo_root: &Path,
    pipeline_path: &Path,
    scenario: &ScenarioManifest,
    rapidbyte_bin: Option<&Path>,
) -> Command {
    let binary_path = rapidbyte_bin
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_rapidbyte_binary(repo_root, scenario));
    let mut command = Command::new(binary_path);
    command
        .current_dir(repo_root)
        .env("RAPIDBYTE_BENCH", "1")
        .env(
            "RAPIDBYTE_PLUGIN_DIR",
            repo_root.join("target").join("plugins"),
        );
    command.env(
        "RAPIDBYTE_WASMTIME_AOT",
        if scenario.benchmark.aot { "1" } else { "0" },
    );
    command.args(["--quiet", "run"]).arg(pipeline_path);
    command
}

fn parse_bench_json_from_stdout(stdout: &str) -> Result<JsonValue> {
    let marker_line = stdout
        .lines()
        .find_map(|line| line.strip_prefix(BENCH_JSON_MARKER))
        .context("benchmark run did not emit @@BENCH_JSON@@ output")?;
    serde_json::from_str(marker_line).context("failed to parse benchmark JSON payload")
}

fn run_result_from_bench_json(
    scenario: &ScenarioManifest,
    artifact_context: &ArtifactContext,
    bench_json: JsonValue,
    correctness: ArtifactCorrectness,
) -> Result<RunResult> {
    let duration_secs = required_f64(&bench_json, "duration_secs")?;
    let records_written = required_u64(&bench_json, "records_written")?;
    let bytes_written = required_u64(&bench_json, "bytes_written")?;
    let dest_recv_count = optional_u64(&bench_json, "dest_recv_count").unwrap_or(1);
    let connector_metrics = json!({
        "source": {
            "duration_secs": optional_f64(&bench_json, "source_duration_secs"),
            "connect_secs": optional_f64(&bench_json, "source_connect_secs"),
            "query_secs": optional_f64(&bench_json, "source_query_secs"),
            "fetch_secs": optional_f64(&bench_json, "source_fetch_secs"),
            "arrow_encode_secs": optional_f64(&bench_json, "source_arrow_encode_secs"),
            "module_load_ms": optional_f64(&bench_json, "source_module_load_ms"),
        },
        "destination": {
            "duration_secs": optional_f64(&bench_json, "dest_duration_secs"),
            "connect_secs": optional_f64(&bench_json, "dest_connect_secs"),
            "flush_secs": optional_f64(&bench_json, "dest_flush_secs"),
            "commit_secs": optional_f64(&bench_json, "dest_commit_secs"),
            "recv_secs": optional_f64(&bench_json, "dest_recv_secs"),
            "vm_setup_secs": optional_f64(&bench_json, "dest_vm_setup_secs"),
            "arrow_decode_secs": optional_f64(&bench_json, "dest_arrow_decode_secs"),
            "module_load_ms": optional_f64(&bench_json, "dest_module_load_ms"),
            "load_method": scenario.connector_options.destination.load_method.clone(),
            "write_mode": scenario.connector_options.destination.write_mode.clone(),
        },
        "transform": {
            "count": optional_u64(&bench_json, "transform_count"),
            "duration_secs": optional_f64(&bench_json, "transform_duration_secs"),
            "module_load_ms": bench_json.get("transform_module_load_ms").cloned(),
        },
        "process": {
            "cpu_secs": optional_f64(&bench_json, "process_cpu_secs"),
            "peak_rss_mb": optional_f64(&bench_json, "process_peak_rss_mb"),
            "available_cores": optional_u64(&bench_json, "available_cores"),
        },
        "parallelism": optional_u64(&bench_json, "parallelism"),
        "retry_count": optional_u64(&bench_json, "retry_count"),
        "stream_metrics": bench_json
            .get("stream_metrics")
            .cloned()
            .unwrap_or(JsonValue::Array(vec![])),
    });

    Ok(RunResult {
        suite_id: scenario.suite.clone(),
        scenario_id: scenario.id.clone(),
        benchmark_kind: scenario.kind,
        git_sha: artifact_context.git_sha.clone(),
        hardware_class: artifact_context.hardware_class.clone(),
        scenario_fingerprint: artifact_context.scenario_fingerprint.clone(),
        build_mode: build_mode_name(scenario.benchmark.build_mode).to_string(),
        connector_metrics,
        canonical_metrics: json!({
            "duration_secs": duration_secs,
            "records_per_sec": rate(records_written, duration_secs),
            "mb_per_sec": rate(bytes_written, duration_secs) / 1024.0 / 1024.0,
            "cpu_secs": optional_f64(&bench_json, "process_cpu_secs"),
            "peak_rss_mb": optional_f64(&bench_json, "process_peak_rss_mb"),
            "batch_count": dest_recv_count,
        }),
        execution_flags: json!({
            "synthetic": false,
            "aot": scenario.benchmark.aot,
        }),
        correctness,
    })
}

fn build_mode_name(build_mode: BenchmarkBuildMode) -> &'static str {
    match build_mode {
        BenchmarkBuildMode::Debug => "debug",
        BenchmarkBuildMode::Release => "release",
    }
}

fn enforce_row_count_assertions(
    scenario_id: &str,
    workload: &ResolvedWorkloadPlan,
    bench_json: &JsonValue,
) -> Result<ArtifactCorrectness> {
    let actual_read = required_u64(bench_json, "records_read")?;
    let actual_written = required_u64(bench_json, "records_written")?;

    if actual_read != workload.expected_records_read {
        bail!(
            "scenario {scenario_id} expected {} records read but benchmark reported {actual_read}",
            workload.expected_records_read
        );
    }
    if actual_written != workload.expected_records_written {
        bail!(
            "scenario {scenario_id} expected {} records written but benchmark reported {actual_written}",
            workload.expected_records_written
        );
    }

    Ok(ArtifactCorrectness {
        passed: true,
        validator: "row_count".to_string(),
        details: Some(json!({
            "expected_records_read": workload.expected_records_read,
            "actual_records_read": actual_read,
            "expected_records_written": workload.expected_records_written,
            "actual_records_written": actual_written,
        })),
    })
}

fn default_rapidbyte_binary(repo_root: &Path, scenario: &ScenarioManifest) -> PathBuf {
    repo_root
        .join("target")
        .join(build_mode_name(scenario.benchmark.build_mode))
        .join("rapidbyte")
}

fn resolve_git_sha(root: &Path) -> Result<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .context("failed to resolve git sha")?;
    if !output.status.success() {
        bail!("git rev-parse failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn scenario_fingerprint(scenario: &ScenarioManifest) -> Result<String> {
    let bytes = serde_json::to_vec(scenario).context("failed to serialize scenario fingerprint")?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn benchmark_temp_root(scenario_id: &str) -> Result<PathBuf> {
    let root = std::env::current_dir()
        .context("failed to determine benchmark cwd")?
        .join("target")
        .join("benchmarks")
        .join("tmp")
        .join(format!(
            "{scenario_id}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .context("system clock before unix epoch")?
                .as_nanos()
        ));
    fs::create_dir_all(&root)
        .with_context(|| format!("failed to create benchmark temp root {}", root.display()))?;
    Ok(root)
}

fn required_u64(json: &JsonValue, key: &str) -> Result<u64> {
    optional_u64(json, key)
        .with_context(|| format!("benchmark JSON is missing integer field {key}"))
}

fn optional_u64(json: &JsonValue, key: &str) -> Option<u64> {
    json.get(key).and_then(JsonValue::as_u64)
}

fn required_f64(json: &JsonValue, key: &str) -> Result<f64> {
    optional_f64(json, key)
        .with_context(|| format!("benchmark JSON is missing numeric field {key}"))
}

fn optional_f64(json: &JsonValue, key: &str) -> Option<f64> {
    json.get(key).and_then(JsonValue::as_f64)
}

fn rate(value: u64, duration_secs: f64) -> f64 {
    if duration_secs <= 0.0 {
        return 0.0;
    }
    value as f64 / duration_secs
}

#[cfg(test)]
mod tests {
    use rapidbyte_types::wire::SyncMode;
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    use super::*;
    use crate::environment::{
        EnvironmentBinding, EnvironmentBindings, EnvironmentCommandExecutor, EnvironmentProfile,
        EnvironmentProvider, EnvironmentService,
    };
    use crate::scenario::{
        BenchmarkBuildMode, BenchmarkKind, ConnectorOptions, DestinationConnectorOptions,
        EnvironmentConfig, ExecutionProfile, PostgresBenchmarkEnvironment,
        PostgresConnectionProfile, ScenarioAssertions, ScenarioConnectorRef, ScenarioManifest,
        SourceConnectorOptions, WorkloadProfile,
    };
    use crate::workload::{resolve_workload_plan, WorkloadFamily};

    fn artifact_context() -> ArtifactContext {
        ArtifactContext {
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
        }
    }

    #[test]
    fn bench_json_maps_into_benchmark_artifact() {
        let scenario = sample_postgres_scenario("insert");
        let bench_json = parse_bench_json_from_stdout(&format!(
            "noise\n{BENCH_JSON_MARKER}{}",
            sample_bench_json(1_000, 1_000)
        ))
        .expect("parse bench json");
        let workload = resolve_workload_plan(&scenario).expect("workload");
        let bench_json_value: JsonValue =
            serde_json::from_str(&sample_bench_json(1_000, 1_000)).expect("bench json");
        let correctness = enforce_row_count_assertions(&scenario.id, &workload, &bench_json_value)
            .expect("correctness");
        let run =
            run_result_from_bench_json(&scenario, &artifact_context(), bench_json, correctness)
                .expect("run result");
        let artifact = materialize_artifact(run).expect("artifact");

        assert_eq!(artifact.scenario_id, "pg_dest_insert");
        assert_eq!(artifact.benchmark_kind, BenchmarkKind::Pipeline);
        assert_eq!(artifact.git_sha, "abc1234");
        assert_eq!(artifact.canonical_metrics["duration_secs"], 2.0);
        assert_eq!(artifact.canonical_metrics["records_per_sec"], 500.0);
        assert_eq!(artifact.canonical_metrics["batch_count"], 4);
    }

    #[test]
    fn mismatched_row_count_assertions_fail_the_scenario() {
        let scenario = sample_postgres_scenario("insert");
        let workload = resolve_workload_plan(&scenario).expect("workload");
        let bench_json: JsonValue = serde_json::from_str(&sample_bench_json(1_000, 999)).unwrap();

        let err = enforce_row_count_assertions(&scenario.id, &workload, &bench_json)
            .expect_err("row-count mismatch should fail");

        assert!(err.to_string().contains("expected 1000 records written"));
    }

    #[test]
    fn real_execution_path_preserves_scenario_id_and_connector_metrics() {
        let scenario = sample_postgres_scenario("copy");
        let bench_json: JsonValue = serde_json::from_str(&sample_bench_json(1_000, 1_000)).unwrap();
        let correctness = enforce_row_count_assertions(
            &scenario.id,
            &resolve_workload_plan(&scenario).expect("workload"),
            &bench_json,
        )
        .expect("correctness");
        let artifact = materialize_artifact(
            run_result_from_bench_json(&scenario, &artifact_context(), bench_json, correctness)
                .expect("run result"),
        )
        .expect("artifact");

        assert_eq!(artifact.scenario_id, "pg_dest_copy");
        assert_eq!(
            artifact.connector_metrics["destination"]["load_method"],
            "copy"
        );
        assert_eq!(artifact.connector_metrics["destination"]["flush_secs"], 1.2);
        assert_eq!(artifact.connector_metrics["parallelism"], 4);
    }

    #[test]
    fn emit_scenario_artifacts_filters_to_selected_scenarios() {
        let temp_root = std::env::temp_dir().join(format!(
            "rapidbyte-benchmark-filter-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");
        let output_path = temp_root.join("candidate.jsonl");
        let insert = synthetic_scenario("synthetic_insert");
        let copy = synthetic_scenario("synthetic_copy");

        let written = emit_scenario_artifacts(
            std::path::Path::new("."),
            &[insert, copy],
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
        )
        .expect("emit artifacts");

        assert_eq!(written, 1);
        let rendered = fs::read_to_string(&output_path).expect("read output");
        assert!(rendered.contains("\"scenario_id\":\"synthetic_copy\""));
        assert!(!rendered.contains("\"scenario_id\":\"synthetic_insert\""));
    }

    #[test]
    fn logical_environment_reference_requires_env_profile() {
        let temp_root = std::env::temp_dir().join(format!(
            "rapidbyte-benchmark-env-required-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");
        let output_path = temp_root.join("candidate.jsonl");

        let err = emit_scenario_artifacts(
            std::path::Path::new("."),
            &[logical_environment_scenario("pg_dest_insert")],
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["pg_dest_insert".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
        )
        .expect_err("logical environment should require an env profile");

        assert!(err.to_string().contains("requires environment profile"));
    }

    #[test]
    fn emit_scenario_artifacts_tears_down_environment_after_success() {
        let temp_root = temp_dir("runner-teardown-success");
        let output_path = temp_root.join("candidate.jsonl");
        let events = Rc::new(RefCell::new(vec!["provision".to_string()]));
        let environment = Box::new(RecordingEnvironmentHandle::success(events.clone()));
        let scenario = synthetic_scenario("synthetic_copy");
        let filtered = vec![&scenario];

        let written = emit_scenario_artifacts_with_dependencies(
            std::path::Path::new("."),
            &filtered,
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
            environment,
            |_, scenario, _, _, artifact_context, _| {
                events.borrow_mut().push("execute".to_string());
                Ok(RunResult::success(
                    "lab",
                    scenario.id.clone(),
                    artifact_context,
                    SyntheticRunSpec {
                        benchmark_kind: BenchmarkKind::Pipeline,
                        build_mode: "debug".to_string(),
                        connector_metrics: json!({}),
                        records_written: 100,
                        aot: false,
                    },
                ))
            },
        )
        .expect("emit artifacts");

        assert_eq!(written, 1);
        assert_eq!(
            events.borrow().as_slice(),
            &["provision", "execute", "teardown"]
        );
    }

    #[test]
    fn emit_scenario_artifacts_returns_teardown_error_after_successful_run() {
        let temp_root = temp_dir("runner-teardown-error-after-success");
        let output_path = temp_root.join("candidate.jsonl");
        let events = Rc::new(RefCell::new(vec!["provision".to_string()]));
        let environment = Box::new(RecordingEnvironmentHandle::with_teardown_error(
            events.clone(),
            "teardown failed",
        ));
        let scenario = synthetic_scenario("synthetic_copy");
        let filtered = vec![&scenario];

        let err = emit_scenario_artifacts_with_dependencies(
            std::path::Path::new("."),
            &filtered,
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
            environment,
            |_, scenario, _, _, artifact_context, _| {
                events.borrow_mut().push("execute".to_string());
                Ok(RunResult::success(
                    "lab",
                    scenario.id.clone(),
                    artifact_context,
                    SyntheticRunSpec {
                        benchmark_kind: BenchmarkKind::Pipeline,
                        build_mode: "debug".to_string(),
                        connector_metrics: json!({}),
                        records_written: 100,
                        aot: false,
                    },
                ))
            },
        )
        .expect_err("teardown failure should bubble up after success");

        assert!(err.to_string().contains("teardown failed"));
        assert_eq!(
            events.borrow().as_slice(),
            &["provision", "execute", "teardown"]
        );
    }

    #[test]
    fn emit_scenario_artifacts_tears_down_environment_after_execution_failure() {
        let temp_root = temp_dir("runner-teardown-failure");
        let output_path = temp_root.join("candidate.jsonl");
        let events = Rc::new(RefCell::new(vec!["provision".to_string()]));
        let environment = Box::new(RecordingEnvironmentHandle::success(events.clone()));
        let scenario = synthetic_scenario("synthetic_copy");
        let filtered = vec![&scenario];

        let err = emit_scenario_artifacts_with_dependencies(
            std::path::Path::new("."),
            &filtered,
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
            environment,
            |_, _, _, _, _, _| {
                events.borrow_mut().push("execute".to_string());
                Err(anyhow::anyhow!("benchmark failed"))
            },
        )
        .expect_err("execution failure should bubble up");

        assert!(err.to_string().contains("benchmark failed"));
        assert_eq!(
            events.borrow().as_slice(),
            &["provision", "execute", "teardown"]
        );
    }

    #[test]
    fn emit_scenario_artifacts_reports_execution_failure_with_teardown_context() {
        let temp_root = temp_dir("runner-teardown-context");
        let output_path = temp_root.join("candidate.jsonl");
        let events = Rc::new(RefCell::new(vec!["provision".to_string()]));
        let environment = Box::new(RecordingEnvironmentHandle::with_teardown_error(
            events.clone(),
            "teardown failed",
        ));
        let scenario = synthetic_scenario("synthetic_copy");
        let filtered = vec![&scenario];

        let err = emit_scenario_artifacts_with_dependencies(
            std::path::Path::new("."),
            &filtered,
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
            environment,
            |_, _, _, _, _, _| {
                events.borrow_mut().push("execute".to_string());
                Err(anyhow::anyhow!("benchmark failed"))
            },
        )
        .expect_err("execution failure should bubble up");

        let rendered = err.to_string();
        assert!(rendered.contains("benchmark failed"));
        assert!(rendered.contains("teardown failed"));
        assert_eq!(
            events.borrow().as_slice(),
            &["provision", "execute", "teardown"]
        );
    }

    #[test]
    fn emit_scenario_artifacts_skips_environment_acquisition_when_no_scenarios_match() {
        let output_path = temp_dir("runner-no-matching-scenarios").join("candidate.jsonl");
        let written = emit_scenario_artifacts(
            std::path::Path::new("."),
            &[synthetic_scenario("synthetic_copy")],
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["typoed_scenario".to_string()],
                env_profile: Some("definitely-not-a-real-profile"),
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
        )
        .expect("empty selection should not acquire environment");

        assert_eq!(written, 0);
        let rendered = fs::read_to_string(&output_path).expect("read output");
        assert!(rendered.is_empty());
    }

    #[test]
    fn emit_scenario_artifacts_tears_down_environment_when_output_creation_fails() {
        let temp_root = temp_dir("runner-output-failure");
        let events = Rc::new(RefCell::new(vec!["provision".to_string()]));
        let environment = Box::new(RecordingEnvironmentHandle::success(events.clone()));
        let scenario = synthetic_scenario("synthetic_copy");
        let filtered = vec![&scenario];

        let err = emit_scenario_artifacts_with_dependencies(
            std::path::Path::new("."),
            &filtered,
            EmitArtifactsOptions {
                suite: Some("lab"),
                scenario_ids: &["synthetic_copy".to_string()],
                env_profile: None,
                hardware_class: Some("local-dev"),
                rapidbyte_bin: None,
                output_path: &temp_root,
            },
            environment,
            |_, _, _, _, _, _| {
                events.borrow_mut().push("execute".to_string());
                Err(anyhow::anyhow!("execute should not run"))
            },
        )
        .expect_err("directory output path should fail");

        assert!(!err.to_string().contains("execute should not run"));
        assert_eq!(events.borrow().as_slice(), &["provision", "teardown"]);
    }

    #[test]
    fn session_backed_environment_handle_resolves_logical_environment_from_session_profile() {
        let session = EnvironmentSession::provision(
            std::path::Path::new("/repo"),
            existing_profile(),
            &NoopEnvironmentCommandExecutor,
        )
        .expect("provision session");
        let handle = SessionBackedEnvironmentHandle {
            session: Some(session),
        };

        let resolved = handle
            .resolve_for_scenario(&logical_environment_scenario("pg_dest_insert"))
            .expect("resolve logical environment")
            .expect("resolved postgres environment");

        assert_eq!(resolved.stream_name, "bench_events");
        assert_eq!(resolved.source.host, "127.0.0.1");
        assert_eq!(resolved.destination.schema, "raw");
    }

    #[test]
    fn rapidbyte_run_command_sets_plugin_dir() {
        let repo_root = std::path::Path::new("/tmp/rapidbyte");
        let pipeline_path = std::path::Path::new("/tmp/rapidbyte/bench.yaml");
        let scenario = sample_postgres_scenario("insert");

        let command = rapidbyte_run_command(repo_root, pipeline_path, &scenario, None);
        let envs = command.get_envs().collect::<Vec<_>>();
        let plugin_dir = envs
            .iter()
            .find_map(|(key, value)| {
                if *key == std::ffi::OsStr::new("RAPIDBYTE_PLUGIN_DIR") {
                    value.as_deref()
                } else {
                    None
                }
            })
            .expect("plugin dir env");

        assert_eq!(
            plugin_dir,
            std::ffi::OsStr::new("/tmp/rapidbyte/target/plugins")
        );
    }

    #[test]
    fn release_scenarios_use_release_runner() {
        let repo_root = std::path::Path::new("/tmp/rapidbyte");
        let pipeline_path = std::path::Path::new("/tmp/rapidbyte/bench.yaml");
        let mut scenario = sample_postgres_scenario("copy");
        scenario.benchmark.build_mode = BenchmarkBuildMode::Release;
        scenario.benchmark.aot = true;

        let command = rapidbyte_run_command(repo_root, pipeline_path, &scenario, None);
        let args = command.get_args().collect::<Vec<_>>();
        let program = command.get_program();
        let envs = command.get_envs().collect::<Vec<_>>();

        assert_eq!(
            program,
            std::ffi::OsStr::new("/tmp/rapidbyte/target/release/rapidbyte")
        );
        assert!(args.contains(&std::ffi::OsStr::new("run")));
        let aot = envs
            .iter()
            .find_map(|(key, value)| {
                if *key == std::ffi::OsStr::new("RAPIDBYTE_WASMTIME_AOT") {
                    value.as_deref()
                } else {
                    None
                }
            })
            .expect("aot env");
        assert_eq!(aot, std::ffi::OsStr::new("1"));
    }

    #[test]
    fn artifacts_record_selected_build_mode_and_aot() {
        let mut scenario = sample_postgres_scenario("copy");
        scenario.id = "pg_dest_copy_release".to_string();
        scenario.benchmark.build_mode = BenchmarkBuildMode::Release;
        scenario.benchmark.aot = true;

        let bench_json = parse_bench_json_from_stdout(&format!(
            "noise\n{BENCH_JSON_MARKER}{}",
            sample_bench_json(1_000, 1_000)
        ))
        .expect("parse bench json");
        let workload = resolve_workload_plan(&scenario).expect("workload");
        let bench_json_value: JsonValue =
            serde_json::from_str(&sample_bench_json(1_000, 1_000)).expect("bench json");
        let correctness = enforce_row_count_assertions(&scenario.id, &workload, &bench_json_value)
            .expect("correctness");
        let artifact = materialize_artifact(
            run_result_from_bench_json(&scenario, &artifact_context(), bench_json, correctness)
                .expect("run result"),
        )
        .expect("artifact");

        assert_eq!(artifact.build_mode, "release");
        assert_eq!(artifact.execution_flags["aot"], true);
    }

    #[test]
    fn source_benchmark_execution_uses_real_harness_path() {
        let mut scenario = sample_postgres_scenario("insert");
        scenario.id = "pg_source_read".to_string();
        scenario.kind = BenchmarkKind::Source;
        scenario.connectors = vec![ScenarioConnectorRef {
            kind: "source".to_string(),
            plugin: "postgres".to_string(),
        }];
        scenario.environment = EnvironmentConfig {
            reference: None,
            stream_name: None,
            postgres: Some(PostgresBenchmarkEnvironment {
                stream_name: "bench_events".to_string(),
                source: PostgresConnectionProfile {
                    host: "source-db".to_string(),
                    port: 5432,
                    user: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "bench_source".to_string(),
                    schema: "analytics".to_string(),
                },
                destination: PostgresConnectionProfile {
                    host: "dest-db".to_string(),
                    port: 5432,
                    user: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "bench_dest".to_string(),
                    schema: "raw".to_string(),
                },
            }),
        };

        let workload = resolve_workload_plan(&scenario).expect("workload");
        let env = scenario.environment.postgres.as_ref();
        let err = execute_scenario_once(
            std::path::Path::new("."),
            &scenario,
            &workload,
            env,
            &artifact_context(),
            None,
        )
        .expect_err("source not implemented");

        assert!(!err.to_string().contains("not implemented"));
    }

    #[test]
    fn destination_benchmark_execution_uses_real_harness_path() {
        let mut scenario = sample_postgres_scenario("insert");
        scenario.id = "pg_dest_write".to_string();
        scenario.kind = BenchmarkKind::Destination;
        scenario.connectors = vec![ScenarioConnectorRef {
            kind: "destination".to_string(),
            plugin: "postgres".to_string(),
        }];

        let workload = resolve_workload_plan(&scenario).expect("workload");
        let env = scenario.environment.postgres.as_ref();
        let err = execute_scenario_once(
            std::path::Path::new("."),
            &scenario,
            &workload,
            env,
            &artifact_context(),
            None,
        )
        .expect_err("destination not implemented");

        assert!(!err.to_string().contains("not implemented"));
    }

    #[test]
    fn transform_benchmark_execution_uses_real_harness_path() {
        let mut scenario = sample_postgres_scenario("insert");
        scenario.id = "sql_transform".to_string();
        scenario.kind = BenchmarkKind::Transform;
        scenario.connectors = vec![ScenarioConnectorRef {
            kind: "transform".to_string(),
            plugin: "sql".to_string(),
        }];

        let workload = resolve_workload_plan(&scenario).expect("workload");
        let env = scenario.environment.postgres.as_ref();
        let err = execute_scenario_once(
            std::path::Path::new("."),
            &scenario,
            &workload,
            env,
            &artifact_context(),
            None,
        )
        .expect_err("transform not implemented");

        assert!(!err.to_string().contains("not implemented"));
    }

    #[test]
    fn committed_pr_smoke_scenario_requires_environment_profile_resolution() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let output_path = std::env::temp_dir().join(format!(
            "rapidbyte-pr-smoke-env-required-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let scenario =
            crate::scenario::ScenarioManifest::from_path(&root.join("scenarios/pr/smoke.yaml"))
                .expect("smoke scenario");

        let err = emit_scenario_artifacts(
            root,
            &[scenario],
            EmitArtifactsOptions {
                suite: Some("pr"),
                scenario_ids: &["pr_smoke_pipeline".to_string()],
                env_profile: None,
                hardware_class: Some("ci"),
                rapidbyte_bin: None,
                output_path: &output_path,
            },
        )
        .expect_err("pr smoke should require env profile");

        assert!(err.to_string().contains("requires environment profile"));
    }

    fn sample_postgres_scenario(load_method: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: format!("pg_dest_{load_method}"),
            name: format!("Postgres destination via {load_method}"),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string()],
            connectors: vec![
                ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                },
                ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                },
            ],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 1_000,
            },
            execution: ExecutionProfile {
                iterations: 3,
                warmups: 1,
            },
            benchmark: Default::default(),
            environment: EnvironmentConfig {
                reference: None,
                stream_name: None,
                postgres: Some(PostgresBenchmarkEnvironment {
                    stream_name: "bench_events".to_string(),
                    source: PostgresConnectionProfile {
                        host: "source-db".to_string(),
                        port: 5432,
                        user: "postgres".to_string(),
                        password: "postgres".to_string(),
                        database: "bench_source".to_string(),
                        schema: "analytics".to_string(),
                    },
                    destination: PostgresConnectionProfile {
                        host: "dest-db".to_string(),
                        port: 5432,
                        user: "postgres".to_string(),
                        password: "postgres".to_string(),
                        database: "bench_dest".to_string(),
                        schema: "raw".to_string(),
                    },
                }),
            },
            connector_options: ConnectorOptions {
                source: SourceConnectorOptions {
                    sync_mode: Some(SyncMode::FullRefresh),
                    config: Default::default(),
                },
                destination: DestinationConnectorOptions {
                    load_method: Some(load_method.to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
                transforms: vec![],
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(1_000),
                expected_records_written: Some(1_000),
            },
        }
    }

    fn sample_bench_json(records_read: u64, records_written: u64) -> String {
        json!({
            "records_read": records_read,
            "records_written": records_written,
            "bytes_read": 4_194_304,
            "bytes_written": 2_097_152,
            "duration_secs": 2.0,
            "source_duration_secs": 0.8,
            "dest_duration_secs": 1.2,
            "dest_connect_secs": 0.1,
            "dest_flush_secs": 1.2,
            "dest_commit_secs": 0.05,
            "dest_recv_secs": 1.1,
            "dest_vm_setup_secs": 0.02,
            "dest_arrow_decode_secs": 0.01,
            "dest_module_load_ms": 4.0,
            "source_connect_secs": 0.06,
            "source_query_secs": 0.11,
            "source_fetch_secs": 0.63,
            "source_arrow_encode_secs": 0.01,
            "source_module_load_ms": 5.0,
            "transform_count": 0,
            "transform_duration_secs": 0.0,
            "transform_module_load_ms": [],
            "retry_count": 0,
            "parallelism": 4,
            "dest_recv_count": 4,
            "process_cpu_secs": 1.5,
            "process_peak_rss_mb": 128.0,
            "available_cores": 8,
            "stream_metrics": [{
                "stream_name": "analytics.bench_events",
                "partition_index": null,
                "partition_count": null,
                "records_read": records_read,
                "records_written": records_written,
                "bytes_read": 4_194_304,
                "bytes_written": 2_097_152,
                "source_duration_secs": 0.8,
                "dest_duration_secs": 1.2,
                "dest_vm_setup_secs": 0.02,
                "dest_recv_secs": 1.1
            }]
        })
        .to_string()
    }

    fn synthetic_scenario(id: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: id.to_string(),
            name: id.to_string(),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string(), "synthetic".to_string()],
            connectors: vec![
                ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                },
                ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                },
            ],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 100,
            },
            execution: ExecutionProfile {
                iterations: 1,
                warmups: 0,
            },
            benchmark: Default::default(),
            environment: EnvironmentConfig::default(),
            connector_options: ConnectorOptions::default(),
            assertions: ScenarioAssertions::default(),
        }
    }

    fn logical_environment_scenario(id: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: id.to_string(),
            name: id.to_string(),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string()],
            connectors: vec![
                ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                },
                ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                },
            ],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 100,
            },
            execution: ExecutionProfile {
                iterations: 1,
                warmups: 0,
            },
            benchmark: Default::default(),
            environment: EnvironmentConfig {
                reference: Some("local-dev-postgres".to_string()),
                stream_name: Some("bench_events".to_string()),
                postgres: None,
            },
            connector_options: ConnectorOptions {
                source: SourceConnectorOptions {
                    sync_mode: Some(SyncMode::FullRefresh),
                    config: Default::default(),
                },
                destination: DestinationConnectorOptions {
                    load_method: Some("insert".to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
                transforms: vec![],
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(100),
                expected_records_written: Some(100),
            },
        }
    }

    struct RecordingEnvironmentHandle {
        events: Rc<RefCell<Vec<String>>>,
        teardown_error: Option<String>,
    }

    impl RecordingEnvironmentHandle {
        fn success(events: Rc<RefCell<Vec<String>>>) -> Self {
            Self {
                events,
                teardown_error: None,
            }
        }

        fn with_teardown_error(events: Rc<RefCell<Vec<String>>>, message: &str) -> Self {
            Self {
                events,
                teardown_error: Some(message.to_string()),
            }
        }
    }

    impl BenchmarkEnvironmentHandle for RecordingEnvironmentHandle {
        fn resolve_for_scenario(
            &self,
            _scenario: &ScenarioManifest,
        ) -> Result<Option<PostgresBenchmarkEnvironment>> {
            Ok(None)
        }

        fn finish(self: Box<Self>) -> Result<()> {
            self.events.borrow_mut().push("teardown".to_string());
            match self.teardown_error {
                Some(message) => Err(anyhow::anyhow!(message)),
                None => Ok(()),
            }
        }
    }

    struct NoopEnvironmentCommandExecutor;

    impl EnvironmentCommandExecutor for NoopEnvironmentCommandExecutor {
        fn run(&self, _cwd: &Path, _program: &str, _args: &[&str]) -> Result<()> {
            Ok(())
        }
    }

    fn existing_profile() -> EnvironmentProfile {
        EnvironmentProfile {
            id: "local-dev-postgres".to_string(),
            provider: EnvironmentProvider {
                kind: "existing".to_string(),
                project_dir: None,
            },
            services: std::collections::BTreeMap::from([(
                "postgres".to_string(),
                EnvironmentService {
                    kind: "postgres".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 5433,
                    user: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "rapidbyte_test".to_string(),
                },
            )]),
            bindings: EnvironmentBindings {
                source: EnvironmentBinding {
                    service: "postgres".to_string(),
                    schema: "public".to_string(),
                },
                destination: EnvironmentBinding {
                    service: "postgres".to_string(),
                    schema: "raw".to_string(),
                },
            },
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rapidbyte-benchmark-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
