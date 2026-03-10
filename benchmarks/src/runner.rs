#![cfg_attr(not(test), allow(dead_code))]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value as JsonValue};

use crate::adapters::prepare_pipeline_fixtures;
use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};
use crate::environment::resolve_postgres_environment;
use crate::output::write_artifact_json;
use crate::pipeline::write_rendered_pipeline;
use crate::scenario::{filter_scenarios, PostgresBenchmarkEnvironment, ScenarioManifest};
use crate::workload::{resolve_workload_plan_with_environment, ResolvedWorkloadPlan};

const BENCH_JSON_MARKER: &str = "@@BENCH_JSON@@";
const DEFAULT_GIT_SHA: &str = "unknown";
const DEFAULT_HARDWARE_CLASS: &str = "local-dev";
const DEFAULT_BUILD_MODE: &str = "debug";

#[derive(Debug, Clone)]
pub struct RunResult {
    suite_id: String,
    scenario_id: String,
    connector_metrics: JsonValue,
    canonical_metrics: JsonValue,
    execution_flags: JsonValue,
    correctness_assertions_present: bool,
}

impl RunResult {
    pub fn success(
        suite_id: impl Into<String>,
        scenario_id: impl Into<String>,
        connector_metrics: JsonValue,
        records_written: u64,
    ) -> Self {
        let duration_secs = 1.0;
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            connector_metrics,
            canonical_metrics: json!({
                "duration_secs": duration_secs,
                "records_per_sec": records_written as f64 / duration_secs,
                "mb_per_sec": (records_written as f64 * 128.0) / 1024.0 / 1024.0 / duration_secs,
                "cpu_secs": 0.5,
                "peak_rss_mb": 64.0,
                "batch_count": 1,
            }),
            execution_flags: json!({
                "synthetic": true,
                "aot": false,
            }),
            correctness_assertions_present: true,
        }
    }

    pub fn without_assertions(suite_id: impl Into<String>, scenario_id: impl Into<String>) -> Self {
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            connector_metrics: JsonValue::Object(Map::new()),
            canonical_metrics: JsonValue::Object(Map::new()),
            execution_flags: json!({
                "synthetic": true,
                "aot": false,
            }),
            correctness_assertions_present: false,
        }
    }
}

pub fn materialize_artifact(result: RunResult) -> Result<BenchmarkArtifact> {
    if !result.correctness_assertions_present {
        bail!(
            "benchmark run for scenario {} is missing correctness assertions",
            result.scenario_id
        );
    }

    Ok(BenchmarkArtifact {
        schema_version: 1,
        suite_id: result.suite_id,
        scenario_id: result.scenario_id,
        git_sha: DEFAULT_GIT_SHA.to_string(),
        hardware_class: DEFAULT_HARDWARE_CLASS.to_string(),
        build_mode: DEFAULT_BUILD_MODE.to_string(),
        execution_flags: result.execution_flags,
        canonical_metrics: result.canonical_metrics,
        connector_metrics: result.connector_metrics,
        correctness: ArtifactCorrectness {
            passed: true,
            validator: "row_count".to_string(),
        },
    })
}

pub fn emit_scenario_artifacts(
    root: &Path,
    scenarios: &[ScenarioManifest],
    suite: Option<&str>,
    scenario_ids: &[String],
    env_profile: Option<&str>,
    output_path: &Path,
) -> Result<usize> {
    let filtered: Vec<&ScenarioManifest> = filter_scenarios(scenarios, suite, &[])
        .into_iter()
        .filter(|scenario| {
            scenario_ids.is_empty() || scenario_ids.iter().any(|selected| selected == &scenario.id)
        })
        .collect();
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = File::create(output_path)?;
    let mut artifact_count = 0;

    for scenario in &filtered {
        let resolved_env = resolve_postgres_environment(root, scenario, env_profile)?;
        let workload = resolve_workload_plan_with_environment(scenario, resolved_env.as_ref())?;

        for _ in 0..scenario.execution.warmups {
            let _ = execute_scenario_once(scenario, &workload, resolved_env.as_ref())?;
        }

        let measured_iterations = scenario.execution.iterations.max(1);
        for _ in 0..measured_iterations {
            let artifact = materialize_artifact(execute_scenario_once(
                scenario,
                &workload,
                resolved_env.as_ref(),
            )?)?;
            write_artifact_json(&mut file, &artifact)?;
            artifact_count += 1;
        }
    }

    Ok(artifact_count)
}

fn execute_scenario_once(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
) -> Result<RunResult> {
    match scenario.kind {
        crate::scenario::BenchmarkKind::Pipeline => {
            if workload.seed.is_some() && !workload.synthetic {
                return execute_real_postgres_pipeline(scenario, workload, environment);
            }

            Ok(RunResult::success(
                scenario.suite.clone(),
                scenario.id.clone(),
                json!({
                    "workload_family": format!("{:?}", scenario.workload.family),
                }),
                workload.rows,
            ))
        }
        crate::scenario::BenchmarkKind::Source => {
            bail!(
                "scenario {} source benchmarks are not implemented yet",
                scenario.id
            )
        }
        crate::scenario::BenchmarkKind::Destination => {
            bail!(
                "scenario {} destination benchmarks are not implemented yet",
                scenario.id
            )
        }
        crate::scenario::BenchmarkKind::Transform => {
            bail!(
                "scenario {} transform benchmarks are not implemented yet",
                scenario.id
            )
        }
    }
}

fn execute_real_postgres_pipeline(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    environment: Option<&PostgresBenchmarkEnvironment>,
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
    let bench_json = invoke_rapidbyte_run(&rendered.path)?;
    enforce_row_count_assertions(&scenario.id, workload, &bench_json)?;
    run_result_from_bench_json(scenario, bench_json)
}

fn invoke_rapidbyte_run(pipeline_path: &Path) -> Result<JsonValue> {
    let repo_root = std::env::current_dir().context("failed to determine benchmark cwd")?;
    let output = rapidbyte_run_command(&repo_root, pipeline_path)
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

fn rapidbyte_run_command(repo_root: &Path, pipeline_path: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(repo_root)
        .env("RAPIDBYTE_BENCH", "1")
        .env(
            "RAPIDBYTE_PLUGIN_DIR",
            repo_root.join("target").join("plugins"),
        )
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            "crates/rapidbyte-cli/Cargo.toml",
            "--",
            "run",
        ])
        .arg(pipeline_path);
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
    bench_json: JsonValue,
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
            "aot": false,
        }),
        correctness_assertions_present: true,
    })
}

fn enforce_row_count_assertions(
    scenario_id: &str,
    workload: &ResolvedWorkloadPlan,
    bench_json: &JsonValue,
) -> Result<()> {
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

    Ok(())
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
    use std::fs;

    use super::*;
    use crate::scenario::{
        BenchmarkKind, ConnectorOptions, DestinationConnectorOptions, EnvironmentConfig,
        ExecutionProfile, PostgresBenchmarkEnvironment, PostgresConnectionProfile,
        ScenarioAssertions, ScenarioConnectorRef, ScenarioManifest, SourceConnectorOptions,
        WorkloadProfile,
    };
    use crate::workload::{resolve_workload_plan, WorkloadFamily};

    #[test]
    fn bench_json_maps_into_benchmark_artifact() {
        let scenario = sample_postgres_scenario("insert");
        let bench_json = parse_bench_json_from_stdout(&format!(
            "noise\n{BENCH_JSON_MARKER}{}",
            sample_bench_json(1_000, 1_000)
        ))
        .expect("parse bench json");
        let run = run_result_from_bench_json(&scenario, bench_json).expect("run result");
        let artifact = materialize_artifact(run).expect("artifact");

        assert_eq!(artifact.scenario_id, "pg_dest_insert");
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
        let artifact = materialize_artifact(
            run_result_from_bench_json(&scenario, bench_json).expect("run result"),
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
            Some("lab"),
            &["synthetic_copy".to_string()],
            None,
            &output_path,
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
            Some("lab"),
            &["pg_dest_insert".to_string()],
            None,
            &output_path,
        )
        .expect_err("logical environment should require an env profile");

        assert!(err.to_string().contains("requires environment profile"));
    }

    #[test]
    fn rapidbyte_run_command_sets_plugin_dir() {
        let repo_root = std::path::Path::new("/tmp/rapidbyte");
        let pipeline_path = std::path::Path::new("/tmp/rapidbyte/bench.yaml");

        let command = rapidbyte_run_command(repo_root, pipeline_path);
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
    fn source_benchmark_execution_reports_not_implemented() {
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
        let err =
            execute_scenario_once(&scenario, &workload, env).expect_err("source not implemented");

        assert!(err.to_string().contains("pg_source_read"));
        assert!(err
            .to_string()
            .contains("source benchmarks are not implemented"));
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
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(100),
                expected_records_written: Some(100),
            },
        }
    }
}
