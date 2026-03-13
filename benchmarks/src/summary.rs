use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::artifact::BenchmarkArtifact;

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryReport {
    pub groups: Vec<SummaryGroup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryGroup {
    pub scenario_id: String,
    pub suite_id: String,
    pub benchmark_kind: String,
    pub git_sha: String,
    pub build_mode: String,
    pub aot: bool,
    pub execution_mode: Option<String>,
    pub sample_count: usize,
    pub parallelism: Option<u64>,
    pub distributed_agent_count: Option<u64>,
    pub distributed_controller_url: Option<String>,
    pub distributed_flight_endpoint: Option<String>,
    pub records_written: Option<u64>,
    pub bytes_written: Option<u64>,
    pub bytes_per_row: Option<f64>,
    pub records_per_sec: MetricSummary,
    pub mb_per_sec: MetricSummary,
    pub duration_secs: MetricSummary,
    pub correctness_passed: usize,
    pub correctness_failed: usize,
    pub connector_metric_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSummary {
    pub min: f64,
    pub median: f64,
    pub max: f64,
}

pub fn load_and_render_summary(path: &Path) -> Result<String> {
    let artifacts = load_artifacts(path)?;
    let report = summarize_artifacts(&artifacts)?;
    Ok(render_summary_report(&report))
}

pub(crate) fn load_artifacts(path: &Path) -> Result<Vec<BenchmarkArtifact>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read benchmark artifact file {}", path.display()))?;
    let mut artifacts = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let artifact: BenchmarkArtifact = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "failed to parse benchmark artifact at {}:{}",
                path.display(),
                line_no + 1
            )
        })?;
        artifacts.push(artifact);
    }

    Ok(artifacts)
}

pub fn summarize_artifacts(artifacts: &[BenchmarkArtifact]) -> Result<SummaryReport> {
    if artifacts.is_empty() {
        bail!("no benchmark artifacts found");
    }

    let mut groups = Vec::new();
    let mut remaining: Vec<&BenchmarkArtifact> = artifacts.iter().collect();
    remaining.sort_by_key(|left| identity_key(left));

    let mut current: Vec<&BenchmarkArtifact> = Vec::new();
    let mut current_key: Option<String> = None;

    for artifact in remaining {
        let key = identity_key(artifact);
        if current_key.as_deref() != Some(key.as_str()) {
            if !current.is_empty() {
                groups.push(summarize_group(&current)?);
                current.clear();
            }
            current_key = Some(key);
        }
        current.push(artifact);
    }

    if !current.is_empty() {
        groups.push(summarize_group(&current)?);
    }

    Ok(SummaryReport { groups })
}

fn summarize_group(group: &[&BenchmarkArtifact]) -> Result<SummaryGroup> {
    let first = group[0];
    let mut connector_metric_keys = BTreeSet::new();
    let mut passed = 0;
    let mut failed = 0;

    for artifact in group {
        if artifact.correctness.passed {
            passed += 1;
        } else {
            failed += 1;
        }

        if let Some(object) = artifact.connector_metrics.as_object() {
            for key in object.keys() {
                connector_metric_keys.insert(key.clone());
            }
        }
    }

    Ok(SummaryGroup {
        scenario_id: first.scenario_id.clone(),
        suite_id: first.suite_id.clone(),
        benchmark_kind: format!("{:?}", first.benchmark_kind).to_lowercase(),
        git_sha: first.git_sha.clone(),
        build_mode: first.build_mode.clone(),
        aot: first
            .execution_flags
            .get("aot")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        execution_mode: first
            .execution_flags
            .get("execution_mode")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        sample_count: group.len(),
        parallelism: median_u64(
            group
                .iter()
                .filter_map(|artifact| connector_metric_u64(artifact, "parallelism"))
                .collect(),
        ),
        distributed_agent_count: median_u64(
            group
                .iter()
                .filter_map(|artifact| distributed_agent_count(artifact))
                .collect(),
        ),
        distributed_controller_url: first_distributed_string(group, "controller_url"),
        distributed_flight_endpoint: first_distributed_string(group, "flight_endpoint"),
        records_written: median_u64(
            group
                .iter()
                .filter_map(|artifact| total_records_written(artifact))
                .collect(),
        ),
        bytes_written: median_u64(
            group
                .iter()
                .filter_map(|artifact| total_bytes_written(artifact))
                .collect(),
        ),
        bytes_per_row: median_f64(
            group
                .iter()
                .filter_map(|artifact| {
                    let records = total_records_written(artifact)?;
                    let bytes = total_bytes_written(artifact)?;
                    (records > 0).then_some(bytes as f64 / records as f64)
                })
                .collect(),
        ),
        records_per_sec: summarize_metric(group, "records_per_sec")?,
        mb_per_sec: summarize_metric(group, "mb_per_sec")?,
        duration_secs: summarize_metric(group, "duration_secs")?,
        correctness_passed: passed,
        correctness_failed: failed,
        connector_metric_keys: connector_metric_keys.into_iter().collect(),
    })
}

fn summarize_metric(group: &[&BenchmarkArtifact], name: &str) -> Result<MetricSummary> {
    let mut values = Vec::with_capacity(group.len());

    for artifact in group {
        let value = artifact
            .canonical_metrics
            .get(name)
            .and_then(|value| value.as_f64())
            .with_context(|| {
                format!(
                    "benchmark artifact for scenario {} is missing canonical metric {}",
                    artifact.scenario_id, name
                )
            })?;
        values.push(value);
    }

    values.sort_by(|left, right| left.total_cmp(right));
    let median = if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let right = values.len() / 2;
        (values[right - 1] + values[right]) / 2.0
    };

    Ok(MetricSummary {
        min: values[0],
        median,
        max: values[values.len() - 1],
    })
}

pub(crate) fn identity_key(artifact: &BenchmarkArtifact) -> String {
    format!(
        "{}|{}|{:?}|{}|{}|{}|{}|{}",
        artifact.scenario_id,
        artifact.suite_id,
        artifact.benchmark_kind,
        artifact.git_sha,
        artifact.hardware_class,
        artifact.build_mode,
        serde_json::to_string(&artifact.execution_flags).unwrap_or_default(),
        artifact.scenario_fingerprint,
    )
}

fn connector_metric_u64(artifact: &BenchmarkArtifact, key: &str) -> Option<u64> {
    artifact
        .connector_metrics
        .get(key)
        .and_then(|value| value.as_u64())
}

fn stream_metric_sum(artifact: &BenchmarkArtifact, key: &str) -> Option<u64> {
    let metrics = artifact
        .connector_metrics
        .get("stream_metrics")?
        .as_array()?;
    if metrics.is_empty() {
        return None;
    }
    Some(
        metrics
            .iter()
            .filter_map(|metric| metric.get(key).and_then(|value| value.as_u64()))
            .sum(),
    )
}

fn total_records_written(artifact: &BenchmarkArtifact) -> Option<u64> {
    stream_metric_sum(artifact, "records_written").or_else(|| {
        artifact
            .correctness
            .details
            .as_ref()?
            .get("actual_records_written")
            .and_then(|value| value.as_u64())
    })
}

fn total_bytes_written(artifact: &BenchmarkArtifact) -> Option<u64> {
    stream_metric_sum(artifact, "bytes_written").or_else(|| {
        let mb_per_sec = artifact
            .canonical_metrics
            .get("mb_per_sec")
            .and_then(|value| value.as_f64())?;
        let duration_secs = artifact
            .canonical_metrics
            .get("duration_secs")
            .and_then(|value| value.as_f64())?;
        Some((mb_per_sec * 1024.0 * 1024.0 * duration_secs).round() as u64)
    })
}

fn distributed_agent_count(artifact: &BenchmarkArtifact) -> Option<u64> {
    artifact
        .connector_metrics
        .get("distributed")?
        .get("agent_count")
        .and_then(|value| value.as_u64())
}

fn first_distributed_string(group: &[&BenchmarkArtifact], key: &str) -> Option<String> {
    group.iter().find_map(|artifact| {
        artifact
            .connector_metrics
            .get("distributed")?
            .get(key)?
            .as_str()
            .map(str::to_string)
    })
}

fn median_u64(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let median = if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let right = values.len() / 2;
        (values[right - 1] + values[right]) / 2
    };
    Some(median)
}

fn median_f64(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let median = if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let right = values.len() / 2;
        (values[right - 1] + values[right]) / 2.0
    };
    Some(median)
}

pub fn render_summary_report(report: &SummaryReport) -> String {
    let mut lines = vec!["# Benchmark Summary".to_string(), String::new()];

    for group in &report.groups {
        lines.push(format!("- scenario: {}", group.scenario_id));
        lines.push(format!("  suite: {}", group.suite_id));
        lines.push(format!("  benchmark kind: {}", group.benchmark_kind));
        lines.push(format!("  git sha: {}", group.git_sha));
        lines.push("  measurement: end-to-end wall-clock".to_string());
        lines.push(format!("  build mode: {}", group.build_mode));
        lines.push(format!("  aot: {}", group.aot));
        if let Some(execution_mode) = &group.execution_mode {
            lines.push(format!("  execution mode: {}", execution_mode));
        }
        if let Some(parallelism) = group.parallelism {
            lines.push(format!("  parallelism: {}", parallelism));
        }
        if let Some(agent_count) = group.distributed_agent_count {
            lines.push(format!("  agent count: {}", agent_count));
        }
        if let Some(controller_url) = &group.distributed_controller_url {
            lines.push(format!("  controller url: {}", controller_url));
        }
        if let Some(flight_endpoint) = &group.distributed_flight_endpoint {
            lines.push(format!("  flight endpoint: {}", flight_endpoint));
        }
        if let Some(records_written) = group.records_written {
            lines.push(format!("  records written: {}", records_written));
        }
        if let Some(bytes_written) = group.bytes_written {
            lines.push(format!("  bytes written: {}", bytes_written));
        }
        if let Some(bytes_per_row) = group.bytes_per_row {
            lines.push(format!("  bytes/row: {}", format_float(bytes_per_row)));
        }
        lines.push(format!("  samples: {}", group.sample_count));
        lines.push(format!(
            "  throughput: {} records/sec median ({} min, {} max)",
            format_float(group.records_per_sec.median),
            format_float(group.records_per_sec.min),
            format_float(group.records_per_sec.max),
        ));
        lines.push(format!(
            "  bandwidth: {} MiB/sec median ({} min, {} max)",
            format_float(group.mb_per_sec.median),
            format_float(group.mb_per_sec.min),
            format_float(group.mb_per_sec.max),
        ));
        lines.push(format!(
            "  latency: {}s median ({}s min, {}s max)",
            format_float(group.duration_secs.median),
            format_float(group.duration_secs.min),
            format_float(group.duration_secs.max),
        ));
        lines.push(format!(
            "  correctness: {} passed, {} failed",
            group.correctness_passed, group.correctness_failed
        ));
        if group.connector_metric_keys.is_empty() {
            lines.push("  connector metrics: none".to_string());
        } else {
            lines.push(format!(
                "  connector metrics: {}",
                group.connector_metric_keys.join(", ")
            ));
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

fn format_float(value: f64) -> String {
    format!("{value:.3}")
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map, Value};

    use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};
    use crate::scenario::BenchmarkKind;

    use super::{render_summary_report, summarize_artifacts};

    fn sample_artifact(
        records_per_sec: f64,
        mb_per_sec: f64,
        duration_secs: f64,
        passed: bool,
    ) -> BenchmarkArtifact {
        BenchmarkArtifact {
            schema_version: 1,
            suite_id: "lab".to_string(),
            scenario_id: "pg_dest_copy".to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
            build_mode: "debug".to_string(),
            execution_flags: json!({"aot": false}),
            canonical_metrics: json!({
                "records_per_sec": records_per_sec,
                "mb_per_sec": mb_per_sec,
                "duration_secs": duration_secs,
            }),
            connector_metrics: json!({
                "destination": {
                    "load_method": "copy"
                },
                "process": {
                    "peak_rss_mb": 128.0
                }
            }),
            correctness: ArtifactCorrectness {
                passed,
                validator: "row_count".to_string(),
                details: None,
            },
        }
    }

    fn sample_artifact_with_context(
        build_mode: &str,
        aot: bool,
        parallelism: u64,
        records_written: u64,
        bytes_written: u64,
    ) -> BenchmarkArtifact {
        BenchmarkArtifact {
            schema_version: 1,
            suite_id: "lab".to_string(),
            scenario_id: "pg_dest_copy_release".to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
            build_mode: build_mode.to_string(),
            execution_flags: json!({"aot": aot}),
            canonical_metrics: json!({
                "records_per_sec": 500_000.0,
                "mb_per_sec": 64.0,
                "duration_secs": 2.0,
            }),
            connector_metrics: json!({
                "destination": {
                    "load_method": "copy"
                },
                "parallelism": parallelism,
                "stream_metrics": [{
                    "stream_name": "bench_events",
                    "partition_index": 0,
                    "partition_count": parallelism,
                    "records_read": records_written,
                    "records_written": records_written,
                    "bytes_read": bytes_written,
                    "bytes_written": bytes_written,
                    "source_duration_secs": 1.0,
                    "dest_duration_secs": 1.5,
                    "dest_vm_setup_secs": 0.1,
                    "dest_recv_secs": 1.2
                }]
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "row_count".to_string(),
                details: None,
            },
        }
    }

    fn sample_artifact_with_execution_flags(execution_flags: Value) -> BenchmarkArtifact {
        BenchmarkArtifact {
            schema_version: 1,
            suite_id: "lab".to_string(),
            scenario_id: "pg_dest_copy_release".to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
            build_mode: "release".to_string(),
            execution_flags,
            canonical_metrics: json!({
                "records_per_sec": 500_000.0,
                "mb_per_sec": 64.0,
                "duration_secs": 2.0,
            }),
            connector_metrics: json!({
                "parallelism": 12,
                "stream_metrics": [{
                    "records_written": 1_000_000,
                    "bytes_written": 64_000_000
                }]
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "row_count".to_string(),
                details: None,
            },
        }
    }

    fn sample_distributed_artifact_with_empty_stream_metrics() -> BenchmarkArtifact {
        BenchmarkArtifact {
            schema_version: 1,
            suite_id: "lab".to_string(),
            scenario_id: "pg_dest_copy_release_distributed".to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
            build_mode: "release".to_string(),
            execution_flags: json!({
                "aot": true,
                "execution_mode": "distributed",
                "synthetic": false,
            }),
            canonical_metrics: json!({
                "records_per_sec": 1_500_000.0,
                "mb_per_sec": 96.0,
                "duration_secs": 0.6666666667,
            }),
            connector_metrics: json!({
                "parallelism": 1,
                "stream_metrics": [],
                "distributed": {
                    "controller_url": "http://127.0.0.1:56090",
                    "agent_count": 1,
                    "flight_endpoint": "http://127.0.0.1:56091"
                }
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "row_count".to_string(),
                details: Some(json!({
                    "expected_records_read": 1_000_000,
                    "actual_records_read": 1_000_000,
                    "expected_records_written": 1_000_000,
                    "actual_records_written": 1_000_000
                })),
            },
        }
    }

    #[test]
    fn summarize_artifacts_groups_samples_and_computes_stats() {
        let report = summarize_artifacts(&[
            sample_artifact(400_000.0, 48.0, 2.5, true),
            sample_artifact(450_000.0, 54.0, 2.2, true),
            sample_artifact(425_000.0, 51.0, 2.3, false),
        ])
        .expect("summary");

        assert_eq!(report.groups.len(), 1);
        let group = &report.groups[0];
        assert_eq!(group.scenario_id, "pg_dest_copy");
        assert_eq!(group.suite_id, "lab");
        assert_eq!(group.sample_count, 3);
        assert_eq!(group.correctness_passed, 2);
        assert_eq!(group.correctness_failed, 1);
        assert_eq!(group.records_per_sec.median, 425_000.0);
        assert_eq!(group.mb_per_sec.median, 51.0);
        assert_eq!(group.duration_secs.median, 2.3);
        assert_eq!(
            group.connector_metric_keys,
            vec!["destination".to_string(), "process".to_string()]
        );
    }

    #[test]
    fn summarize_artifacts_groups_equivalent_execution_flags() {
        let mut first_flags = Map::new();
        first_flags.insert("synthetic".to_string(), Value::Bool(false));
        first_flags.insert("aot".to_string(), Value::Bool(true));

        let mut second_flags = Map::new();
        second_flags.insert("aot".to_string(), Value::Bool(true));
        second_flags.insert("synthetic".to_string(), Value::Bool(false));

        let report = summarize_artifacts(&[
            sample_artifact_with_execution_flags(Value::Object(first_flags)),
            sample_artifact_with_execution_flags(Value::Object(second_flags)),
        ])
        .expect("summary");

        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.groups[0].sample_count, 2);
    }

    #[test]
    fn render_summary_report_uses_mib_label() {
        let report = summarize_artifacts(&[
            sample_artifact(400_000.0, 48.0, 2.5, true),
            sample_artifact(450_000.0, 54.0, 2.2, true),
            sample_artifact(425_000.0, 51.0, 2.3, true),
        ])
        .expect("summary");

        let rendered = render_summary_report(&report);
        assert!(rendered.contains("# Benchmark Summary"));
        assert!(rendered.contains("pg_dest_copy"));
        assert!(rendered.contains("records/sec"));
        assert!(rendered.contains("MiB/sec"));
        assert!(!rendered.contains("MB/sec"));
        assert!(rendered.contains("correctness"));
    }

    #[test]
    fn render_summary_report_includes_execution_context() {
        let report = summarize_artifacts(&[
            sample_artifact_with_context("release", true, 12, 1_000_000, 64_000_000),
            sample_artifact_with_context("release", true, 12, 1_000_000, 64_000_000),
            sample_artifact_with_context("release", true, 12, 1_000_000, 64_000_000),
        ])
        .expect("summary");

        let rendered = render_summary_report(&report);
        assert!(rendered.contains("build mode"));
        assert!(rendered.contains("release"));
        assert!(rendered.contains("aot"));
        assert!(rendered.contains("true"));
        assert!(rendered.contains("parallelism"));
        assert!(rendered.contains("12"));
        assert!(rendered.contains("records written"));
        assert!(rendered.contains("1000000"));
        assert!(rendered.contains("bytes written"));
        assert!(rendered.contains("64000000"));
        assert!(rendered.contains("bytes/row"));
        assert!(rendered.contains("end-to-end wall-clock"));
    }

    #[test]
    fn render_summary_report_includes_distributed_context_and_fallback_totals() {
        let report = summarize_artifacts(&[
            sample_distributed_artifact_with_empty_stream_metrics(),
            sample_distributed_artifact_with_empty_stream_metrics(),
            sample_distributed_artifact_with_empty_stream_metrics(),
        ])
        .expect("summary");

        let rendered = render_summary_report(&report);
        assert!(rendered.contains("execution mode"));
        assert!(rendered.contains("distributed"));
        assert!(rendered.contains("agent count"));
        assert!(rendered.contains("controller url"));
        assert!(rendered.contains("flight endpoint"));
        assert!(rendered.contains("records written"));
        assert!(rendered.contains("1000000"));
        assert!(rendered.contains("bytes written"));
        assert!(rendered.contains("67108864"));
    }

    #[test]
    fn render_summary_report_preserves_zero_write_totals_from_nonempty_stream_metrics() {
        let report = summarize_artifacts(&[
            sample_artifact_with_context("release", true, 1, 0, 0),
            sample_artifact_with_context("release", true, 1, 0, 0),
            sample_artifact_with_context("release", true, 1, 0, 0),
        ])
        .expect("summary");

        let rendered = render_summary_report(&report);
        assert!(rendered.contains("records written"));
        assert!(rendered.contains("  records written: 0"));
        assert!(rendered.contains("bytes written"));
        assert!(rendered.contains("  bytes written: 0"));
    }

    #[test]
    fn summarize_artifacts_rejects_empty_input() {
        let err = summarize_artifacts(&[]).expect_err("empty input should fail");
        assert!(err.to_string().contains("no benchmark artifacts"));
    }

    #[test]
    fn summarize_artifacts_rejects_missing_required_metric() {
        let mut artifact = sample_artifact(425_000.0, 51.0, 2.3, true);
        artifact.canonical_metrics = json!({
            "records_per_sec": 425_000.0,
            "duration_secs": 2.3,
        });

        let err = summarize_artifacts(&[artifact]).expect_err("missing metric should fail");
        assert!(err.to_string().contains("mb_per_sec"));
    }
}
