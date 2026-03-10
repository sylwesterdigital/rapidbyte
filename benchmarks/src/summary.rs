#![cfg_attr(not(test), allow(dead_code))]

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
    pub sample_count: usize,
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

fn load_artifacts(path: &Path) -> Result<Vec<BenchmarkArtifact>> {
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
    remaining.sort_by(|left, right| identity_key(left).cmp(&identity_key(right)));

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
        sample_count: group.len(),
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

fn identity_key(artifact: &BenchmarkArtifact) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        artifact.scenario_id,
        artifact.suite_id,
        artifact.hardware_class,
        artifact.build_mode,
        serde_json::to_string(&artifact.execution_flags).unwrap_or_default()
    )
}

pub fn render_summary_report(report: &SummaryReport) -> String {
    let mut lines = vec!["# Benchmark Summary".to_string(), String::new()];

    for group in &report.groups {
        lines.push(format!("- scenario: {}", group.scenario_id));
        lines.push(format!("  suite: {}", group.suite_id));
        lines.push(format!("  samples: {}", group.sample_count));
        lines.push(format!(
            "  throughput: {} records/sec median ({} min, {} max)",
            format_float(group.records_per_sec.median),
            format_float(group.records_per_sec.min),
            format_float(group.records_per_sec.max),
        ));
        lines.push(format!(
            "  bandwidth: {} MB/sec median ({} min, {} max)",
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
    use serde_json::json;

    use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};

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
            git_sha: "abc1234".to_string(),
            hardware_class: "local-dev".to_string(),
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
    fn render_summary_report_includes_records_and_mb_per_sec() {
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
        assert!(rendered.contains("MB/sec"));
        assert!(rendered.contains("correctness"));
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
