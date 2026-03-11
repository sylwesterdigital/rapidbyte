use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;

use crate::artifact::BenchmarkArtifact;
use crate::summary::load_artifacts;

#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonReport {
    pub comparisons: Vec<ScenarioComparison>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioComparison {
    pub scenario_id: String,
    pub status: String,
    pub reasons: Vec<String>,
    pub baseline_samples: usize,
    pub candidate_samples: usize,
    pub baseline_throughput: Option<f64>,
    pub candidate_throughput: Option<f64>,
    pub baseline_latency: Option<f64>,
    pub candidate_latency: Option<f64>,
}

pub fn load_and_render_comparison(
    baseline: &Path,
    candidate: &Path,
    min_samples: usize,
    throughput_drop_pct: f64,
    latency_increase_pct: f64,
) -> Result<String> {
    let report = compare_artifact_sets(
        &load_artifacts(baseline)?,
        &load_artifacts(candidate)?,
        min_samples,
        throughput_drop_pct,
        latency_increase_pct,
    );
    Ok(render_markdown(&report))
}

pub fn compare_artifact_sets(
    baseline_rows: &[BenchmarkArtifact],
    candidate_rows: &[BenchmarkArtifact],
    min_samples: usize,
    throughput_drop_pct: f64,
    latency_increase_pct: f64,
) -> ComparisonReport {
    let baseline_groups = group_artifacts(baseline_rows);
    let baseline_legacy_groups = group_artifacts_by_legacy_identity(baseline_rows);
    let candidate_groups = group_artifacts(candidate_rows);
    let mut comparisons = Vec::new();

    for (identity, candidates) in candidate_groups {
        let scenario_id = candidates[0].scenario_id.clone();
        let baselines = baseline_groups.get(&identity).cloned().or_else(|| {
            baseline_legacy_groups
                .get(&compare_legacy_identity_key(candidates[0]))
                .map(|rows| {
                    rows.iter()
                        .copied()
                        .filter(|row| scenario_fingerprint_compatible(row, candidates[0]))
                        .collect::<Vec<_>>()
                })
                .filter(|rows| !rows.is_empty())
        });
        let Some(baselines) = baselines else {
            comparisons.push(ScenarioComparison {
                scenario_id,
                status: "missing_baseline".to_string(),
                reasons: vec!["no matching baseline artifacts".to_string()],
                baseline_samples: 0,
                candidate_samples: candidates.len(),
                baseline_throughput: None,
                candidate_throughput: median_metric(&candidates, "records_per_sec"),
                baseline_latency: None,
                candidate_latency: median_metric(&candidates, "duration_secs"),
            });
            continue;
        };

        if candidates.len() < min_samples || baselines.len() < min_samples {
            comparisons.push(ScenarioComparison {
                scenario_id,
                status: "insufficient_confidence".to_string(),
                reasons: vec![format!(
                    "baseline_samples={} candidate_samples={}",
                    baselines.len(),
                    candidates.len()
                )],
                baseline_samples: baselines.len(),
                candidate_samples: candidates.len(),
                baseline_throughput: median_metric(&baselines, "records_per_sec"),
                candidate_throughput: median_metric(&candidates, "records_per_sec"),
                baseline_latency: median_metric(&baselines, "duration_secs"),
                candidate_latency: median_metric(&candidates, "duration_secs"),
            });
            continue;
        }

        comparisons.push(compare_group(
            &baselines,
            &candidates,
            throughput_drop_pct,
            latency_increase_pct,
        ));
    }

    comparisons.sort_by(|left, right| left.scenario_id.cmp(&right.scenario_id));
    ComparisonReport { comparisons }
}

fn group_artifacts(rows: &[BenchmarkArtifact]) -> BTreeMap<String, Vec<&BenchmarkArtifact>> {
    let mut grouped = BTreeMap::<String, Vec<&BenchmarkArtifact>>::new();
    for row in rows {
        grouped
            .entry(compare_identity_key(row))
            .or_default()
            .push(row);
    }
    grouped
}

fn group_artifacts_by_legacy_identity(
    rows: &[BenchmarkArtifact],
) -> BTreeMap<String, Vec<&BenchmarkArtifact>> {
    let mut grouped = BTreeMap::<String, Vec<&BenchmarkArtifact>>::new();
    for row in rows {
        grouped
            .entry(compare_legacy_identity_key(row))
            .or_default()
            .push(row);
    }
    grouped
}

fn compare_identity_key(artifact: &BenchmarkArtifact) -> String {
    format!(
        "{}|{}|{:?}|{}|{}|{}|{}",
        artifact.scenario_id,
        artifact.suite_id,
        artifact.benchmark_kind,
        artifact.hardware_class,
        artifact.build_mode,
        serde_json::to_string(&artifact.execution_flags).unwrap_or_default(),
        artifact.scenario_fingerprint,
    )
}

fn compare_legacy_identity_key(artifact: &BenchmarkArtifact) -> String {
    format!(
        "{}|{}|{:?}|{}|{}|{}",
        artifact.scenario_id,
        artifact.suite_id,
        artifact.benchmark_kind,
        artifact.hardware_class,
        artifact.build_mode,
        serde_json::to_string(&artifact.execution_flags).unwrap_or_default(),
    )
}

fn scenario_fingerprint_compatible(
    baseline: &BenchmarkArtifact,
    candidate: &BenchmarkArtifact,
) -> bool {
    baseline.scenario_fingerprint == candidate.scenario_fingerprint
        || baseline.scenario_fingerprint == "unknown"
        || candidate.scenario_fingerprint == "unknown"
}

fn compare_group(
    baselines: &[&BenchmarkArtifact],
    candidates: &[&BenchmarkArtifact],
    throughput_drop_pct_threshold: f64,
    latency_increase_pct_threshold: f64,
) -> ScenarioComparison {
    let scenario_id = candidates[0].scenario_id.clone();
    let baseline_throughput = median_metric(baselines, "records_per_sec");
    let candidate_throughput = median_metric(candidates, "records_per_sec");
    let baseline_latency = median_metric(baselines, "duration_secs");
    let candidate_latency = median_metric(candidates, "duration_secs");

    let mut reasons = Vec::new();
    if let (Some(baseline), Some(candidate)) = (baseline_throughput, candidate_throughput) {
        let drop_pct = percentage_drop(baseline, candidate);
        if drop_pct > throughput_drop_pct_threshold {
            reasons.push(format!(
                "throughput dropped {:.1}% ({:.1} vs {:.1})",
                drop_pct, candidate, baseline
            ));
        }
    }
    if let (Some(baseline), Some(candidate)) = (baseline_latency, candidate_latency) {
        let increase_pct = percentage_increase(baseline, candidate);
        if increase_pct > latency_increase_pct_threshold {
            reasons.push(format!(
                "latency increased {:.1}% ({:.3}s vs {:.3}s)",
                increase_pct, candidate, baseline
            ));
        }
    }
    if candidates.iter().any(|row| !row.correctness.passed) {
        reasons.push("correctness failed".to_string());
    }

    ScenarioComparison {
        scenario_id,
        status: if reasons.is_empty() {
            "ok".to_string()
        } else {
            "regressed".to_string()
        },
        reasons,
        baseline_samples: baselines.len(),
        candidate_samples: candidates.len(),
        baseline_throughput,
        candidate_throughput,
        baseline_latency,
        candidate_latency,
    }
}

fn median_metric(rows: &[&BenchmarkArtifact], metric_name: &str) -> Option<f64> {
    let mut values = rows
        .iter()
        .filter_map(|row| {
            row.canonical_metrics
                .get(metric_name)
                .and_then(|value| value.as_f64())
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    Some(if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let right = values.len() / 2;
        (values[right - 1] + values[right]) / 2.0
    })
}

fn percentage_drop(baseline: f64, candidate: f64) -> f64 {
    if baseline <= 0.0 {
        0.0
    } else {
        ((baseline - candidate) / baseline) * 100.0
    }
}

fn percentage_increase(baseline: f64, candidate: f64) -> f64 {
    if baseline <= 0.0 {
        0.0
    } else {
        ((candidate - baseline) / baseline) * 100.0
    }
}

fn render_markdown(report: &ComparisonReport) -> String {
    let mut lines = vec!["# Benchmark Comparison".to_string(), String::new()];
    for comparison in &report.comparisons {
        lines.push(format!(
            "- `{}`: {}",
            comparison.scenario_id, comparison.status
        ));
        for reason in &comparison.reasons {
            lines.push(format!("  - {reason}"));
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};
    use crate::scenario::BenchmarkKind;

    use super::compare_artifact_sets;

    fn artifact(
        scenario_id: &str,
        git_sha: &str,
        throughput: f64,
        latency: f64,
        passed: bool,
    ) -> BenchmarkArtifact {
        BenchmarkArtifact {
            schema_version: 1,
            suite_id: "pr".to_string(),
            scenario_id: scenario_id.to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: git_sha.to_string(),
            hardware_class: "ci".to_string(),
            scenario_fingerprint: "same".to_string(),
            build_mode: "debug".to_string(),
            execution_flags: json!({"aot": false}),
            canonical_metrics: json!({
                "records_per_sec": throughput,
                "duration_secs": latency,
            }),
            connector_metrics: json!({}),
            correctness: ArtifactCorrectness {
                passed,
                validator: "row_count".to_string(),
                details: None,
            },
        }
    }

    #[test]
    fn compare_matches_across_git_sha() {
        let baseline = vec![artifact("smoke", "base111", 100.0, 1.0, true)];
        let candidate = vec![artifact("smoke", "cand222", 80.0, 1.3, true)];

        let report = compare_artifact_sets(&baseline, &candidate, 1, 10.0, 15.0);

        assert_eq!(report.comparisons[0].status, "regressed");
        assert!(report.comparisons[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("throughput dropped")));
    }

    #[test]
    fn compare_flags_regression_on_latency_and_correctness() {
        let baseline = vec![artifact("smoke", "abc123", 100.0, 1.0, true)];
        let candidate = vec![artifact("smoke", "abc123", 80.0, 1.3, false)];

        let report = compare_artifact_sets(&baseline, &candidate, 1, 10.0, 15.0);

        assert_eq!(report.comparisons[0].status, "regressed");
        assert!(report.comparisons[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("correctness failed")));
    }

    #[test]
    fn compare_matches_old_unknown_fingerprint_baseline() {
        let mut baseline_artifact = artifact("smoke", "base111", 100.0, 1.0, true);
        baseline_artifact.scenario_fingerprint = "unknown".to_string();
        let candidate = vec![artifact("smoke", "cand222", 80.0, 1.3, true)];

        let report = compare_artifact_sets(&[baseline_artifact], &candidate, 1, 10.0, 15.0);

        assert_eq!(report.comparisons[0].status, "regressed");
        assert!(report.comparisons[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("throughput dropped")));
    }
}
