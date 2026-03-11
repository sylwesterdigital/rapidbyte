#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use crate::runner::{materialize_artifact, ArtifactContext, RunResult, SyntheticRunSpec};
    use crate::scenario::BenchmarkKind;

    fn artifact_context() -> ArtifactContext {
        ArtifactContext {
            git_sha: "abc1234".to_string(),
            hardware_class: "ci-small".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
        }
    }

    #[test]
    fn canonical_metrics_are_present_in_emitted_artifacts() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            &artifact_context(),
            SyntheticRunSpec {
                benchmark_kind: BenchmarkKind::Pipeline,
                build_mode: "debug".to_string(),
                connector_metrics: JsonValue::Null,
                records_written: 1_000,
                aot: false,
            },
        ))
        .expect("artifact");

        for key in [
            "duration_secs",
            "records_per_sec",
            "mb_per_sec",
            "cpu_secs",
            "peak_rss_mb",
            "batch_count",
        ] {
            assert!(
                artifact.canonical_metrics.get(key).is_some(),
                "missing canonical metric {key}"
            );
        }
    }

    #[test]
    fn connector_specific_metrics_attach_without_breaking_schema() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            &artifact_context(),
            SyntheticRunSpec {
                benchmark_kind: BenchmarkKind::Pipeline,
                build_mode: "debug".to_string(),
                connector_metrics: serde_json::json!({
                    "destination": {
                        "insert_statement_count": 12
                    }
                }),
                records_written: 1_000,
                aot: false,
            },
        ))
        .expect("artifact");

        assert_eq!(
            artifact.connector_metrics["destination"]["insert_statement_count"],
            serde_json::json!(12)
        );
    }
}
