use crate::scenario::BenchmarkKind;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkArtifact {
    pub schema_version: u32,
    pub suite_id: String,
    pub scenario_id: String,
    #[serde(default = "default_benchmark_kind")]
    pub benchmark_kind: BenchmarkKind,
    pub git_sha: String,
    pub hardware_class: String,
    #[serde(default = "default_scenario_fingerprint")]
    pub scenario_fingerprint: String,
    pub build_mode: String,
    pub execution_flags: JsonValue,
    pub canonical_metrics: JsonValue,
    pub connector_metrics: JsonValue,
    pub correctness: ArtifactCorrectness,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactCorrectness {
    pub passed: bool,
    pub validator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
}

fn default_benchmark_kind() -> BenchmarkKind {
    BenchmarkKind::Pipeline
}

fn default_scenario_fingerprint() -> String {
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_serializes_required_top_level_fields() {
        let artifact = BenchmarkArtifact {
            schema_version: 1,
            suite_id: "pr".to_string(),
            scenario_id: "pr_smoke_pipeline".to_string(),
            benchmark_kind: BenchmarkKind::Pipeline,
            git_sha: "abc1234".to_string(),
            hardware_class: "ci-small".to_string(),
            scenario_fingerprint: "fingerprint".to_string(),
            build_mode: "release".to_string(),
            execution_flags: serde_json::json!({
                "suite": "pr",
                "synthetic": true,
            }),
            canonical_metrics: serde_json::json!({
                "duration_secs": 1.23,
                "records_per_sec": 1000.0,
            }),
            connector_metrics: serde_json::json!({
                "source": {
                    "pages": 4
                }
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "row_count".to_string(),
                details: None,
            },
        };

        let json = serde_json::to_value(&artifact).expect("serialize artifact");
        let object = json.as_object().expect("top-level object");

        for key in [
            "schema_version",
            "suite_id",
            "scenario_id",
            "benchmark_kind",
            "git_sha",
            "hardware_class",
            "scenario_fingerprint",
            "build_mode",
            "execution_flags",
            "canonical_metrics",
            "connector_metrics",
            "correctness",
        ] {
            assert!(object.contains_key(key), "missing key {key}");
        }
    }
}
