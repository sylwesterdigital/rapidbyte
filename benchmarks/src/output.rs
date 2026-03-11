use std::io::Write;

use anyhow::Result;

use crate::artifact::BenchmarkArtifact;

pub fn write_artifact_json<W: Write>(writer: &mut W, artifact: &BenchmarkArtifact) -> Result<()> {
    serde_json::to_writer(&mut *writer, artifact)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_artifact_json;
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
    fn missing_correctness_assertions_fail_the_run() {
        let err = materialize_artifact(RunResult::without_assertions(
            "pr",
            "pr_smoke_pipeline",
            &artifact_context(),
        ))
        .expect_err("should fail");

        assert!(err.to_string().contains("correctness assertions"));
    }

    #[test]
    fn artifact_writer_emits_json_document() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            &artifact_context(),
            SyntheticRunSpec {
                benchmark_kind: BenchmarkKind::Pipeline,
                build_mode: "debug".to_string(),
                connector_metrics: serde_json::json!({}),
                records_written: 1_000,
                aot: false,
            },
        ))
        .expect("artifact");

        let mut buf = Vec::new();
        write_artifact_json(&mut buf, &artifact).expect("write artifact");
        let rendered = String::from_utf8(buf).expect("utf8");

        assert!(rendered.contains("\"scenario_id\":\"pr_smoke_pipeline\""));
        assert!(rendered.ends_with('\n'));
    }
}
