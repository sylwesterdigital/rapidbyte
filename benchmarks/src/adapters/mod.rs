pub mod destination;
pub mod source;
pub mod transform;

#[cfg(test)]
mod tests {
    use rapidbyte_types::wire::Feature;

    use crate::scenario::{
        filter_scenarios, validate_scenario_capabilities, BenchmarkKind, ExecutionProfile,
        ScenarioConnectorRef, ScenarioManifest, WorkloadProfile,
    };
    use crate::workload::WorkloadFamily;

    fn sample_scenarios() -> Vec<ScenarioManifest> {
        vec![
            ScenarioManifest {
                id: "pr_smoke_pipeline".to_string(),
                name: "PR smoke pipeline".to_string(),
                suite: "pr".to_string(),
                kind: BenchmarkKind::Pipeline,
                tags: vec!["pr".to_string(), "synthetic".to_string()],
                connectors: vec![ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                }],
                requires: vec![Feature::BulkLoad],
                workload: WorkloadProfile {
                    family: WorkloadFamily::NarrowAppend,
                    rows: 1_000,
                },
                execution: ExecutionProfile {
                    iterations: 3,
                    warmups: 1,
                },
            },
            ScenarioManifest {
                id: "lab_partitioned_pipeline".to_string(),
                name: "Lab partitioned pipeline".to_string(),
                suite: "lab".to_string(),
                kind: BenchmarkKind::Pipeline,
                tags: vec!["lab".to_string(), "partitioned".to_string()],
                connectors: vec![ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                }],
                requires: vec![Feature::PartitionedRead],
                workload: WorkloadProfile {
                    family: WorkloadFamily::PartitionedScan,
                    rows: 10_000,
                },
                execution: ExecutionProfile {
                    iterations: 5,
                    warmups: 1,
                },
            },
        ]
    }

    #[test]
    fn pr_tagged_scenarios_filter_independently_from_lab() {
        let scenarios = sample_scenarios();
        let filtered = filter_scenarios(&scenarios, Some("pr"), &["pr"]);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "pr_smoke_pipeline");
    }

    #[test]
    fn capability_requirements_are_validated_before_execution() {
        let scenario = &sample_scenarios()[1];
        let err = validate_scenario_capabilities(scenario, &[]).expect_err("should fail");
        assert!(err.to_string().contains("partitioned_read"));

        validate_scenario_capabilities(scenario, &[Feature::PartitionedRead]).expect("supported");
    }
}
