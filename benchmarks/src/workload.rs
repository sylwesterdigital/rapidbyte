#![cfg_attr(not(test), allow(dead_code))]

use anyhow::Result;

use crate::scenario::ScenarioManifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadFamily {
    NarrowAppend,
    WideAppend,
    JsonHeavy,
    HighNullDensity,
    HighCardinalityKeys,
    PartitionedScan,
    CdcMicroBatch,
    CdcBackfill,
    TransformHeavy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedWorkloadPlan {
    pub rows: u64,
    pub target_row_bytes: u64,
    pub synthetic: bool,
}

pub fn resolve_workload_plan(scenario: &ScenarioManifest) -> Result<ResolvedWorkloadPlan> {
    let target_row_bytes = match scenario.workload.family {
        WorkloadFamily::NarrowAppend => 128,
        WorkloadFamily::WideAppend => 2048,
        WorkloadFamily::JsonHeavy => 4096,
        WorkloadFamily::HighNullDensity => 512,
        WorkloadFamily::HighCardinalityKeys => 256,
        WorkloadFamily::PartitionedScan => 768,
        WorkloadFamily::CdcMicroBatch => 384,
        WorkloadFamily::CdcBackfill => 1024,
        WorkloadFamily::TransformHeavy => 1536,
    };

    Ok(ResolvedWorkloadPlan {
        rows: scenario.workload.rows,
        target_row_bytes,
        synthetic: scenario.tags.iter().any(|tag| tag == "synthetic"),
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_workload_plan, WorkloadFamily};
    use crate::scenario::{
        BenchmarkKind, ExecutionProfile, ScenarioConnectorRef, ScenarioManifest, WorkloadProfile,
    };

    #[test]
    fn logical_workload_family_resolves_to_executable_plan() {
        let scenario = ScenarioManifest {
            id: "pr_smoke_pipeline".to_string(),
            name: "PR smoke pipeline".to_string(),
            suite: "pr".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["pr".to_string(), "synthetic".to_string()],
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
        };

        let plan = resolve_workload_plan(&scenario).expect("resolve workload");
        assert_eq!(plan.rows, 1_000);
        assert!(plan.target_row_bytes > 0);
    }
}
