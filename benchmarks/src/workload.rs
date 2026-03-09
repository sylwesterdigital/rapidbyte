#![cfg_attr(not(test), allow(dead_code))]

use anyhow::Result;

use crate::scenario::{PostgresBenchmarkEnvironment, ScenarioManifest};

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
    pub seed: Option<PostgresSeedPlan>,
    pub expected_records_read: u64,
    pub expected_records_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSeedPlan {
    pub source_schema: String,
    pub source_table: String,
    pub destination_schema: String,
    pub destination_table: String,
    pub rows: u64,
    pub target_row_bytes: u64,
}

pub fn resolve_workload_plan(scenario: &ScenarioManifest) -> Result<ResolvedWorkloadPlan> {
    resolve_workload_plan_with_environment(scenario, scenario.environment.postgres.as_ref())
}

pub fn resolve_workload_plan_with_environment(
    scenario: &ScenarioManifest,
    environment: Option<&PostgresBenchmarkEnvironment>,
) -> Result<ResolvedWorkloadPlan> {
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
    let seed = environment.map(|env| PostgresSeedPlan {
            source_schema: env.source.schema.clone(),
            source_table: env.stream_name.clone(),
            destination_schema: env.destination.schema.clone(),
            destination_table: env.stream_name.clone(),
            rows: scenario.workload.rows,
            target_row_bytes,
        });
    let expected_records_read = scenario
        .assertions
        .expected_records_read
        .or_else(|| seed.as_ref().map(|plan| plan.rows))
        .unwrap_or(scenario.workload.rows);
    let expected_records_written = scenario
        .assertions
        .expected_records_written
        .or_else(|| seed.as_ref().map(|plan| plan.rows))
        .unwrap_or(scenario.workload.rows);

    Ok(ResolvedWorkloadPlan {
        rows: scenario.workload.rows,
        target_row_bytes,
        synthetic: scenario.tags.iter().any(|tag| tag == "synthetic"),
        seed,
        expected_records_read,
        expected_records_written,
    })
}

#[cfg(test)]
mod tests {
    use rapidbyte_types::wire::SyncMode;

    use super::{resolve_workload_plan, WorkloadFamily};
    use crate::scenario::{
        BenchmarkKind, ConnectorOptions, DestinationConnectorOptions, EnvironmentConfig,
        ExecutionProfile, PostgresBenchmarkEnvironment, PostgresConnectionProfile,
        ScenarioAssertions, ScenarioConnectorRef, ScenarioManifest, SourceConnectorOptions,
        WorkloadProfile,
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
            environment: Default::default(),
            connector_options: Default::default(),
            assertions: Default::default(),
        };

        let plan = resolve_workload_plan(&scenario).expect("resolve workload");
        assert_eq!(plan.rows, 1_000);
        assert!(plan.target_row_bytes > 0);
    }

    #[test]
    fn narrow_append_pipeline_resolves_to_deterministic_postgres_seed_plan() {
        let plan = resolve_workload_plan(&postgres_pipeline_scenario()).expect("resolve workload");
        let seed = plan.seed.expect("postgres seed plan");

        assert_eq!(seed.source_schema, "public");
        assert_eq!(seed.source_table, "bench_events");
        assert_eq!(seed.destination_schema, "raw");
        assert_eq!(seed.destination_table, "bench_events");
        assert_eq!(seed.rows, 1_000_000);
        assert_eq!(seed.target_row_bytes, 128);
    }

    #[test]
    fn row_count_expectations_default_from_seed_plan() {
        let plan = resolve_workload_plan(&postgres_pipeline_scenario()).expect("resolve workload");

        assert_eq!(plan.expected_records_read, 1_000_000);
        assert_eq!(plan.expected_records_written, 1_000_000);
    }

    #[test]
    fn logical_environment_reference_does_not_require_embedded_connections() {
        let plan =
            resolve_workload_plan(&logical_environment_reference_scenario()).expect("resolve");

        assert!(plan.seed.is_none());
        assert_eq!(plan.expected_records_read, 1_000_000);
        assert_eq!(plan.expected_records_written, 1_000_000);
    }

    fn postgres_pipeline_scenario() -> ScenarioManifest {
        ScenarioManifest {
            id: "pg_dest_insert".to_string(),
            name: "Postgres destination via insert".to_string(),
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
                rows: 1_000_000,
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
                        schema: "public".to_string(),
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
                    load_method: Some("insert".to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
            },
            assertions: ScenarioAssertions::default(),
        }
    }

    fn logical_environment_reference_scenario() -> ScenarioManifest {
        ScenarioManifest {
            id: "pg_dest_insert".to_string(),
            name: "Postgres destination via insert".to_string(),
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
                rows: 1_000_000,
            },
            execution: ExecutionProfile {
                iterations: 3,
                warmups: 1,
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
            assertions: ScenarioAssertions::default(),
        }
    }
}
