pub mod destination;
pub mod source;
pub mod transform;

use anyhow::{bail, Context, Result};

use crate::scenario::{BenchmarkKind, ScenarioManifest};

pub use destination::DestinationAdapter;
pub use source::SourceAdapter;
pub use transform::TransformAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedScenarioAdapters {
    pub source: Option<SourceAdapter>,
    pub destination: Option<DestinationAdapter>,
    pub transforms: Vec<TransformAdapter>,
}

pub fn resolve_scenario_adapters(scenario: &ScenarioManifest) -> Result<ResolvedScenarioAdapters> {
    let mut resolved = ResolvedScenarioAdapters {
        source: None,
        destination: None,
        transforms: Vec::new(),
    };

    for connector in &scenario.connectors {
        match connector.kind.as_str() {
            "source" => {
                if resolved.source.is_some() {
                    bail!(
                        "scenario {} defines multiple source connectors",
                        scenario.id
                    );
                }
                let adapter = SourceAdapter::from_plugin(&connector.plugin).with_context(|| {
                    format!(
                        "scenario {} failed to resolve source plugin {}",
                        scenario.id, connector.plugin
                    )
                })?;
                resolved.source = Some(adapter);
            }
            "destination" => {
                if resolved.destination.is_some() {
                    bail!(
                        "scenario {} defines multiple destination connectors",
                        scenario.id
                    );
                }
                let adapter =
                    DestinationAdapter::from_plugin(&connector.plugin).with_context(|| {
                        format!(
                            "scenario {} failed to resolve destination plugin {}",
                            scenario.id, connector.plugin
                        )
                    })?;
                resolved.destination = Some(adapter);
            }
            "transform" => {
                let adapter =
                    TransformAdapter::from_plugin(&connector.plugin).with_context(|| {
                        format!(
                            "scenario {} failed to resolve transform plugin {}",
                            scenario.id, connector.plugin
                        )
                    })?;
                resolved.transforms.push(adapter);
            }
            other => bail!(
                "scenario {} declares unsupported connector kind {}",
                scenario.id,
                other
            ),
        }
    }

    Ok(resolved)
}

pub fn validate_scenario_adapters(scenario: &ScenarioManifest) -> Result<()> {
    let resolved = resolve_scenario_adapters(scenario)?;

    match scenario.kind {
        BenchmarkKind::Pipeline => {
            let source = resolved.source.with_context(|| {
                format!(
                    "scenario {} pipeline benchmark requires a source connector",
                    scenario.id
                )
            })?;
            let destination = resolved.destination.with_context(|| {
                format!(
                    "scenario {} pipeline benchmark requires a destination connector",
                    scenario.id
                )
            })?;
            if !source.supports_kind(scenario.kind) {
                bail!(
                    "scenario {} source adapter does not support pipeline benchmarks",
                    scenario.id
                );
            }
            if !destination.supports_kind(scenario.kind) {
                bail!(
                    "scenario {} destination adapter does not support pipeline benchmarks",
                    scenario.id
                );
            }
            for transform in resolved.transforms {
                if !transform.supports_kind(scenario.kind) {
                    bail!(
                        "scenario {} transform adapter does not support pipeline benchmarks",
                        scenario.id
                    );
                }
            }
        }
        BenchmarkKind::Source => {
            let source = resolved.source.with_context(|| {
                format!(
                    "scenario {} source benchmark requires a source connector",
                    scenario.id
                )
            })?;
            if resolved.destination.is_some() || !resolved.transforms.is_empty() {
                bail!(
                    "scenario {} source benchmark may only declare a source connector",
                    scenario.id
                );
            }
            if !source.supports_kind(scenario.kind) {
                bail!(
                    "scenario {} source adapter does not support source benchmarks",
                    scenario.id
                );
            }
        }
        BenchmarkKind::Destination => {
            let destination = resolved.destination.with_context(|| {
                format!(
                    "scenario {} destination benchmark requires a destination connector",
                    scenario.id
                )
            })?;
            if resolved.source.is_some() || !resolved.transforms.is_empty() {
                bail!(
                    "scenario {} destination benchmark may only declare a destination connector",
                    scenario.id
                );
            }
            if !destination.supports_kind(scenario.kind) {
                bail!(
                    "scenario {} destination adapter does not support destination benchmarks",
                    scenario.id
                );
            }
        }
        BenchmarkKind::Transform => {
            if resolved.source.is_some() || resolved.destination.is_some() {
                bail!(
                    "scenario {} transform benchmark may only declare transform connectors",
                    scenario.id
                );
            }
            if resolved.transforms.is_empty() {
                bail!(
                    "scenario {} transform benchmark requires at least one transform connector",
                    scenario.id
                );
            }
            for transform in resolved.transforms {
                if !transform.supports_kind(scenario.kind) {
                    bail!(
                        "scenario {} transform adapter does not support transform benchmarks",
                        scenario.id
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rapidbyte_types::wire::Feature;

    use super::{resolve_scenario_adapters, validate_scenario_adapters};
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
                environment: Default::default(),
                connector_options: Default::default(),
                assertions: Default::default(),
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
                environment: Default::default(),
                connector_options: Default::default(),
                assertions: Default::default(),
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

    #[test]
    fn scenario_connector_refs_resolve_to_supported_adapters() {
        let scenario = ScenarioManifest {
            id: "pg_dest_insert".to_string(),
            name: "Postgres destination benchmark".to_string(),
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
            environment: Default::default(),
            connector_options: Default::default(),
            assertions: Default::default(),
        };

        let resolved = resolve_scenario_adapters(&scenario).expect("resolve adapters");
        assert!(resolved.source.is_some());
        assert!(resolved.destination.is_some());
        assert!(resolved.transforms.is_empty());
    }

    #[test]
    fn unsupported_connector_plugin_fails_with_scenario_context() {
        let scenario = ScenarioManifest {
            id: "unsupported_source".to_string(),
            name: "Unsupported source benchmark".to_string(),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string()],
            connectors: vec![ScenarioConnectorRef {
                kind: "source".to_string(),
                plugin: "mysql".to_string(),
            }],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 1_000,
            },
            execution: ExecutionProfile {
                iterations: 1,
                warmups: 0,
            },
            environment: Default::default(),
            connector_options: Default::default(),
            assertions: Default::default(),
        };

        let err = resolve_scenario_adapters(&scenario).expect_err("unsupported adapter");
        assert!(err.to_string().contains("unsupported_source"));
        assert!(err.to_string().contains("mysql"));
    }

    #[test]
    fn invalid_benchmark_kind_shape_fails_validation() {
        let scenario = ScenarioManifest {
            id: "invalid_source_benchmark".to_string(),
            name: "Invalid source benchmark".to_string(),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Source,
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
                iterations: 1,
                warmups: 0,
            },
            environment: Default::default(),
            connector_options: Default::default(),
            assertions: Default::default(),
        };

        let err = validate_scenario_adapters(&scenario).expect_err("invalid shape");
        assert!(err.to_string().contains("invalid_source_benchmark"));
        assert!(err.to_string().contains("source benchmark"));
    }

    #[test]
    fn source_destination_and_transform_kinds_are_explicitly_recognized() {
        for (kind, connectors) in [
            (
                BenchmarkKind::Source,
                vec![ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                }],
            ),
            (
                BenchmarkKind::Destination,
                vec![ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                }],
            ),
            (
                BenchmarkKind::Transform,
                vec![ScenarioConnectorRef {
                    kind: "transform".to_string(),
                    plugin: "sql".to_string(),
                }],
            ),
        ] {
            let scenario = ScenarioManifest {
                id: format!("{kind:?}").to_lowercase(),
                name: "kind validation".to_string(),
                suite: "lab".to_string(),
                kind,
                tags: vec!["lab".to_string()],
                connectors,
                requires: vec![],
                workload: WorkloadProfile {
                    family: WorkloadFamily::NarrowAppend,
                    rows: 1_000,
                },
                execution: ExecutionProfile {
                    iterations: 1,
                    warmups: 0,
                },
                environment: Default::default(),
                connector_options: Default::default(),
                assertions: Default::default(),
            };

            validate_scenario_adapters(&scenario).expect("kind should be recognized");
        }
    }
}
