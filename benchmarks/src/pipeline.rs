#![cfg_attr(not(test), allow(dead_code))]

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_yaml::{Mapping, Value as YamlValue};

use crate::scenario::{BenchmarkKind, PostgresConnectionProfile, ScenarioManifest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedPipeline {
    pub path: PathBuf,
    pub yaml: String,
}

pub fn render_pipeline_yaml(scenario: &ScenarioManifest) -> Result<String> {
    render_pipeline_yaml_with_state(scenario, None)
}

fn render_pipeline_yaml_with_state(
    scenario: &ScenarioManifest,
    state_connection: Option<&Path>,
) -> Result<String> {
    if scenario.kind != BenchmarkKind::Pipeline {
        bail!("scenario {} is not a pipeline benchmark", scenario.id);
    }

    let env = scenario
        .environment
        .postgres
        .as_ref()
        .with_context(|| format!("scenario {} is missing postgres environment", scenario.id))?;
    ensure_postgres_connector(scenario, "source")?;
    ensure_postgres_connector(scenario, "destination")?;

    let mut root = Mapping::new();
    root.insert(str_key("version"), YamlValue::String("1.0".to_string()));
    root.insert(
        str_key("pipeline"),
        YamlValue::String(format!("benchmark_{}", scenario.id)),
    );
    root.insert(
        str_key("source"),
        YamlValue::Mapping(render_source_mapping(scenario, env)?),
    );
    root.insert(
        str_key("destination"),
        YamlValue::Mapping(render_destination_mapping(scenario, env)?),
    );
    root.insert(str_key("state"), render_state_mapping(state_connection));

    serde_yaml::to_string(&root).context("failed to serialize rendered pipeline")
}

pub fn write_rendered_pipeline(scenario: &ScenarioManifest, temp_root: &Path) -> Result<RenderedPipeline> {
    fs::create_dir_all(temp_root)
        .with_context(|| format!("failed to create pipeline temp root {}", temp_root.display()))?;
    let state_path = temp_root.join(format!("{}.state.db", scenario.id));
    let yaml = render_pipeline_yaml_with_state(scenario, Some(&state_path))?;
    let path = temp_root.join(format!("{}.yaml", scenario.id));
    fs::write(&path, &yaml)
        .with_context(|| format!("failed to write rendered pipeline {}", path.display()))?;
    Ok(RenderedPipeline { path, yaml })
}

fn render_source_mapping(
    scenario: &ScenarioManifest,
    env: &crate::scenario::PostgresBenchmarkEnvironment,
) -> Result<Mapping> {
    let mut source = Mapping::new();
    source.insert(str_key("use"), YamlValue::String("postgres".to_string()));
    source.insert(
        str_key("config"),
        YamlValue::Mapping(render_source_config(&env.source, &scenario.connector_options.source.config)?),
    );

    let mut stream = Mapping::new();
    stream.insert(
        str_key("name"),
        YamlValue::String(qualified_stream_name(&env.source.schema, &env.stream_name)),
    );
    stream.insert(
        str_key("sync_mode"),
        to_yaml_value(
            scenario
                .connector_options
                .source
                .sync_mode
                .unwrap_or(rapidbyte_types::wire::SyncMode::FullRefresh),
        )?,
    );
    source.insert(
        str_key("streams"),
        YamlValue::Sequence(vec![YamlValue::Mapping(stream)]),
    );
    Ok(source)
}

fn render_destination_mapping(
    scenario: &ScenarioManifest,
    env: &crate::scenario::PostgresBenchmarkEnvironment,
) -> Result<Mapping> {
    let mut destination = Mapping::new();
    destination.insert(str_key("use"), YamlValue::String("postgres".to_string()));
    destination.insert(
        str_key("config"),
        YamlValue::Mapping(render_destination_config(
            &env.destination,
            scenario.connector_options.destination.load_method.as_deref(),
            &scenario.connector_options.destination.config,
        )?),
    );
    destination.insert(
        str_key("write_mode"),
        YamlValue::String(
            scenario
                .connector_options
                .destination
                .write_mode
                .clone()
                .unwrap_or_else(|| "append".to_string()),
        ),
    );
    Ok(destination)
}

fn render_source_config(
    profile: &PostgresConnectionProfile,
    overrides: &std::collections::BTreeMap<String, YamlValue>,
) -> Result<Mapping> {
    let mut mapping = postgres_connection_mapping(profile, false);
    merge_yaml_mapping(&mut mapping, overrides)?;
    Ok(mapping)
}

fn render_destination_config(
    profile: &PostgresConnectionProfile,
    load_method: Option<&str>,
    overrides: &std::collections::BTreeMap<String, YamlValue>,
) -> Result<Mapping> {
    let mut mapping = postgres_connection_mapping(profile, true);
    if let Some(load_method) = load_method {
        mapping.insert(
            str_key("load_method"),
            YamlValue::String(load_method.to_string()),
        );
    }
    merge_yaml_mapping(&mut mapping, overrides)?;
    Ok(mapping)
}

fn postgres_connection_mapping(profile: &PostgresConnectionProfile, include_schema: bool) -> Mapping {
    let mut mapping = Mapping::new();
    mapping.insert(str_key("host"), YamlValue::String(profile.host.clone()));
    mapping.insert(str_key("port"), YamlValue::Number(profile.port.into()));
    mapping.insert(str_key("user"), YamlValue::String(profile.user.clone()));
    mapping.insert(
        str_key("password"),
        YamlValue::String(profile.password.clone()),
    );
    mapping.insert(
        str_key("database"),
        YamlValue::String(profile.database.clone()),
    );
    if include_schema {
        mapping.insert(
            str_key("schema"),
            YamlValue::String(profile.schema.clone()),
        );
    }
    mapping
}

fn render_state_mapping(state_connection: Option<&Path>) -> YamlValue {
    let mut state = Mapping::new();
    state.insert(str_key("backend"), YamlValue::String("sqlite".to_string()));
    if let Some(path) = state_connection {
        state.insert(
            str_key("connection"),
            YamlValue::String(path.display().to_string()),
        );
    }
    YamlValue::Mapping(state)
}

fn ensure_postgres_connector(scenario: &ScenarioManifest, kind: &str) -> Result<()> {
    let connector = scenario
        .connectors
        .iter()
        .find(|entry| entry.kind == kind)
        .with_context(|| format!("scenario {} is missing {kind} connector", scenario.id))?;
    if connector.plugin != "postgres" {
        bail!(
            "scenario {} only supports postgres {kind} connector, found {}",
            scenario.id,
            connector.plugin
        );
    }
    Ok(())
}

fn qualified_stream_name(schema: &str, stream_name: &str) -> String {
    format!("{schema}.{stream_name}")
}

fn merge_yaml_mapping(
    target: &mut Mapping,
    overrides: &std::collections::BTreeMap<String, YamlValue>,
) -> Result<()> {
    for (key, value) in overrides {
        if matches!(value, YamlValue::Mapping(_)) {
            bail!("nested connector config override is not supported for key {key}");
        }
        target.insert(str_key(key), value.clone());
    }
    Ok(())
}

fn to_yaml_value<T: Serialize>(value: T) -> Result<YamlValue> {
    serde_yaml::to_value(value).context("failed to serialize pipeline value")
}

fn str_key(value: &str) -> YamlValue {
    YamlValue::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rapidbyte_engine::config::{parser::parse_pipeline_str, types::PipelineWriteMode};
    use rapidbyte_types::wire::SyncMode;

    use super::*;
    use crate::scenario::{
        BenchmarkKind, ConnectorOptions, DestinationConnectorOptions, EnvironmentConfig,
        ExecutionProfile, PostgresBenchmarkEnvironment, PostgresConnectionProfile,
        ScenarioAssertions, ScenarioConnectorRef, ScenarioManifest, SourceConnectorOptions,
        WorkloadProfile,
    };
    use crate::workload::WorkloadFamily;

    #[test]
    fn renders_valid_postgres_pipeline_yaml() {
        let scenario = sample_postgres_pipeline("insert");

        let yaml = render_pipeline_yaml(&scenario).expect("render pipeline yaml");
        let parsed = parse_pipeline_str(&yaml).expect("rendered yaml must parse");

        assert_eq!(parsed.pipeline, "benchmark_pg_dest_insert");
        assert_eq!(parsed.source.use_ref, "postgres");
        assert_eq!(parsed.destination.use_ref, "postgres");
        assert_eq!(parsed.destination.write_mode, PipelineWriteMode::Append);
        assert_eq!(parsed.source.streams.len(), 1);
        assert_eq!(parsed.source.streams[0].name, "analytics.bench_events");
        assert_eq!(parsed.source.streams[0].sync_mode, SyncMode::FullRefresh);
    }

    #[test]
    fn renders_destination_load_method_override() {
        let scenario = sample_postgres_pipeline("copy");

        let yaml = render_pipeline_yaml(&scenario).expect("render pipeline yaml");
        let parsed = parse_pipeline_str(&yaml).expect("rendered yaml must parse");

        assert_eq!(parsed.destination.config["load_method"], "copy");
        assert_eq!(parsed.destination.config["schema"], "raw");
    }

    #[test]
    fn writes_rendered_pipeline_with_environment_connection_data() {
        let scenario = sample_postgres_pipeline("insert");
        let temp_root = temp_dir("pipeline-render");

        let rendered = write_rendered_pipeline(&scenario, &temp_root).expect("write pipeline");
        let yaml = fs::read_to_string(&rendered.path).expect("read rendered pipeline");
        let parsed = parse_pipeline_str(&yaml).expect("rendered yaml must parse");

        assert!(rendered.path.starts_with(&temp_root));
        assert!(rendered.path.exists());
        assert_eq!(parsed.source.config["host"], "source-db");
        assert_eq!(parsed.source.config["database"], "bench_source");
        assert_eq!(parsed.destination.config["host"], "dest-db");
        assert_eq!(parsed.destination.config["database"], "bench_dest");
        assert_eq!(parsed.destination.config["schema"], "raw");
    }

    #[test]
    fn writes_rendered_pipeline_with_isolated_state_db_path() {
        let scenario = sample_postgres_pipeline("insert");
        let temp_root = temp_dir("pipeline-state");

        let rendered = write_rendered_pipeline(&scenario, &temp_root).expect("write pipeline");
        let yaml = fs::read_to_string(&rendered.path).expect("read rendered pipeline");
        let parsed = parse_pipeline_str(&yaml).expect("rendered yaml must parse");

        let connection = parsed
            .state
            .connection
            .expect("rendered pipeline should pin state db path");
        assert!(connection.starts_with(temp_root.to_string_lossy().as_ref()));
        assert!(connection.ends_with("pg_dest_insert.state.db"));
    }

    fn sample_postgres_pipeline(load_method: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: format!("pg_dest_{load_method}"),
            name: format!("Postgres destination via {load_method}"),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string(), "postgres".to_string()],
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
                        schema: "analytics".to_string(),
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
                    load_method: Some(load_method.to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(1_000_000),
                expected_records_written: Some(1_000_000),
            },
        }
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rapidbyte-benchmarks-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
