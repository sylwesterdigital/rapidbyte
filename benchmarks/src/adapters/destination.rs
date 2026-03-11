use anyhow::{bail, Context, Result};
use postgres::{Config as PostgresConfig, NoTls};
use serde_yaml::{Mapping, Value as YamlValue};

use crate::scenario::{BenchmarkKind, PostgresBenchmarkEnvironment, ScenarioManifest};
use crate::workload::{PostgresSeedPlan, ResolvedWorkloadPlan, WorkloadFamily};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationAdapter {
    Postgres,
}

impl DestinationAdapter {
    pub fn from_plugin(plugin: &str) -> Result<Self> {
        match plugin {
            "postgres" => Ok(Self::Postgres),
            other => bail!("unsupported destination adapter plugin {other}"),
        }
    }

    pub fn supports_kind(self, kind: BenchmarkKind) -> bool {
        matches!(kind, BenchmarkKind::Pipeline | BenchmarkKind::Destination)
    }

    pub fn prepare_pipeline_mapping(
        self,
        scenario: &ScenarioManifest,
        env: &PostgresBenchmarkEnvironment,
    ) -> Result<Mapping> {
        match self {
            Self::Postgres => prepare_postgres_pipeline_mapping(scenario, env),
        }
    }

    pub fn prepare_fixtures(
        self,
        scenario: &ScenarioManifest,
        workload: &ResolvedWorkloadPlan,
        env: &PostgresBenchmarkEnvironment,
    ) -> Result<()> {
        match self {
            Self::Postgres => prepare_postgres_fixtures(scenario, workload, env),
        }
    }

    pub fn seed_plan(
        self,
        scenario: &ScenarioManifest,
        env: &PostgresBenchmarkEnvironment,
        target_row_bytes: u64,
    ) -> Result<Option<PostgresSeedPlan>> {
        match self {
            Self::Postgres => postgres_seed_plan(scenario, env, target_row_bytes),
        }
    }
}

fn prepare_postgres_pipeline_mapping(
    scenario: &ScenarioManifest,
    env: &PostgresBenchmarkEnvironment,
) -> Result<Mapping> {
    let mut destination = Mapping::new();
    destination.insert(str_key("use"), YamlValue::String("postgres".to_string()));
    destination.insert(
        str_key("config"),
        YamlValue::Mapping(render_destination_config(
            &env.destination,
            scenario
                .connector_options
                .destination
                .load_method
                .as_deref(),
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

fn prepare_postgres_fixtures(
    scenario: &ScenarioManifest,
    workload: &ResolvedWorkloadPlan,
    env: &PostgresBenchmarkEnvironment,
) -> Result<()> {
    let seed = workload
        .seed
        .as_ref()
        .context("missing postgres seed plan")?;

    let mut config = PostgresConfig::new();
    config.host(&env.destination.host);
    config.port(env.destination.port);
    config.user(&env.destination.user);
    config.password(&env.destination.password);
    config.dbname(&env.destination.database);
    let mut destination = config.connect(NoTls).with_context(|| {
        format!(
            "failed to connect to postgres {}:{}",
            env.destination.host, env.destination.port
        )
    })?;
    destination
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {dest_schema};
             DROP TABLE IF EXISTS {dest_table} CASCADE;",
            dest_schema = quote_identifier(&seed.destination_schema),
            dest_table = qualified_table_name(&seed.destination_schema, &seed.destination_table),
        ))
        .with_context(|| {
            format!(
                "failed to prepare destination fixtures for scenario {}",
                scenario.id
            )
        })?;
    Ok(())
}

fn postgres_seed_plan(
    scenario: &ScenarioManifest,
    env: &PostgresBenchmarkEnvironment,
    target_row_bytes: u64,
) -> Result<Option<PostgresSeedPlan>> {
    match scenario.workload.family {
        WorkloadFamily::NarrowAppend => Ok(Some(PostgresSeedPlan {
            source_schema: env.source.schema.clone(),
            source_table: env.stream_name.clone(),
            destination_schema: env.destination.schema.clone(),
            destination_table: env.stream_name.clone(),
            rows: scenario.workload.rows,
            target_row_bytes,
        })),
        other => bail!(
            "scenario {} postgres destination adapter does not support real workload {:?}",
            scenario.id,
            other
        ),
    }
}

fn render_destination_config(
    profile: &crate::scenario::PostgresConnectionProfile,
    load_method: Option<&str>,
    overrides: &std::collections::BTreeMap<String, YamlValue>,
) -> Result<Mapping> {
    let mut mapping = postgres_connection_mapping(profile);
    if let Some(load_method) = load_method {
        mapping.insert(
            str_key("load_method"),
            YamlValue::String(load_method.to_string()),
        );
    }
    merge_yaml_mapping(&mut mapping, overrides)?;
    Ok(mapping)
}

fn postgres_connection_mapping(profile: &crate::scenario::PostgresConnectionProfile) -> Mapping {
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
    mapping.insert(str_key("schema"), YamlValue::String(profile.schema.clone()));
    mapping
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

fn qualified_table_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn str_key(value: &str) -> YamlValue {
    YamlValue::String(value.to_string())
}
