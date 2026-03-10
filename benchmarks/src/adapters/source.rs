use std::io::Write;

use anyhow::{bail, Context, Result};
use postgres::{Client, Config as PostgresConfig, NoTls};
use serde_yaml::{Mapping, Value as YamlValue};

use crate::scenario::{BenchmarkKind, PostgresBenchmarkEnvironment, ScenarioManifest};
use crate::workload::{PostgresSeedPlan, ResolvedWorkloadPlan, WorkloadFamily};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAdapter {
    Postgres,
}

impl SourceAdapter {
    pub fn from_plugin(plugin: &str) -> Result<Self> {
        match plugin {
            "postgres" => Ok(Self::Postgres),
            other => bail!("unsupported source adapter plugin {other}"),
        }
    }

    pub fn supports_kind(self, kind: BenchmarkKind) -> bool {
        matches!(kind, BenchmarkKind::Pipeline | BenchmarkKind::Source)
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
}

fn prepare_postgres_pipeline_mapping(
    scenario: &ScenarioManifest,
    env: &PostgresBenchmarkEnvironment,
) -> Result<Mapping> {
    let mut source = Mapping::new();
    source.insert(str_key("use"), YamlValue::String("postgres".to_string()));
    source.insert(
        str_key("config"),
        YamlValue::Mapping(render_source_config(
            &env.source,
            &scenario.connector_options.source.config,
        )?),
    );

    let mut stream = Mapping::new();
    stream.insert(str_key("name"), YamlValue::String(env.stream_name.clone()));
    stream.insert(
        str_key("sync_mode"),
        serde_yaml::to_value(
            scenario
                .connector_options
                .source
                .sync_mode
                .unwrap_or(rapidbyte_types::wire::SyncMode::FullRefresh),
        )
        .context("failed to serialize source sync mode")?,
    );
    source.insert(
        str_key("streams"),
        YamlValue::Sequence(vec![YamlValue::Mapping(stream)]),
    );

    Ok(source)
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
            "scenario {} postgres source adapter does not support real workload {:?}",
            scenario.id,
            other
        ),
    }
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
    let mut source = connect_postgres(&env.source)?;
    source
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {source_schema};
             DROP TABLE IF EXISTS {source_table} CASCADE;
             CREATE TABLE {source_table} (
                 id BIGINT PRIMARY KEY,
                 tenant_id INTEGER NOT NULL,
                 event_name TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 created_at TIMESTAMPTZ NOT NULL
             );",
            source_schema = quote_identifier(&seed.source_schema),
            source_table = qualified_table_name(&seed.source_schema, &seed.source_table),
        ))
        .with_context(|| {
            format!(
                "failed to prepare source fixtures for scenario {}",
                scenario.id
            )
        })?;

    match scenario.workload.family {
        WorkloadFamily::NarrowAppend => seed_narrow_append(&mut source, seed),
        other => bail!(
            "scenario {} postgres source adapter cannot seed workload {:?}",
            scenario.id,
            other
        ),
    }
}

fn render_source_config(
    profile: &crate::scenario::PostgresConnectionProfile,
    overrides: &std::collections::BTreeMap<String, YamlValue>,
) -> Result<Mapping> {
    let mut mapping = postgres_connection_mapping(profile);
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

fn seed_narrow_append(client: &mut Client, seed: &PostgresSeedPlan) -> Result<()> {
    let copy_stmt = format!(
        "COPY {} (id, tenant_id, event_name, payload, created_at) FROM STDIN",
        qualified_table_name(&seed.source_schema, &seed.source_table)
    );
    let mut writer = client
        .copy_in(&copy_stmt)
        .context("failed to start postgres benchmark seed copy")?;

    for row in 0..seed.rows {
        let row_id = row + 1;
        let tenant_id = row % 128;
        let event_name = if row % 2 == 0 { "signup" } else { "purchase" };
        let payload = format!("payload-{row_id:010}");
        writeln!(
            writer,
            "{row_id}\t{tenant_id}\t{event_name}\t{payload}\t2024-01-01 00:00:00+00"
        )
        .context("failed to write postgres benchmark seed row")?;
    }

    let _ = writer
        .finish()
        .context("failed to finish postgres benchmark seed copy")?;
    Ok(())
}

fn connect_postgres(profile: &crate::scenario::PostgresConnectionProfile) -> Result<Client> {
    let mut config = PostgresConfig::new();
    config.host(&profile.host);
    config.port(profile.port);
    config.user(&profile.user);
    config.password(&profile.password);
    config.dbname(&profile.database);
    config.connect(NoTls).with_context(|| {
        format!(
            "failed to connect to postgres {}:{}",
            profile.host, profile.port
        )
    })
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
