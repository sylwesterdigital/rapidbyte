use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::scenario::{PostgresBenchmarkEnvironment, PostgresConnectionProfile, ScenarioManifest};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentProfile {
    pub id: String,
    pub provider: EnvironmentProvider,
    pub services: BTreeMap<String, EnvironmentService>,
    pub bindings: EnvironmentBindings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentProvider {
    pub kind: String,
    #[serde(default)]
    pub project_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentService {
    pub kind: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentBindings {
    pub source: EnvironmentBinding,
    pub destination: EnvironmentBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentBinding {
    pub service: String,
    pub schema: String,
}

impl EnvironmentProfile {
    pub fn from_path(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read environment profile {}", path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse environment profile {}", path.display()))
    }
}

pub fn resolve_environment_profile(root: &Path, id_or_path: &str) -> Result<EnvironmentProfile> {
    let direct_path = Path::new(id_or_path);
    let path = if direct_path.components().count() > 1 || direct_path.extension().is_some() {
        absolutize(root, direct_path)
    } else {
        root.join("benchmarks")
            .join("environments")
            .join(format!("{id_or_path}.yaml"))
    };
    EnvironmentProfile::from_path(&path)
}

pub fn apply_environment_overrides(profile: &mut EnvironmentProfile) -> Result<()> {
    if let Some(service) = profile.services.get_mut("postgres") {
        if let Ok(host) = std::env::var("RB_BENCH_PG_HOST") {
            service.host = host;
        }
        if let Ok(port) = std::env::var("RB_BENCH_PG_PORT") {
            service.port = port
                .parse::<u16>()
                .with_context(|| format!("invalid RB_BENCH_PG_PORT value {port}"))?;
        }
        if let Ok(user) = std::env::var("RB_BENCH_PG_USER") {
            service.user = user;
        }
        if let Ok(password) = std::env::var("RB_BENCH_PG_PASSWORD") {
            service.password = password;
        }
        if let Ok(database) = std::env::var("RB_BENCH_PG_DATABASE") {
            service.database = database;
        }
    }

    if let Ok(schema) = std::env::var("RB_BENCH_PG_SOURCE_SCHEMA") {
        profile.bindings.source.schema = schema;
    }
    if let Ok(schema) = std::env::var("RB_BENCH_PG_DEST_SCHEMA") {
        profile.bindings.destination.schema = schema;
    }

    Ok(())
}

pub fn resolve_postgres_environment(
    root: &Path,
    scenario: &ScenarioManifest,
    env_profile: Option<&str>,
) -> Result<Option<PostgresBenchmarkEnvironment>> {
    if let Some(env) = scenario.environment.postgres.clone() {
        return Ok(Some(env));
    }

    let Some(reference) = scenario.environment.reference.as_deref() else {
        return Ok(None);
    };
    let profile_id = env_profile.with_context(|| {
        format!(
            "scenario {} requires environment profile {}",
            scenario.id, reference
        )
    })?;

    let mut profile = resolve_environment_profile(root, profile_id)?;
    apply_environment_overrides(&mut profile)?;

    if profile.id != reference {
        anyhow::bail!(
            "scenario {} requires environment profile {} but resolved {}",
            scenario.id,
            reference,
            profile.id
        );
    }

    let stream_name = scenario.environment.stream_name.clone().with_context(|| {
        format!(
            "scenario {} is missing environment.stream_name",
            scenario.id
        )
    })?;
    let source = connection_profile_for_binding(&profile, &profile.bindings.source)?;
    let destination = connection_profile_for_binding(&profile, &profile.bindings.destination)?;

    Ok(Some(PostgresBenchmarkEnvironment {
        stream_name,
        source,
        destination,
    }))
}

fn connection_profile_for_binding(
    profile: &EnvironmentProfile,
    binding: &EnvironmentBinding,
) -> Result<PostgresConnectionProfile> {
    let service = profile
        .services
        .get(&binding.service)
        .with_context(|| format!("unknown environment service {}", binding.service))?;
    if service.kind != "postgres" {
        anyhow::bail!(
            "service {} has unsupported kind {}",
            binding.service,
            service.kind
        );
    }

    Ok(PostgresConnectionProfile {
        host: service.host.clone(),
        port: service.port,
        user: service.user.clone(),
        password: service.password.clone(),
        database: service.database.clone(),
        schema: binding.schema.clone(),
    })
}

fn absolutize(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn parses_local_environment_profile_from_yaml() {
        let root = temp_dir("env-profile");
        let profile_path = root.join("local-dev-postgres.yaml");
        fs::write(
            &profile_path,
            r#"
id: local-dev-postgres
provider:
  kind: docker_compose
  project_dir: .
services:
  postgres:
    kind: postgres
    host: 127.0.0.1
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
bindings:
  source:
    service: postgres
    schema: analytics
  destination:
    service: postgres
    schema: raw
"#,
        )
        .expect("write environment profile");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        assert_eq!(profile.id, "local-dev-postgres");
        assert_eq!(profile.provider.kind, "docker_compose");
        assert_eq!(profile.provider.project_dir.as_deref(), Some("."));
        assert_eq!(profile.services["postgres"].host, "127.0.0.1");
        assert_eq!(profile.bindings.source.service, "postgres");
        assert_eq!(profile.bindings.destination.schema, "raw");
    }

    #[test]
    fn resolves_environment_profile_by_id_from_environment_directory() {
        let root = temp_dir("env-resolve");
        let env_dir = root.join("benchmarks").join("environments");
        fs::create_dir_all(&env_dir).expect("create env dir");
        let profile_path = env_dir.join("local-dev-postgres.yaml");
        fs::write(
            &profile_path,
            r#"
id: local-dev-postgres
provider:
  kind: existing
services:
  postgres:
    kind: postgres
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
bindings:
  source:
    service: postgres
    schema: analytics
  destination:
    service: postgres
    schema: raw
"#,
        )
        .expect("write environment profile");

        let profile =
            resolve_environment_profile(&root, "local-dev-postgres").expect("resolve profile");

        assert_eq!(profile.id, "local-dev-postgres");
        assert_eq!(profile.provider.kind, "existing");
    }

    #[test]
    fn applies_postgres_environment_overrides() {
        let root = temp_dir("env-overrides");
        let profile_path = root.join("local-dev-postgres.yaml");
        fs::write(
            &profile_path,
            r#"
id: local-dev-postgres
provider:
  kind: docker_compose
services:
  postgres:
    kind: postgres
    host: 127.0.0.1
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
bindings:
  source:
    service: postgres
    schema: analytics
  destination:
    service: postgres
    schema: raw
"#,
        )
        .expect("write environment profile");

        std::env::set_var("RB_BENCH_PG_HOST", "db.internal");
        std::env::set_var("RB_BENCH_PG_PASSWORD", "secret");
        std::env::set_var("RB_BENCH_PG_SOURCE_SCHEMA", "bench_src");
        std::env::set_var("RB_BENCH_PG_DEST_SCHEMA", "bench_dst");

        let mut profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");
        apply_environment_overrides(&mut profile).expect("apply overrides");

        assert_eq!(profile.services["postgres"].host, "db.internal");
        assert_eq!(profile.services["postgres"].password, "secret");
        assert_eq!(profile.bindings.source.schema, "bench_src");
        assert_eq!(profile.bindings.destination.schema, "bench_dst");

        std::env::remove_var("RB_BENCH_PG_HOST");
        std::env::remove_var("RB_BENCH_PG_PASSWORD");
        std::env::remove_var("RB_BENCH_PG_SOURCE_SCHEMA");
        std::env::remove_var("RB_BENCH_PG_DEST_SCHEMA");
    }

    #[test]
    fn committed_local_dev_profile_parses() {
        let profile_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("environments/local-dev-postgres.yaml");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        assert_eq!(profile.id, "local-dev-postgres");
        assert_eq!(profile.provider.kind, "docker_compose");
        assert_eq!(profile.services["postgres"].port, 5433);
        assert_eq!(profile.bindings.source.schema, "public");
        assert_eq!(profile.bindings.destination.schema, "raw");
    }

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rapidbyte-benchmark-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
