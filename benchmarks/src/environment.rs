use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::scenario::{PostgresBenchmarkEnvironment, PostgresConnectionProfile, ScenarioManifest};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentProfile {
    pub id: String,
    pub provider: EnvironmentProvider,
    pub services: BTreeMap<String, EnvironmentService>,
    #[serde(default)]
    pub distributed: Option<DistributedRuntimeProfile>,
    pub bindings: EnvironmentBindings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedRuntimeProfile {
    pub controller_url: String,
    #[serde(default)]
    pub controller_auth_token: Option<String>,
    pub signing_key: String,
    pub agent_flight_url: String,
    #[serde(default = "default_agent_count")]
    pub agent_count: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentProvider {
    pub kind: String,
    #[serde(default)]
    pub project_dir: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
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

pub trait EnvironmentCommandExecutor {
    fn run(&self, cwd: &Path, program: &str, args: &[&str]) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemEnvironmentCommandExecutor;

impl EnvironmentCommandExecutor for SystemEnvironmentCommandExecutor {
    fn run(&self, cwd: &Path, program: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(program)
            .current_dir(cwd)
            .args(args)
            .output()
            .with_context(|| {
                format!(
                    "failed to invoke {} {} in {}",
                    program,
                    args.join(" "),
                    cwd.display()
                )
            })?;
        if output.status.success() {
            return Ok(());
        }

        anyhow::bail!(
            "{} {} failed in {}:\nstdout:\n{}\nstderr:\n{}",
            program,
            args.join(" "),
            cwd.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[derive(Debug, Clone)]
pub struct EnvironmentSession {
    profile: EnvironmentProfile,
    teardown: Option<EnvironmentTeardown>,
}

#[derive(Debug, Clone)]
enum EnvironmentTeardown {
    DockerCompose {
        project_dir: PathBuf,
        project_name: String,
    },
}

impl EnvironmentProfile {
    pub fn from_path(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read environment profile {}", path.display()))?;
        let profile: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse environment profile {}", path.display()))?;
        validate_environment_profile(&profile).with_context(|| {
            format!(
                "environment profile {} has invalid distributed environment configuration",
                path.display()
            )
        })?;
        Ok(profile)
    }
}

impl EnvironmentSession {
    pub fn provision(
        repo_root: &Path,
        profile: EnvironmentProfile,
        executor: &dyn EnvironmentCommandExecutor,
    ) -> Result<Self> {
        match profile.provider.kind.as_str() {
            "docker_compose" => {
                let project_dir = profile.provider.project_dir.as_deref().with_context(|| {
                    format!(
                        "environment profile {} requires provider.project_dir",
                        profile.id
                    )
                })?;
                let project_name = profile.provider.project_name.as_deref().with_context(|| {
                    format!(
                        "environment profile {} requires provider.project_name",
                        profile.id
                    )
                })?;
                let project_dir = absolutize(repo_root, Path::new(project_dir));
                let teardown = EnvironmentTeardown::DockerCompose {
                    project_dir: project_dir.clone(),
                    project_name: project_name.to_string(),
                };
                let provision_args = ["compose", "-p", project_name, "up", "-d", "--wait"];
                if let Err(provision_err) = executor.run(&project_dir, "docker", &provision_args) {
                    return match teardown.run(executor) {
                        Ok(()) => Err(provision_err),
                        Err(teardown_err) => Err(anyhow::anyhow!(
                            "failed to provision environment: {provision_err}; cleanup after failed provision also failed: {teardown_err}"
                        )),
                    };
                }
                Ok(Self {
                    profile,
                    teardown: Some(teardown),
                })
            }
            "existing" => Ok(Self {
                profile,
                teardown: None,
            }),
            other => anyhow::bail!("unsupported environment provider kind {}", other),
        }
    }

    pub fn profile(&self) -> &EnvironmentProfile {
        &self.profile
    }

    pub fn finish(mut self, executor: &dyn EnvironmentCommandExecutor) -> Result<()> {
        match self.teardown.take() {
            Some(teardown) => teardown.run(executor),
            None => Ok(()),
        }
    }
}

impl EnvironmentTeardown {
    fn run(&self, executor: &dyn EnvironmentCommandExecutor) -> Result<()> {
        match self {
            Self::DockerCompose {
                project_dir,
                project_name,
            } => executor.run(
                project_dir,
                "docker",
                &["compose", "-p", project_name.as_str(), "down", "-v"],
            ),
        }
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

fn default_agent_count() -> u16 {
    1
}

fn validate_environment_profile(profile: &EnvironmentProfile) -> Result<()> {
    let Some(distributed) = profile.distributed.as_ref() else {
        return Ok(());
    };

    if distributed.controller_url.trim().is_empty() {
        anyhow::bail!("distributed.controller_url must not be empty");
    }
    if distributed.signing_key.trim().is_empty() {
        anyhow::bail!("distributed.signing_key must not be empty");
    }
    if distributed.agent_flight_url.trim().is_empty() {
        anyhow::bail!("distributed.agent_flight_url must not be empty");
    }
    if distributed.agent_count == 0 {
        anyhow::bail!("distributed.agent_count must be at least 1");
    }

    Ok(())
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

#[cfg(test)]
pub fn resolve_postgres_environment(
    root: &Path,
    scenario: &ScenarioManifest,
    env_profile: Option<&str>,
) -> Result<Option<PostgresBenchmarkEnvironment>> {
    let profile = match env_profile {
        Some(profile_id) => {
            let mut profile = resolve_environment_profile(root, profile_id)?;
            apply_environment_overrides(&mut profile)?;
            Some(profile)
        }
        None => None,
    };
    resolve_postgres_environment_from_profile(scenario, profile.as_ref())
}

pub fn resolve_postgres_environment_from_profile(
    scenario: &ScenarioManifest,
    profile: Option<&EnvironmentProfile>,
) -> Result<Option<PostgresBenchmarkEnvironment>> {
    if let Some(env) = scenario.environment.postgres.clone() {
        return Ok(Some(env));
    }

    let Some(reference) = scenario.environment.reference.as_deref() else {
        return Ok(None);
    };
    let profile = profile.with_context(|| {
        format!(
            "scenario {} requires environment profile {}",
            scenario.id, reference
        )
    })?;

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
    let source = connection_profile_for_binding(profile, &profile.bindings.source)?;
    let destination = connection_profile_for_binding(profile, &profile.bindings.destination)?;

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
    use std::cell::RefCell;
    use std::fs;
    use std::path::{Path, PathBuf};

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
    fn committed_local_dev_profile_parses_with_docker_compose_project_dir() {
        let profile_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("environments/local-dev-postgres.yaml");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        assert_eq!(profile.id, "local-dev-postgres");
        assert_eq!(profile.provider.kind, "existing");
        assert!(profile.provider.project_dir.is_none());
        assert!(profile.provider.project_name.is_none());
        assert_eq!(profile.services["postgres"].port, 5433);
        assert_eq!(profile.bindings.source.schema, "public");
        assert_eq!(profile.bindings.destination.schema, "raw");
    }

    #[test]
    fn committed_local_bench_profile_parses_with_isolated_compose_metadata() {
        let profile_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("environments/local-bench-postgres.yaml");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        assert_eq!(profile.id, "local-bench-postgres");
        assert_eq!(profile.provider.kind, "docker_compose");
        assert_eq!(profile.provider.project_dir.as_deref(), Some("benchmarks"));
        assert_eq!(
            profile.provider.project_name.as_deref(),
            Some("rapidbyte-bench")
        );
        assert_eq!(profile.services["postgres"].port, 55433);
        assert_eq!(profile.bindings.source.schema, "public");
        assert_eq!(profile.bindings.destination.schema, "raw");
        assert!(profile.distributed.is_none());
    }

    #[test]
    fn parses_distributed_runtime_profile_metadata() {
        let root = temp_dir("distributed-profile");
        let profile_path = root.join("local-bench-distributed-postgres.yaml");
        fs::write(
            &profile_path,
            r#"
id: local-bench-distributed-postgres
provider:
  kind: docker_compose
  project_dir: benchmarks
  project_name: rapidbyte-bench-distributed
services:
  postgres:
    kind: postgres
    host: 127.0.0.1
    port: 55433
    user: postgres
    password: postgres
    database: rapidbyte_test
distributed:
  controller_url: http://127.0.0.1:56090
  controller_auth_token: bench-token
  signing_key: bench-signing-key
  agent_flight_url: http://127.0.0.1:56091
  agent_count: 1
bindings:
  source:
    service: postgres
    schema: public
  destination:
    service: postgres
    schema: raw
"#,
        )
        .expect("write distributed environment profile");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        let distributed = profile.distributed.expect("distributed profile metadata");
        assert_eq!(distributed.controller_url, "http://127.0.0.1:56090");
        assert_eq!(
            distributed.controller_auth_token.as_deref(),
            Some("bench-token")
        );
        assert_eq!(distributed.signing_key, "bench-signing-key");
        assert_eq!(distributed.agent_flight_url, "http://127.0.0.1:56091");
        assert_eq!(distributed.agent_count, 1);
    }

    #[test]
    fn committed_distributed_bench_profile_parses_with_runtime_endpoints() {
        let profile_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("environments/local-bench-distributed-postgres.yaml");

        let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

        assert_eq!(profile.id, "local-bench-distributed-postgres");
        assert_eq!(profile.provider.kind, "docker_compose");
        assert_eq!(
            profile.provider.project_name.as_deref(),
            Some("rapidbyte-bench-distributed")
        );
        assert_eq!(profile.services["postgres"].port, 55433);
        assert_eq!(profile.bindings.source.schema, "public");
        assert_eq!(profile.bindings.destination.schema, "raw");
        let distributed = profile.distributed.expect("distributed profile metadata");
        assert_eq!(distributed.controller_url, "http://127.0.0.1:56090");
        assert_eq!(distributed.agent_flight_url, "http://127.0.0.1:56091");
        assert_eq!(distributed.agent_count, 1);
    }

    #[test]
    fn distributed_profile_requires_signing_key_when_runtime_is_declared() {
        let root = temp_dir("distributed-profile-invalid");
        let profile_path = root.join("invalid.yaml");
        fs::write(
            &profile_path,
            r#"
id: local-bench-distributed-postgres
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
distributed:
  controller_url: http://127.0.0.1:56090
  agent_flight_url: http://127.0.0.1:56091
bindings:
  source:
    service: postgres
    schema: public
  destination:
    service: postgres
    schema: raw
"#,
        )
        .expect("write invalid distributed environment profile");

        let err = EnvironmentProfile::from_path(&profile_path).expect_err("should reject");
        let message = format!("{err:#}");
        assert!(message.contains("failed to parse environment profile"));
        assert!(message.contains("missing field `signing_key`"));
    }

    #[test]
    fn committed_local_profiles_distinguish_shared_and_owned_use() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("environments");
        let dev =
            EnvironmentProfile::from_path(&root.join("local-dev-postgres.yaml")).expect("dev");
        let bench =
            EnvironmentProfile::from_path(&root.join("local-bench-postgres.yaml")).expect("bench");

        assert_eq!(dev.id, "local-dev-postgres");
        assert_eq!(dev.provider.kind, "existing");
        assert!(dev.provider.project_dir.is_none());
        assert!(dev.provider.project_name.is_none());

        assert_eq!(bench.id, "local-bench-postgres");
        assert_eq!(bench.provider.kind, "docker_compose");
        assert_eq!(bench.provider.project_dir.as_deref(), Some("benchmarks"));
        assert_eq!(
            bench.provider.project_name.as_deref(),
            Some("rapidbyte-bench")
        );
    }

    #[test]
    fn existing_provider_does_not_run_lifecycle_commands() {
        let executor = RecordingCommandExecutor::default();
        let session = EnvironmentSession::provision(
            Path::new("/repo"),
            EnvironmentProfile {
                id: "local-dev-postgres".to_string(),
                provider: EnvironmentProvider {
                    kind: "existing".to_string(),
                    project_dir: None,
                    project_name: None,
                },
                services: BTreeMap::from([(
                    "postgres".to_string(),
                    EnvironmentService {
                        kind: "postgres".to_string(),
                        host: "127.0.0.1".to_string(),
                        port: 5433,
                        user: "postgres".to_string(),
                        password: "postgres".to_string(),
                        database: "rapidbyte_test".to_string(),
                    },
                )]),
                distributed: None,
                bindings: EnvironmentBindings {
                    source: EnvironmentBinding {
                        service: "postgres".to_string(),
                        schema: "public".to_string(),
                    },
                    destination: EnvironmentBinding {
                        service: "postgres".to_string(),
                        schema: "raw".to_string(),
                    },
                },
            },
            &executor,
        )
        .expect("provision existing session");

        session.finish(&executor).expect("finish existing session");
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn docker_compose_provider_requires_project_dir() {
        let executor = RecordingCommandExecutor::default();
        let err = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(None, Some("rapidbyte-bench".to_string())),
            &executor,
        )
        .expect_err("docker compose provider should require project_dir");

        assert!(err.to_string().contains("project_dir"));
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn docker_compose_provider_requires_project_name() {
        let executor = RecordingCommandExecutor::default();
        let err = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(Some("bench".to_string()), None),
            &executor,
        )
        .expect_err("docker compose provider should require project_name");

        assert!(err.to_string().contains("project_name"));
        assert!(executor.commands.borrow().is_empty());
    }

    #[test]
    fn docker_compose_provider_uses_repo_relative_project_dir_for_up_and_down() {
        let executor = RecordingCommandExecutor::default();
        let session = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(
                Some("bench".to_string()),
                Some("rapidbyte-bench".to_string()),
            ),
            &executor,
        )
        .expect("provision session");

        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[RecordedCommand {
                cwd: PathBuf::from("/repo/bench"),
                program: "docker".to_string(),
                args: vec![
                    "compose".to_string(),
                    "-p".to_string(),
                    "rapidbyte-bench".to_string(),
                    "up".to_string(),
                    "-d".to_string(),
                    "--wait".to_string(),
                ],
            }]
        );

        session.finish(&executor).expect("teardown session");

        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "up".to_string(),
                        "-d".to_string(),
                        "--wait".to_string(),
                    ],
                },
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "down".to_string(),
                        "-v".to_string(),
                    ],
                }
            ]
        );
    }

    #[test]
    fn docker_compose_provider_tears_down_after_failed_up() {
        let executor =
            RecordingCommandExecutor::with_results(vec![Err(anyhow::anyhow!("up failed")), Ok(())]);
        let err = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(
                Some("bench".to_string()),
                Some("rapidbyte-bench".to_string()),
            ),
            &executor,
        )
        .expect_err("failed up should fail provisioning");

        assert!(err.to_string().contains("up failed"));
        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "up".to_string(),
                        "-d".to_string(),
                        "--wait".to_string(),
                    ],
                },
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "down".to_string(),
                        "-v".to_string(),
                    ],
                }
            ]
        );
    }

    #[test]
    fn docker_compose_provider_reports_cleanup_failure_after_failed_up() {
        let executor = RecordingCommandExecutor::with_results(vec![
            Err(anyhow::anyhow!("up failed")),
            Err(anyhow::anyhow!("down failed")),
        ]);
        let err = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(
                Some("bench".to_string()),
                Some("rapidbyte-bench".to_string()),
            ),
            &executor,
        )
        .expect_err("failed up should fail provisioning");

        let rendered = err.to_string();
        assert!(rendered.contains("up failed"));
        assert!(rendered.contains("down failed"));
        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "up".to_string(),
                        "-d".to_string(),
                        "--wait".to_string(),
                    ],
                },
                RecordedCommand {
                    cwd: PathBuf::from("/repo/bench"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "down".to_string(),
                        "-v".to_string(),
                    ],
                }
            ]
        );
    }

    #[test]
    fn docker_compose_provider_uses_project_name_for_owned_benchmark_stack() {
        let executor = RecordingCommandExecutor::default();
        let session = EnvironmentSession::provision(
            Path::new("/repo"),
            sample_profile(
                Some("benchmarks".to_string()),
                Some("rapidbyte-bench".to_string()),
            ),
            &executor,
        )
        .expect("provision session");

        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[RecordedCommand {
                cwd: PathBuf::from("/repo/benchmarks"),
                program: "docker".to_string(),
                args: vec![
                    "compose".to_string(),
                    "-p".to_string(),
                    "rapidbyte-bench".to_string(),
                    "up".to_string(),
                    "-d".to_string(),
                    "--wait".to_string(),
                ],
            }]
        );

        session.finish(&executor).expect("teardown session");

        assert_eq!(
            executor.commands.borrow().as_slice(),
            &[
                RecordedCommand {
                    cwd: PathBuf::from("/repo/benchmarks"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "up".to_string(),
                        "-d".to_string(),
                        "--wait".to_string(),
                    ],
                },
                RecordedCommand {
                    cwd: PathBuf::from("/repo/benchmarks"),
                    program: "docker".to_string(),
                    args: vec![
                        "compose".to_string(),
                        "-p".to_string(),
                        "rapidbyte-bench".to_string(),
                        "down".to_string(),
                        "-v".to_string(),
                    ],
                }
            ]
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedCommand {
        cwd: PathBuf,
        program: String,
        args: Vec<String>,
    }

    #[derive(Default)]
    struct RecordingCommandExecutor {
        commands: RefCell<Vec<RecordedCommand>>,
        results: RefCell<Vec<Result<()>>>,
    }

    impl EnvironmentCommandExecutor for RecordingCommandExecutor {
        fn run(&self, cwd: &Path, program: &str, args: &[&str]) -> Result<()> {
            self.commands.borrow_mut().push(RecordedCommand {
                cwd: cwd.to_path_buf(),
                program: program.to_string(),
                args: args.iter().map(|arg| (*arg).to_string()).collect(),
            });
            let mut results = self.results.borrow_mut();
            if results.is_empty() {
                Ok(())
            } else {
                results.remove(0)
            }
        }
    }

    impl RecordingCommandExecutor {
        fn with_results(results: Vec<Result<()>>) -> Self {
            Self {
                commands: RefCell::new(Vec::new()),
                results: RefCell::new(results),
            }
        }
    }

    fn sample_profile(
        project_dir: Option<String>,
        project_name: Option<String>,
    ) -> EnvironmentProfile {
        EnvironmentProfile {
            id: "local-dev-postgres".to_string(),
            provider: EnvironmentProvider {
                kind: "docker_compose".to_string(),
                project_dir,
                project_name,
            },
            services: BTreeMap::from([(
                "postgres".to_string(),
                EnvironmentService {
                    kind: "postgres".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 5433,
                    user: "postgres".to_string(),
                    password: "postgres".to_string(),
                    database: "rapidbyte_test".to_string(),
                },
            )]),
            distributed: None,
            bindings: EnvironmentBindings {
                source: EnvironmentBinding {
                    service: "postgres".to_string(),
                    schema: "public".to_string(),
                },
                destination: EnvironmentBinding {
                    service: "postgres".to_string(),
                    schema: "raw".to_string(),
                },
            },
        }
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
