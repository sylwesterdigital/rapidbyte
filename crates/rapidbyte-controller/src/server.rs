//! gRPC server startup and wiring.

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::{Identity, Server, ServerTlsConfig as TonicServerTlsConfig};
use tracing::info;

use crate::agent_service::AgentServiceImpl;
use crate::middleware::BearerAuthInterceptor;
use crate::pipeline_service::PipelineServiceImpl;
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;
use crate::proto::rapidbyte::v1::{run_event, RunEvent, RunFailed, TaskError};
use crate::run_state::RunState as InternalRunState;
use crate::state::ControllerState;

/// Configuration for the controller server.
#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub signing_key: Vec<u8>,
    pub agent_reap_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub lease_check_interval: Duration,
    /// Bearer tokens for authentication. Empty requires `allow_unauthenticated`.
    pub auth_tokens: Vec<String>,
    /// Explicit escape hatch for local/dev use. Production should configure auth.
    pub allow_unauthenticated: bool,
    /// Explicit escape hatch for the built-in development preview signing key.
    pub allow_insecure_default_signing_key: bool,
    pub tls: Option<ServerTlsConfig>,
}

/// Default signing key used when no explicit key is configured.
/// Shared between controller and agent so preview tickets work out of the box.
/// **Not suitable for production** — always set `RAPIDBYTE_SIGNING_KEY` in deployed environments.
const DEFAULT_SIGNING_KEY: &[u8] = b"rapidbyte-dev-signing-key-not-for-production";

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".parse().unwrap(),
            signing_key: DEFAULT_SIGNING_KEY.to_vec(),
            agent_reap_interval: Duration::from_secs(15),
            agent_reap_timeout: Duration::from_secs(60),
            lease_check_interval: Duration::from_secs(10),
            auth_tokens: Vec::new(),
            allow_unauthenticated: false,
            allow_insecure_default_signing_key: false,
            tls: None,
        }
    }
}

fn validate_auth_config(config: &ControllerConfig) -> anyhow::Result<()> {
    if config.auth_tokens.is_empty() && !config.allow_unauthenticated {
        anyhow::bail!(
            "Controller auth is required by default. Set --auth-token / RAPIDBYTE_AUTH_TOKEN or pass --allow-unauthenticated for local development."
        );
    }
    Ok(())
}

fn validate_signing_key_config(config: &ControllerConfig) -> anyhow::Result<()> {
    if config.signing_key == DEFAULT_SIGNING_KEY && !config.allow_insecure_default_signing_key {
        anyhow::bail!(
            "Controller preview signing key must be set explicitly. Pass --signing-key / RAPIDBYTE_SIGNING_KEY or --allow-insecure-default-signing-key for local development."
        );
    }
    Ok(())
}

async fn handle_expired_lease(state: &ControllerState, task_id: &str, run_id: &str) {
    let error_msg = {
        let mut runs = state.runs.write().await;
        if let Ok(()) = runs.transition(run_id, InternalRunState::TimedOut) {
            let record = runs.get_run_mut(run_id);
            if let Some(r) = record {
                let msg = format!("Task {task_id} lease expired (agent unresponsive)");
                r.error_message = Some(msg.clone());
                Some((msg, r.attempt))
            } else {
                None
            }
        } else {
            let actual_state = runs.get_run(run_id).map(|r| r.state);
            if let Some(actual_state) = actual_state {
                tracing::warn!(
                    task_id,
                    run_id,
                    ?actual_state,
                    "Skipping lease-expiry terminal event because run could not transition to TimedOut"
                );
            }
            None
        }
    };

    if let Some((msg, attempt)) = error_msg {
        state.watchers.write().await.publish_terminal(
            run_id,
            RunEvent {
                run_id: run_id.to_string(),
                event: Some(run_event::Event::Failed(RunFailed {
                    error: Some(TaskError {
                        code: "LEASE_EXPIRED".into(),
                        message: msg,
                        retryable: true,
                        safe_to_retry: true,
                        commit_state: String::new(),
                    }),
                    attempt,
                })),
            },
        );
    }
}

/// Start the controller gRPC server.
///
/// # Errors
///
/// Returns an error if the gRPC server fails to bind or encounters a
/// transport-level failure.
pub async fn run(config: ControllerConfig) -> anyhow::Result<()> {
    validate_auth_config(&config)?;
    validate_signing_key_config(&config)?;

    if config.signing_key == DEFAULT_SIGNING_KEY {
        tracing::warn!(
            "Using default signing key — set RAPIDBYTE_SIGNING_KEY for production deployments"
        );
    }
    if config.allow_unauthenticated {
        tracing::warn!(
            "Controller authentication is disabled via explicit allow_unauthenticated override"
        );
    }

    let state = ControllerState::new(&config.signing_key);

    // Background task: reap dead agents
    let reap_state = state.clone();
    let reap_timeout = config.agent_reap_timeout;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.agent_reap_interval);
        loop {
            interval.tick().await;
            let dead = reap_state.registry.write().await.reap_dead(reap_timeout);
            for agent_id in &dead {
                info!(agent_id, "Reaped dead agent");
            }
        }
    });

    // Background task: expire leases
    let lease_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.lease_check_interval);
        loop {
            interval.tick().await;
            let expired = lease_state.tasks.write().await.expire_leases();
            for (task_id, run_id) in &expired {
                info!(task_id, run_id, "Task lease expired");
                handle_expired_lease(&lease_state, task_id, run_id).await;
            }
        }
    });

    let auth = BearerAuthInterceptor::new(config.auth_tokens.clone());

    let pipeline_svc = PipelineServiceServer::with_interceptor(
        PipelineServiceImpl::new(state.clone()),
        auth.clone(),
    );
    let agent_svc = AgentServiceServer::with_interceptor(AgentServiceImpl::new(state), auth);

    info!(addr = %config.listen_addr, "Controller listening");

    let mut server = Server::builder();
    if let Some(tls) = &config.tls {
        server = server.tls_config(TonicServerTlsConfig::new().identity(Identity::from_pem(
            tls.cert_pem.clone(),
            tls.key_pem.clone(),
        )))?;
    }

    server
        .add_service(pipeline_svc)
        .add_service(agent_svc)
        .serve(config.listen_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_is_required_by_default() {
        let config = ControllerConfig::default();
        let err = validate_auth_config(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("Controller auth is required by default"));
    }

    #[test]
    fn allow_unauthenticated_permits_empty_token_list() {
        let config = ControllerConfig {
            allow_unauthenticated: true,
            ..Default::default()
        };
        validate_auth_config(&config).unwrap();
    }

    #[test]
    fn default_signing_key_requires_explicit_override() {
        let config = ControllerConfig {
            auth_tokens: vec!["secret".into()],
            ..Default::default()
        };
        let err = validate_signing_key_config(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("preview signing key must be set explicitly"));
    }

    #[test]
    fn allow_insecure_default_signing_key_permits_dev_default() {
        let config = ControllerConfig {
            auth_tokens: vec!["secret".into()],
            allow_insecure_default_signing_key: true,
            ..Default::default()
        };
        validate_signing_key_config(&config).unwrap();
    }

    #[tokio::test]
    async fn handle_expired_lease_transitions_assigned_run_to_timed_out() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
        }

        handle_expired_lease(&state, &task_id, &run_id).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::TimedOut);
        assert_eq!(
            record.error_message.as_deref(),
            Some(format!("Task {task_id} lease expired (agent unresponsive)").as_str())
        );
    }
}
