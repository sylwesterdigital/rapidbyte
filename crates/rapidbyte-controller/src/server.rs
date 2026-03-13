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
use crate::store;

/// Configuration for the controller server.
#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub signing_key: Vec<u8>,
    pub metadata_database_url: Option<String>,
    pub agent_reap_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub lease_check_interval: Duration,
    pub reconciliation_timeout: Duration,
    pub preview_cleanup_interval: Duration,
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
            metadata_database_url: None,
            agent_reap_interval: Duration::from_secs(15),
            agent_reap_timeout: Duration::from_secs(60),
            lease_check_interval: Duration::from_secs(10),
            reconciliation_timeout: Duration::from_secs(300),
            preview_cleanup_interval: Duration::from_secs(30),
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

fn metadata_database_url(config: &ControllerConfig) -> anyhow::Result<&str> {
    match config.metadata_database_url.as_deref().map(str::trim) {
        Some("") | None => {
            anyhow::bail!(
                "Controller metadata database URL is required. Set --metadata-database-url / RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL."
            );
        }
        Some(url) => Ok(url),
    }
}

async fn initialize_metadata_store(
    config: &ControllerConfig,
) -> anyhow::Result<store::MetadataStore> {
    let url = metadata_database_url(config)?;
    store::initialize_metadata_store(url).await
}

async fn rollback_background_timeout(
    state: &ControllerState,
    previous_run: crate::run_state::RunRecord,
    previous_task: Option<crate::scheduler::TaskRecord>,
) {
    {
        let mut runs = state.runs.write().await;
        runs.restore_run(previous_run);
    }
    if let Some(previous_task) = previous_task {
        let mut tasks = state.tasks.write().await;
        tasks.restore_task(previous_task);
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_expired_lease(
    state: &ControllerState,
    previous_task: crate::scheduler::TaskRecord,
) {
    let task_id = previous_task.task_id.clone();
    let run_id = previous_task.run_id.clone();
    let previous_run = state
        .runs
        .read()
        .await
        .get_run(&run_id)
        .cloned()
        .expect("run should exist for expired lease");
    let timeout_outcome = {
        let mut runs = state.runs.write().await;
        let target_state = if runs
            .get_run(&run_id)
            .is_some_and(|record| record.state == InternalRunState::Reconciling)
        {
            InternalRunState::RecoveryFailed
        } else {
            InternalRunState::TimedOut
        };

        if let Ok(()) = runs.transition(&run_id, target_state) {
            let record = runs.get_run_mut(&run_id);
            if let Some(r) = record {
                let (code, msg, retryable, safe_to_retry) = if target_state
                    == InternalRunState::RecoveryFailed
                {
                    (
                            "RECOVERY_TIMEOUT",
                            format!(
                                "Run recovery reconciliation timed out after controller restart for task {task_id}"
                            ),
                            false,
                            false,
                        )
                } else {
                    (
                        "LEASE_EXPIRED",
                        format!("Task {task_id} lease expired (agent unresponsive)"),
                        true,
                        true,
                    )
                };
                r.error_message = Some(msg.clone());
                r.error_code = Some(code.into());
                r.error_retryable = Some(retryable);
                r.error_safe_to_retry = Some(safe_to_retry);
                r.error_commit_state = Some(String::new());
                Some((code, msg, retryable, safe_to_retry, r.attempt))
            } else {
                None
            }
        } else {
            let actual_state = runs.get_run(&run_id).map(|r| r.state);
            if let Some(actual_state) = actual_state {
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %run_id,
                    ?actual_state,
                    "Skipping lease-expiry terminal event because run could not transition to TimedOut"
                );
            }
            None
        }
    };
    let mut durable = true;
    if let Err(error) = state.persist_task(&task_id).await {
        durable = false;
        tracing::error!(task_id = %task_id, run_id = %run_id, ?error, "failed to persist expired task");
    }
    if let Err(error) = state.persist_run(&run_id).await {
        durable = false;
        tracing::error!(task_id = %task_id, run_id = %run_id, ?error, "failed to persist timed-out run");
    }

    if !durable {
        rollback_background_timeout(state, previous_run, Some(previous_task)).await;
    }

    if durable {
        if let Some((code, msg, retryable, safe_to_retry, attempt)) = timeout_outcome {
            state.watchers.write().await.publish_terminal(
                &run_id,
                RunEvent {
                    run_id: run_id.clone(),
                    event: Some(run_event::Event::Failed(RunFailed {
                        error: Some(TaskError {
                            code: code.into(),
                            message: msg,
                            retryable,
                            safe_to_retry,
                            commit_state: String::new(),
                        }),
                        attempt,
                    })),
                },
            );
        }
    } else if timeout_outcome.is_some() {
        tracing::warn!(
            task_id = %task_id,
            run_id = %run_id,
            "skipping lease-expiry terminal publish because durable persistence failed"
        );
    }
}

#[allow(clippy::too_many_lines)]
async fn sweep_reconciliation_timeouts(state: &ControllerState, reconciliation_timeout: Duration) {
    let now = std::time::SystemTime::now();
    let stale_run_ids = {
        let runs = state.runs.read().await;
        runs.all_runs()
            .into_iter()
            .filter(|run| run.state == InternalRunState::Reconciling)
            .filter(|run| {
                run.recovery_started_at
                    .and_then(|started_at| now.duration_since(started_at).ok())
                    .is_some_and(|elapsed| elapsed >= reconciliation_timeout)
            })
            .map(|run| run.run_id)
            .collect::<Vec<_>>()
    };

    for run_id in stale_run_ids {
        let previous_run = state
            .runs
            .read()
            .await
            .get_run(&run_id)
            .cloned()
            .expect("reconciling run should exist");
        let task_id = previous_run
            .current_task
            .as_ref()
            .map(|task| task.task_id.clone());
        let previous_task = if let Some(task_id) = task_id.as_deref() {
            state.tasks.read().await.get(task_id).cloned()
        } else {
            None
        };
        let attempt = {
            let mut runs = state.runs.write().await;
            let Some(run) = runs.get_run(&run_id) else {
                continue;
            };
            if run.state != InternalRunState::Reconciling {
                continue;
            }
            let attempt = run.attempt;
            if runs
                .transition(&run_id, InternalRunState::RecoveryFailed)
                .is_err()
            {
                continue;
            }
            if let Some(run) = runs.get_run_mut(&run_id) {
                run.error_code = Some("RECOVERY_TIMEOUT".into());
                run.error_message =
                    Some("Run recovery reconciliation timed out after controller restart".into());
                run.error_retryable = Some(false);
                run.error_safe_to_retry = Some(false);
                run.error_commit_state = Some(String::new());
            }
            attempt
        };

        let mut durable = true;
        if let Some(task_id) = task_id.as_deref() {
            let timed_out = {
                let mut tasks = state.tasks.write().await;
                tasks.mark_timed_out(task_id)
            };
            if timed_out {
                if let Err(error) = state.persist_task(task_id).await {
                    durable = false;
                    tracing::error!(
                        task_id,
                        run_id,
                        ?error,
                        "failed to persist reconciliation-timeout task"
                    );
                }
            }
        }
        if let Err(error) = state.persist_run(&run_id).await {
            durable = false;
            tracing::error!(
                run_id,
                ?error,
                "failed to persist reconciliation-timeout run"
            );
        }

        if !durable {
            rollback_background_timeout(state, previous_run, previous_task).await;
        }

        if durable {
            state.watchers.write().await.publish_terminal(
                &run_id,
                RunEvent {
                    run_id: run_id.clone(),
                    event: Some(run_event::Event::Failed(RunFailed {
                        error: Some(TaskError {
                            code: "RECOVERY_TIMEOUT".into(),
                            message:
                                "Run recovery reconciliation timed out after controller restart"
                                    .into(),
                            retryable: false,
                            safe_to_retry: false,
                            commit_state: String::new(),
                        }),
                        attempt,
                    })),
                },
            );
        } else {
            tracing::warn!(run_id, "skipping reconciliation-timeout terminal publish because durable persistence failed");
        }
    }
}

async fn cleanup_expired_previews(state: &ControllerState) -> usize {
    let expired_run_ids = { state.previews.write().await.remove_expired() };
    {
        let mut retry_set = state.preview_delete_retries.write().await;
        retry_set.extend(expired_run_ids);
    }
    let pending = state
        .preview_delete_retries
        .read()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut removed = 0usize;
    for run_id in &pending {
        if let Err(error) = state.delete_preview(run_id).await {
            tracing::error!(run_id, ?error, "failed to delete expired durable preview");
        } else {
            state.preview_delete_retries.write().await.remove(run_id);
            removed += 1;
        }
    }
    removed
}

fn spawn_preview_cleanup_task(
    state: ControllerState,
    interval_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        loop {
            interval.tick().await;
            let removed = cleanup_expired_previews(&state).await;
            if removed > 0 {
                info!(removed, "Removed expired preview metadata entries");
            }
        }
    })
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
    let metadata_store = initialize_metadata_store(&config).await?;

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

    let state = ControllerState::from_metadata_store(&config.signing_key, metadata_store).await?;

    // Background task: reap dead agents
    let reap_state = state.clone();
    let reap_timeout = config.agent_reap_timeout;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.agent_reap_interval);
        loop {
            interval.tick().await;
            let dead = reap_state.registry.write().await.reap_dead(reap_timeout);
            for agent_id in &dead {
                if let Err(error) = reap_state.delete_agent(agent_id).await {
                    tracing::error!(agent_id, ?error, "failed to delete reaped agent");
                }
                info!(agent_id, "Reaped dead agent");
            }
        }
    });

    // Background task: expire leases
    let lease_state = state.clone();
    let reconciliation_timeout = config.reconciliation_timeout;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.lease_check_interval);
        loop {
            interval.tick().await;
            let expired = lease_state.tasks.write().await.expire_leases();
            for previous_task in expired {
                info!(task_id = %previous_task.task_id, run_id = %previous_task.run_id, "Task lease expired");
                handle_expired_lease(&lease_state, previous_task).await;
            }
            sweep_reconciliation_timeouts(&lease_state, reconciliation_timeout).await;
        }
    });

    let preview_cleanup_state = state.clone();
    std::mem::drop(spawn_preview_cleanup_task(
        preview_cleanup_state,
        config.preview_cleanup_interval,
    ));

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
    use crate::store::test_support::FailingMetadataStore;

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
    fn metadata_database_url_is_required() {
        let config = ControllerConfig {
            auth_tokens: vec!["secret".into()],
            signing_key: b"signing".to_vec(),
            ..Default::default()
        };
        let err = metadata_database_url(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("Controller metadata database URL is required"));
    }

    #[test]
    fn metadata_database_url_rejects_whitespace() {
        let config = ControllerConfig {
            auth_tokens: vec!["secret".into()],
            signing_key: b"signing".to_vec(),
            metadata_database_url: Some("   ".into()),
            ..Default::default()
        };
        let err = metadata_database_url(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("Controller metadata database URL is required"));
    }

    #[test]
    fn metadata_database_url_accepts_non_empty_value() {
        let config = ControllerConfig {
            auth_tokens: vec!["secret".into()],
            signing_key: b"signing".to_vec(),
            metadata_database_url: Some("postgresql://localhost/controller".into()),
            ..Default::default()
        };
        assert_eq!(
            metadata_database_url(&config).unwrap(),
            "postgresql://localhost/controller"
        );
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

        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::TimedOut);
        assert_eq!(
            record.error_message.as_deref(),
            Some(format!("Task {task_id} lease expired (agent unresponsive)").as_str())
        );
    }

    #[tokio::test]
    async fn handle_expired_lease_transitions_reconciling_run_to_recovery_failed() {
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::RecoveryFailed);
        assert!(record
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("reconciliation timed out")));
    }

    #[tokio::test]
    async fn handle_expired_lease_rolls_back_when_persist_fails() {
        let state = ControllerState::with_metadata_store(
            b"test-signing-key",
            FailingMetadataStore::new().fail_task_upsert_on(1),
        );
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

        let mut rx = state.watchers.write().await.subscribe(&run_id);
        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            recv.is_err(),
            "terminal event should not publish on persist failure"
        );
        assert_eq!(state.watchers.read().await.channel_count(), 1);
        let task = state.tasks.read().await.get(&task_id).unwrap().clone();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
        let run = state.runs.read().await.get_run(&run_id).unwrap().clone();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_fails_stale_reconciling_run() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().recovery_started_at = Some(
                std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(120))
                    .unwrap(),
            );
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(30)).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::RecoveryFailed);
        assert!(record
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("reconciliation timed out")));
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_rolls_back_when_persist_fails() {
        let state = ControllerState::with_metadata_store(
            b"test-signing-key",
            FailingMetadataStore::new().fail_run_upsert_on(1),
        );
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().recovery_started_at = Some(
                std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(120))
                    .unwrap(),
            );
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(30)).await;

        let run = state.runs.read().await.get_run(&run_id).unwrap().clone();
        assert_eq!(run.state, InternalRunState::Reconciling);
        let task = state.tasks.read().await.get(&task_id).unwrap().clone();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_leaves_fresh_reconciling_run_unchanged() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(300)).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Reconciling);
    }

    #[tokio::test]
    async fn preview_cleanup_task_removes_expired_entries() {
        let store = FailingMetadataStore::new();
        let state = ControllerState::with_metadata_store(b"test-signing-key", store.clone());
        let preview = crate::preview::PreviewEntry {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: std::time::Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        };
        {
            let mut previews = state.previews.write().await;
            previews.store(preview.clone());
        }
        state
            .persist_preview_record(&preview)
            .await
            .expect("preview persistence should succeed");

        let handle = spawn_preview_cleanup_task(state.clone(), Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(25)).await;
        handle.abort();

        let mut previews = state.previews.write().await;
        assert!(previews.get("run-1").is_none());
        assert_eq!(previews.cleanup_expired(), 0);
        assert!(store.persisted_preview("run-1").is_none());
        assert!(state.preview_delete_retries.read().await.is_empty());
    }

    #[tokio::test]
    async fn preview_cleanup_retries_failed_durable_deletes() {
        let store = FailingMetadataStore::new().fail_delete_preview_on(1);
        let state = ControllerState::with_metadata_store(b"test-signing-key", store.clone());
        let preview = crate::preview::PreviewEntry {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: std::time::Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        };
        {
            let mut previews = state.previews.write().await;
            previews.store(preview.clone());
        }
        state
            .persist_preview_record(&preview)
            .await
            .expect("preview persistence should succeed");

        let removed = cleanup_expired_previews(&state).await;
        assert_eq!(removed, 0);
        assert!(store.persisted_preview("run-1").is_some());
        assert!(state.preview_delete_retries.read().await.contains("run-1"));

        let removed = cleanup_expired_previews(&state).await;
        assert_eq!(removed, 1);
        assert!(store.persisted_preview("run-1").is_none());
        assert!(state.preview_delete_retries.read().await.is_empty());
    }
}
