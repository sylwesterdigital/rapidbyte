//! Main agent loop: register, poll tasks, execute, report.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity,
    ServerTlsConfig as TonicServerTlsConfig,
};
use tracing::{error, info, warn};

use crate::auth::request_with_bearer;
use crate::executor::{self, TaskOutcomeKind};
use crate::flight::PreviewFlightService;
use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::{
    agent_directive, poll_task_response, ActiveLease, CompleteTaskRequest, HeartbeatRequest,
    PollTaskRequest, PreviewAccess, PreviewState, RegisterAgentRequest, TaskError, TaskMetrics,
    TaskOutcome,
};
use crate::spool::{PreviewKey, PreviewSpool};

/// Configuration for the agent worker.
#[derive(Clone)]
pub struct ClientTlsConfig {
    pub ca_cert_pem: Vec<u8>,
    pub domain_name: Option<String>,
}

#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

#[derive(Clone)]
pub struct AgentConfig {
    pub controller_url: String,
    pub flight_listen: String,
    pub flight_advertise: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
    pub signing_key: Vec<u8>,
    pub preview_ttl: Duration,
    pub auth_token: Option<String>,
    pub controller_tls: Option<ClientTlsConfig>,
    pub flight_tls: Option<ServerTlsConfig>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller_url: "http://[::]:9090".into(),
            flight_listen: "[::]:9091".into(),
            flight_advertise: "localhost:9091".into(),
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
            signing_key: b"rapidbyte-dev-signing-key-not-for-production".to_vec(),
            preview_ttl: Duration::from_secs(300),
            auth_token: None,
            controller_tls: None,
            flight_tls: None,
        }
    }
}

/// Shared state for tracking active leases across worker and heartbeat.
/// Maps `task_id` to (`lease_epoch`, `cancellation_token`).
type ActiveLeaseMap = Arc<RwLock<HashMap<String, (u64, CancellationToken)>>>;

const COMPLETE_TASK_RETRY_DELAY: Duration = Duration::from_secs(1);

enum WorkerPoll<T> {
    Task(T),
    Idle,
    Stop,
}

/// Run the agent worker loop.
///
/// # Errors
///
/// Returns an error if the controller connection fails, agent registration
/// is rejected, or the Flight server address cannot be parsed.
#[allow(clippy::too_many_lines)]
pub async fn run(config: AgentConfig) -> anyhow::Result<()> {
    // Bind Flight first so startup fails fast before the agent registers
    // itself as preview-capable.
    let flight_listener = tokio::net::TcpListener::bind(&config.flight_listen).await?;
    let flight_addr = flight_listener.local_addr()?;

    let channel = connect_channel(&config.controller_url, config.controller_tls.as_ref()).await?;
    let mut client = AgentServiceClient::new(channel.clone());

    // Set up preview spool and Flight server
    let spool = Arc::new(RwLock::new(PreviewSpool::new(config.preview_ttl)));
    let flight_svc = PreviewFlightService::new(spool.clone(), &config.signing_key);

    info!(addr = %flight_addr, "Starting Flight server");
    let mut flight_server = tonic::transport::Server::builder();
    if let Some(tls) = &config.flight_tls {
        flight_server = flight_server.tls_config(TonicServerTlsConfig::new().identity(
            Identity::from_pem(tls.cert_pem.clone(), tls.key_pem.clone()),
        ))?;
    }
    tokio::spawn(async move {
        if let Err(e) = flight_server
            .add_service(flight_svc.into_server())
            .serve_with_incoming(TcpListenerStream::new(flight_listener))
            .await
        {
            error!(error = %e, "Flight server failed");
        }
    });

    // Register with controller
    let resp = client
        .register_agent(
            request_with_bearer(
                RegisterAgentRequest {
                    max_tasks: config.max_tasks,
                    flight_advertise_endpoint: config.flight_advertise.clone(),
                    plugin_bundle_hash: String::new(),
                    available_plugins: vec![],
                    memory_bytes: 0,
                },
                config.auth_token.as_deref(),
            )
            .map_err(|_| anyhow::anyhow!("Invalid bearer token"))?,
        )
        .await?;
    let agent_id = resp.into_inner().agent_id;
    info!(agent_id, "Registered with controller");

    // Active lease tracking shared between worker and heartbeat
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
    let shutdown_token = CancellationToken::new();

    // Spawn heartbeat loop
    let hb_client = AgentServiceClient::new(channel.clone());
    let hb_agent_id = agent_id.clone();
    let hb_interval = config.heartbeat_interval;
    let hb_leases = active_leases.clone();
    let hb_spool = spool.clone();
    let hb_shutdown = shutdown_token.clone();
    let hb_auth_token = config.auth_token.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            hb_client,
            hb_agent_id,
            hb_interval,
            hb_leases,
            hb_spool,
            hb_shutdown,
            hb_auth_token,
        )
        .await;
    });

    let worker_pool = run_worker_pool(config.max_tasks, {
        let channel = channel.clone();
        let agent_id = agent_id.clone();
        let active_leases = active_leases.clone();
        let spool = spool.clone();
        let config = config.clone();
        let shutdown = shutdown_token.clone();
        move || {
            worker_runner_loop(
                channel.clone(),
                agent_id.clone(),
                active_leases.clone(),
                spool.clone(),
                config.clone(),
                shutdown.clone(),
            )
        }
    });
    tokio::pin!(worker_pool);

    // Main coordinator loop with graceful shutdown
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let pool_result = tokio::select! {
        _ = &mut shutdown => {
            info!("Shutdown signal received, stopping agent...");
            shutdown_token.cancel();
            worker_pool.await
        }
        result = &mut worker_pool => {
            shutdown_token.cancel();
            result
        }
    };

    let _ = heartbeat_handle.await;
    pool_result?;

    info!("Agent stopped");
    Ok(())
}

async fn connect_channel(
    controller_url: &str,
    tls: Option<&ClientTlsConfig>,
) -> anyhow::Result<Channel> {
    let mut endpoint = Endpoint::from_shared(controller_url.to_string())?;
    if controller_url.starts_with("https://") || tls.is_some() {
        let mut tls_config = TonicClientTlsConfig::new();
        if let Some(tls) = tls {
            if !tls.ca_cert_pem.is_empty() {
                tls_config =
                    tls_config.ca_certificate(Certificate::from_pem(tls.ca_cert_pem.clone()));
            }
            if let Some(domain_name) = &tls.domain_name {
                tls_config = tls_config.domain_name(domain_name.clone());
            }
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint.connect().await?)
}

async fn worker_runner_loop(
    channel: Channel,
    agent_id: String,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    config: AgentConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let client = AgentServiceClient::new(channel.clone());
    let poll_wait_seconds = config.poll_wait_seconds;
    let poll_auth_token = config.auth_token.clone();

    worker_loop(
        || {
            let shutdown = shutdown.clone();
            let mut poll_client = client.clone();
            let agent_id = agent_id.clone();
            let auth_token = poll_auth_token.clone();
            async move {
                if shutdown.is_cancelled() {
                    return Ok(WorkerPoll::Stop);
                }

                let resp = poll_client
                    .poll_task(
                        request_with_bearer(
                            PollTaskRequest {
                                agent_id,
                                wait_seconds: poll_wait_seconds,
                            },
                            auth_token.as_deref(),
                        )
                        .map_err(|_| anyhow::anyhow!("Invalid bearer token"))?,
                    )
                    .await?
                    .into_inner();

                Ok(match resp.result {
                    Some(poll_task_response::Result::Task(task)) => WorkerPoll::Task(task),
                    Some(poll_task_response::Result::NoTask(_)) | None => {
                        if shutdown.is_cancelled() {
                            WorkerPoll::Stop
                        } else {
                            WorkerPoll::Idle
                        }
                    }
                })
            }
        },
        |task| {
            process_task(
                channel.clone(),
                agent_id.clone(),
                task,
                active_leases.clone(),
                spool.clone(),
                config.clone(),
                shutdown.clone(),
            )
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn process_task(
    channel: Channel,
    agent_id: String,
    task: crate::proto::rapidbyte::v1::TaskAssignment,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    config: AgentConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    info!(
        task_id = task.task_id,
        run_id = task.run_id,
        attempt = task.attempt,
        lease_epoch = task.lease_epoch,
        "Received task"
    );

    let cancel_token = CancellationToken::new();
    active_leases.write().await.insert(
        task.task_id.clone(),
        (task.lease_epoch, cancel_token.clone()),
    );

    let exec_opts = task.execution.as_ref();
    let dry_run = exec_opts.is_some_and(|e| e.dry_run);
    let limit = exec_opts.and_then(|e| e.limit);

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_client = AgentServiceClient::new(channel.clone());
    let progress_handle = tokio::spawn(crate::progress::forward_progress(
        progress_rx,
        progress_client,
        agent_id.clone(),
        task.task_id.clone(),
        task.lease_epoch,
        config.auth_token.clone(),
    ));

    let result = executor::execute_task(
        &task.pipeline_yaml_utf8,
        dry_run,
        limit,
        Some(progress_tx),
        cancel_token,
    )
    .await;

    let _ = progress_handle.await;

    let (outcome, task_error) = match &result.outcome {
        TaskOutcomeKind::Completed => (TaskOutcome::Completed as i32, None),
        TaskOutcomeKind::Failed(info) => (
            TaskOutcome::Failed as i32,
            Some(TaskError {
                code: info.code.clone(),
                message: info.message.clone(),
                retryable: info.retryable,
                safe_to_retry: info.safe_to_retry,
                commit_state: info.commit_state.clone(),
            }),
        ),
        TaskOutcomeKind::Cancelled => (TaskOutcome::Cancelled as i32, None),
    };

    let preview = if let Some(dr) = result.dry_run_result {
        let stream_previews = dr
            .streams
            .iter()
            .map(|stream| crate::proto::rapidbyte::v1::StreamPreview {
                stream: stream.stream_name.clone(),
                rows: stream.total_rows,
                ticket: Vec::new(),
            })
            .collect();
        spool.write().await.store(
            PreviewKey {
                run_id: task.run_id.clone(),
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
            },
            dr,
        );
        Some(PreviewAccess {
            state: PreviewState::Ready.into(),
            flight_endpoint: config.flight_advertise.clone(),
            ticket: Vec::new(),
            expires_at: None,
            streams: stream_previews,
        })
    } else {
        None
    };

    let complete_request = CompleteTaskRequest {
        agent_id: agent_id.clone(),
        task_id: task.task_id.clone(),
        lease_epoch: task.lease_epoch,
        outcome,
        error: task_error,
        metrics: Some(TaskMetrics {
            records_processed: result.metrics.records_processed,
            bytes_processed: result.metrics.bytes_processed,
            elapsed_seconds: result.metrics.elapsed_seconds,
            cursors_advanced: result.metrics.cursors_advanced,
        }),
        preview,
        backend_run_id: 0,
    };

    let _ = report_completion_until_terminal(
        &active_leases,
        complete_request,
        COMPLETE_TASK_RETRY_DELAY,
        |req| {
            let mut completion_client = AgentServiceClient::new(channel.clone());
            let auth_token = config.auth_token.clone();
            async move {
                completion_client
                    .complete_task(
                        request_with_bearer(req, auth_token.as_deref())
                            .map_err(|_| tonic::Status::unauthenticated("Invalid bearer token"))?,
                    )
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
        shutdown,
    )
    .await;

    Ok(())
}

async fn run_worker_pool<F, Fut>(max_tasks: u32, mut make_worker: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut workers = JoinSet::new();
    for _ in 0..max_tasks.max(1) {
        workers.spawn(make_worker());
    }

    while let Some(result) = workers.join_next().await {
        result??;
    }

    Ok(())
}

async fn worker_loop<P, PFut, H, HFut, T>(mut poll: P, mut handle_task: H) -> anyhow::Result<()>
where
    P: FnMut() -> PFut,
    PFut: Future<Output = anyhow::Result<WorkerPoll<T>>>,
    H: FnMut(T) -> HFut,
    HFut: Future<Output = anyhow::Result<()>>,
{
    loop {
        match poll().await? {
            WorkerPoll::Task(task) => handle_task(task).await?,
            WorkerPoll::Idle => {}
            WorkerPoll::Stop => return Ok(()),
        }
    }
}

async fn heartbeat_loop(
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    interval: Duration,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    shutdown: CancellationToken,
    auth_token: Option<String>,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _tick = ticker.tick() => {}
        }

        let removed = spool.write().await.cleanup_expired();
        if removed > 0 {
            tracing::debug!(removed, "Evicted expired preview entries");
        }

        let leases: Vec<ActiveLease> = active_leases
            .read()
            .await
            .iter()
            .map(|(task_id, (epoch, _))| ActiveLease {
                task_id: task_id.clone(),
                lease_epoch: *epoch,
            })
            .collect();
        let active_count = u32::try_from(leases.len()).unwrap_or(u32::MAX);
        let Ok(request) = request_with_bearer(
            HeartbeatRequest {
                agent_id: agent_id.clone(),
                active_leases: leases,
                active_tasks: active_count,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            },
            auth_token.as_deref(),
        ) else {
            warn!("Failed to build authenticated heartbeat request: invalid bearer token");
            break;
        };
        let resp = client.heartbeat(request).await;
        match resp {
            Ok(resp) => {
                for directive in resp.into_inner().directives {
                    if let Some(agent_directive::Directive::CancelTask(cancel)) =
                        directive.directive
                    {
                        warn!(task_id = cancel.task_id, "Received cancel directive");
                        // Signal the executor to cancel via the token
                        let leases = active_leases.read().await;
                        if let Some((_, token)) = leases.get(&cancel.task_id) {
                            token.cancel();
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Heartbeat failed");
            }
        }
    }
}

async fn report_completion_until_terminal<F, Fut>(
    active_leases: &ActiveLeaseMap,
    request: CompleteTaskRequest,
    retry_delay: Duration,
    mut send_completion: F,
    shutdown: CancellationToken,
) -> bool
where
    F: FnMut(CompleteTaskRequest) -> Fut,
    Fut: Future<Output = Result<crate::proto::rapidbyte::v1::CompleteTaskResponse, tonic::Status>>,
{
    fn is_non_retryable_auth_error(code: tonic::Code) -> bool {
        matches!(
            code,
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
        )
    }

    loop {
        if shutdown.is_cancelled() {
            warn!(
                task_id = request.task_id,
                "Stopping completion retries because the agent is shutting down"
            );
            return false;
        }

        match send_completion(request.clone()).await {
            Ok(resp) => {
                active_leases.write().await.remove(&request.task_id);
                if resp.acknowledged {
                    info!(task_id = request.task_id, "Task completed");
                } else {
                    warn!(
                        task_id = request.task_id,
                        "Stale lease — completion rejected"
                    );
                }
                return resp.acknowledged;
            }
            Err(e) => {
                if is_non_retryable_auth_error(e.code()) {
                    warn!(
                        task_id = request.task_id,
                        error = %e,
                        "Stopping completion retries because authentication config is invalid"
                    );
                    return false;
                }
                warn!(
                    task_id = request.task_id,
                    error = %e,
                    "Failed to report completion, retrying while lease stays active"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {
                        warn!(
                            task_id = request.task_id,
                            "Stopping completion retries because the agent is shutting down"
                        );
                        return false;
                    }
                    () = tokio::time::sleep(retry_delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[tokio::test]
    async fn complete_task_transport_failure_keeps_lease_active() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), (42, CancellationToken::new()));

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let active_for_closure = active_leases.clone();
        let shutdown = CancellationToken::new();
        let acknowledged =
            report_completion_until_terminal(
                &active_leases,
                CompleteTaskRequest {
                    agent_id: "agent-1".into(),
                    task_id: "task-1".into(),
                    lease_epoch: 42,
                    outcome: TaskOutcome::Completed.into(),
                    error: None,
                    metrics: Some(TaskMetrics {
                        records_processed: 1,
                        bytes_processed: 1,
                        elapsed_seconds: 0.1,
                        cursors_advanced: 0,
                    }),
                    preview: None,
                    backend_run_id: 0,
                },
                Duration::from_millis(1),
                move |_req| {
                    let attempts = attempts_for_closure.clone();
                    let active_leases = active_for_closure.clone();
                    async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        assert!(active_leases.read().await.contains_key("task-1"));
                        if attempt == 0 {
                            Err(tonic::Status::unavailable("controller unavailable"))
                        } else {
                            Ok(crate::proto::rapidbyte::v1::CompleteTaskResponse {
                                acknowledged: true,
                            })
                        }
                    }
                },
                shutdown,
            )
            .await;

        assert!(acknowledged);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(!active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn completion_retries_stop_on_shutdown() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), (42, CancellationToken::new()));
        let shutdown = CancellationToken::new();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let completion = tokio::spawn({
            let active_leases = active_leases.clone();
            let shutdown = shutdown.clone();
            async move {
                report_completion_until_terminal(
                    &active_leases,
                    CompleteTaskRequest {
                        agent_id: "agent-1".into(),
                        task_id: "task-1".into(),
                        lease_epoch: 42,
                        outcome: TaskOutcome::Completed.into(),
                        error: None,
                        metrics: Some(TaskMetrics {
                            records_processed: 1,
                            bytes_processed: 1,
                            elapsed_seconds: 0.1,
                            cursors_advanced: 0,
                        }),
                        preview: None,
                        backend_run_id: 0,
                    },
                    Duration::from_millis(1),
                    move |_req| {
                        let attempts = attempts_for_closure.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(tonic::Status::unavailable("controller unavailable"))
                        }
                    },
                    shutdown,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.cancel();

        let acknowledged = tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("completion retry loop should stop on shutdown")
            .unwrap();

        assert!(!acknowledged);
        assert!(attempts.load(Ordering::SeqCst) > 0);
        assert!(active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn completion_retries_stop_on_auth_failures() {
        for code in [tonic::Code::Unauthenticated, tonic::Code::PermissionDenied] {
            let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
            active_leases
                .write()
                .await
                .insert("task-1".into(), (42, CancellationToken::new()));
            let shutdown = CancellationToken::new();

            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_for_closure = attempts.clone();
            let acknowledged = report_completion_until_terminal(
                &active_leases,
                CompleteTaskRequest {
                    agent_id: "agent-1".into(),
                    task_id: "task-1".into(),
                    lease_epoch: 42,
                    outcome: TaskOutcome::Completed.into(),
                    error: None,
                    metrics: Some(TaskMetrics {
                        records_processed: 1,
                        bytes_processed: 1,
                        elapsed_seconds: 0.1,
                        cursors_advanced: 0,
                    }),
                    preview: None,
                    backend_run_id: 0,
                },
                Duration::from_millis(1),
                move |_req| {
                    let attempts = attempts_for_closure.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(tonic::Status::new(code, "auth failure"))
                    }
                },
                shutdown,
            )
            .await;

            assert!(!acknowledged, "expected {code:?} to stop retries");
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(active_leases.read().await.contains_key("task-1"));
        }
    }

    #[tokio::test]
    async fn completion_invalid_argument_retries_until_shutdown() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), (42, CancellationToken::new()));
        let shutdown = CancellationToken::new();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let completion = tokio::spawn({
            let active_leases = active_leases.clone();
            let shutdown = shutdown.clone();
            async move {
                report_completion_until_terminal(
                    &active_leases,
                    CompleteTaskRequest {
                        agent_id: "agent-1".into(),
                        task_id: "task-1".into(),
                        lease_epoch: 42,
                        outcome: TaskOutcome::Completed.into(),
                        error: None,
                        metrics: Some(TaskMetrics {
                            records_processed: 1,
                            bytes_processed: 1,
                            elapsed_seconds: 0.1,
                            cursors_advanced: 0,
                        }),
                        preview: None,
                        backend_run_id: 0,
                    },
                    Duration::from_millis(1),
                    move |_req| {
                        let attempts = attempts_for_closure.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(tonic::Status::invalid_argument("non-auth validation error"))
                        }
                    },
                    shutdown,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.cancel();

        let acknowledged = tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("completion retry loop should stop on shutdown")
            .unwrap();

        assert!(!acknowledged);
        assert!(attempts.load(Ordering::SeqCst) > 1);
        assert!(active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn max_tasks_allows_parallel_execution() {
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let queue = Arc::new(RwLock::new(VecDeque::from([
            "task-1".to_string(),
            "task-2".to_string(),
        ])));

        let pool = tokio::spawn(run_worker_pool(2, {
            let queue = queue.clone();
            let started = started.clone();
            let release = release.clone();
            move || {
                let queue = queue.clone();
                let started = started.clone();
                let release = release.clone();
                async move {
                    worker_loop(
                        || {
                            let queue = queue.clone();
                            async move {
                                Ok(match queue.write().await.pop_front() {
                                    Some(task_id) => WorkerPoll::Task(task_id),
                                    None => WorkerPoll::Stop,
                                })
                            }
                        },
                        |_task_id| {
                            let started = started.clone();
                            let release = release.clone();
                            async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                release.notified().await;
                                Ok(())
                            }
                        },
                    )
                    .await?;
                    Ok(())
                }
            }
        }));

        for _ in 0..100 {
            if started.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(started.load(Ordering::SeqCst), 2);
        release.notify_waiters();
        pool.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn run_fails_when_flight_listener_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let err = run(AgentConfig {
            controller_url: "http://127.0.0.1:1".into(),
            flight_listen: addr.to_string(),
            flight_advertise: addr.to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();

        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("address already in use") || msg.contains("addrinuse"),
            "unexpected error: {err:#}"
        );
    }
}
