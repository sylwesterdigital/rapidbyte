//! Main agent loop: register, poll tasks, execute, report.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::executor::{self, TaskOutcomeKind};
use crate::flight::PreviewFlightService;
use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::*;
use crate::spool::PreviewSpool;
use crate::ticket::TicketVerifier;

/// Configuration for the agent worker.
pub struct AgentConfig {
    pub controller_url: String,
    pub flight_listen: String,
    pub flight_advertise: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
    pub signing_key: Vec<u8>,
    pub preview_ttl: Duration,
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
            signing_key: Vec::new(),
            preview_ttl: Duration::from_secs(300),
        }
    }
}

/// Shared state for tracking active leases across worker and heartbeat.
/// Maps task_id -> (lease_epoch, cancellation_token).
type ActiveLeaseMap = Arc<RwLock<HashMap<String, (u64, CancellationToken)>>>;

/// Run the agent worker loop.
pub async fn run(config: AgentConfig) -> anyhow::Result<()> {
    let channel = Channel::from_shared(config.controller_url.clone())?
        .connect()
        .await?;
    let mut client = AgentServiceClient::new(channel.clone());

    // Set up preview spool and Flight server
    let spool = Arc::new(RwLock::new(PreviewSpool::new(config.preview_ttl)));
    let verifier = Arc::new(TicketVerifier::new(&config.signing_key));
    let flight_svc = PreviewFlightService::new(spool.clone(), verifier);

    let flight_addr: std::net::SocketAddr = config.flight_listen.parse()?;
    info!(addr = %flight_addr, "Starting Flight server");
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(flight_svc.into_server())
            .serve(flight_addr)
            .await
        {
            error!(error = %e, "Flight server failed");
        }
    });

    // Register with controller
    let resp = client
        .register_agent(RegisterAgentRequest {
            max_tasks: config.max_tasks,
            flight_advertise_endpoint: config.flight_advertise.clone(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        })
        .await?;
    let agent_id = resp.into_inner().agent_id;
    info!(agent_id, "Registered with controller");

    // Active lease tracking shared between worker and heartbeat
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

    // Spawn heartbeat loop
    let hb_client = AgentServiceClient::new(channel.clone());
    let hb_agent_id = agent_id.clone();
    let hb_interval = config.heartbeat_interval;
    let hb_leases = active_leases.clone();
    tokio::spawn(async move {
        heartbeat_loop(hb_client, hb_agent_id, hb_interval, hb_leases).await;
    });

    // Main task poll loop with graceful shutdown
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        let poll_fut = client.poll_task(PollTaskRequest {
            agent_id: agent_id.clone(),
            wait_seconds: config.poll_wait_seconds,
        });

        let resp = tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping agent...");
                break;
            }
            resp = poll_fut => resp?,
        };

        let task = match resp.into_inner().result {
            Some(poll_task_response::Result::Task(t)) => t,
            Some(poll_task_response::Result::NoTask(_)) | None => continue,
        };

        info!(
            task_id = task.task_id,
            run_id = task.run_id,
            attempt = task.attempt,
            lease_epoch = task.lease_epoch,
            "Received task"
        );

        // Track active lease with cancellation token
        let cancel_token = CancellationToken::new();
        active_leases.write().await.insert(
            task.task_id.clone(),
            (task.lease_epoch, cancel_token.clone()),
        );

        let exec_opts = task.execution.as_ref();
        let dry_run = exec_opts.map_or(false, |e| e.dry_run);
        let limit = exec_opts.and_then(|e| e.limit);

        // Set up progress forwarding
        let (progress_tx, progress_rx) = mpsc::unbounded_channel();
        let progress_client = AgentServiceClient::new(channel.clone());
        let progress_handle = tokio::spawn(crate::progress::forward_progress(
            progress_rx,
            progress_client,
            agent_id.clone(),
            task.task_id.clone(),
            task.lease_epoch,
        ));

        // Execute (with cancellation token)
        let result = executor::execute_task(
            &task.pipeline_yaml_utf8,
            dry_run,
            limit,
            Some(progress_tx),
            cancel_token,
        )
        .await;

        // Wait for progress forwarding to finish
        let _ = progress_handle.await;

        // Build completion request
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

        // Store dry-run preview in spool and build PreviewAccess
        let preview = if let Some(dr) = result.dry_run_result {
            spool.write().await.store(task.task_id.clone(), dr);
            Some(PreviewAccess {
                state: PreviewState::Ready.into(),
                flight_endpoint: config.flight_advertise.clone(),
                ticket: Vec::new(), // Controller signs the ticket
                expires_at: None,
                streams: vec![],
            })
        } else {
            None
        };

        let complete_resp = client
            .complete_task(CompleteTaskRequest {
                agent_id: agent_id.clone(),
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome,
                error: task_error,
                metrics: Some(crate::proto::rapidbyte::v1::TaskMetrics {
                    records_processed: result.metrics.records_processed,
                    bytes_processed: result.metrics.bytes_processed,
                    elapsed_seconds: result.metrics.elapsed_seconds,
                    cursors_advanced: result.metrics.cursors_advanced,
                }),
                preview,
                backend_run_id: 0,
            })
            .await;

        // Remove active lease tracking
        active_leases.write().await.remove(&task.task_id);

        match complete_resp {
            Ok(resp) => {
                if resp.into_inner().acknowledged {
                    info!(task_id = task.task_id, "Task completed");
                } else {
                    warn!(task_id = task.task_id, "Stale lease — completion rejected");
                }
            }
            Err(e) => {
                error!(task_id = task.task_id, error = %e, "Failed to report completion");
            }
        }
    }

    info!("Agent stopped");
    Ok(())
}

async fn heartbeat_loop(
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    interval: Duration,
    active_leases: ActiveLeaseMap,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let leases: Vec<ActiveLease> = active_leases
            .read()
            .await
            .iter()
            .map(|(task_id, (epoch, _))| ActiveLease {
                task_id: task_id.clone(),
                lease_epoch: *epoch,
            })
            .collect();
        let active_count = leases.len() as u32;
        let resp = client
            .heartbeat(HeartbeatRequest {
                agent_id: agent_id.clone(),
                active_leases: leases,
                active_tasks: active_count,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            })
            .await;
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
