//! gRPC server startup and wiring.

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Server;
use tracing::info;

use crate::agent_service::AgentServiceImpl;
use crate::middleware::BearerAuthInterceptor;
use crate::pipeline_service::PipelineServiceImpl;
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;
use crate::run_state::RunState as InternalRunState;
use crate::state::ControllerState;

/// Configuration for the controller server.
pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub signing_key: Vec<u8>,
    pub agent_reap_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub lease_check_interval: Duration,
    /// Bearer tokens for authentication. Empty = auth disabled.
    pub auth_tokens: Vec<String>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".parse().unwrap(),
            signing_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            agent_reap_interval: Duration::from_secs(15),
            agent_reap_timeout: Duration::from_secs(60),
            lease_check_interval: Duration::from_secs(10),
            auth_tokens: Vec::new(),
        }
    }
}

/// Start the controller gRPC server.
pub async fn run(config: ControllerConfig) -> anyhow::Result<()> {
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
                let _ = lease_state
                    .runs
                    .write()
                    .await
                    .transition(run_id, InternalRunState::TimedOut);
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

    Server::builder()
        .add_service(pipeline_svc)
        .add_service(agent_svc)
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
