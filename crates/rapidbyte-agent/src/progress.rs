//! `ProgressEvent` to `ReportProgress` gRPC forwarding.

use rapidbyte_engine::progress::ProgressEvent;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::warn;

use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::{ProgressUpdate, ReportProgressRequest};

/// Forward engine progress events to the controller.
///
/// Runs until the receiver is closed (engine finished).
pub async fn forward_progress(
    mut rx: mpsc::UnboundedReceiver<ProgressEvent>,
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    task_id: String,
    lease_epoch: u64,
) {
    while let Some(event) = rx.recv().await {
        let progress = match &event {
            ProgressEvent::BatchEmitted { bytes } => Some(ProgressUpdate {
                stream: String::new(),
                phase: "running".into(),
                records: 0,
                bytes: *bytes,
            }),
            ProgressEvent::StreamCompleted { stream } => Some(ProgressUpdate {
                stream: stream.clone(),
                phase: "completed".into(),
                records: 0,
                bytes: 0,
            }),
            ProgressEvent::PhaseChange { phase } => Some(ProgressUpdate {
                stream: String::new(),
                phase: format!("{phase:?}").to_lowercase(),
                records: 0,
                bytes: 0,
            }),
            ProgressEvent::Retry { .. } => None,
        };

        if let Some(progress) = progress {
            let req = ReportProgressRequest {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                lease_epoch,
                progress: Some(progress),
            };
            if let Err(e) = client.report_progress(req).await {
                warn!(error = %e, "Failed to report progress to controller");
            }
        }
    }
}
