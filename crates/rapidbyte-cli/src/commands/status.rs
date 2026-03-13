//! Distributed run status command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{GetRunRequest, RunState};

pub async fn execute(
    controller_url: Option<&str>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "status requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);
    let resp = client
        .get_run(request_with_bearer(
            GetRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    if verbosity != Verbosity::Quiet {
        eprintln!("Run: {}", resp.run_id);
        eprintln!("Pipeline: {}", resp.pipeline_name);
        eprintln!("State: {}", state_label(resp.state));
        if let Some(task) = resp.current_task {
            eprintln!(
                "Current task: {} on {} (attempt {}, lease {})",
                task.task_id, task.agent_id, task.attempt, task.lease_epoch
            );
        }
        if let Some(error) = resp.last_error {
            eprintln!("Last error: {}", error.message);
        }
    }

    Ok(())
}

fn state_label(state: i32) -> &'static str {
    match RunState::try_from(state) {
        Ok(RunState::Pending) => "PENDING",
        Ok(RunState::Assigned) => "ASSIGNED",
        Ok(RunState::Running) => "RUNNING",
        Ok(RunState::Reconciling) => "RECONCILING",
        Ok(RunState::RecoveryFailed) => "RECOVERY_FAILED",
        Ok(RunState::PreviewReady) => "PREVIEW_READY",
        Ok(RunState::Completed) => "COMPLETED",
        Ok(RunState::Failed) => "FAILED",
        Ok(RunState::Cancelled) => "CANCELLED",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_requires_controller() {
        let err = execute(None, "run-1", Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("status requires --controller"));
    }

    #[test]
    fn state_label_includes_reconciling() {
        assert_eq!(state_label(RunState::Reconciling as i32), "RECONCILING");
    }

    #[test]
    fn state_label_includes_recovery_failed() {
        assert_eq!(
            state_label(RunState::RecoveryFailed as i32),
            "RECOVERY_FAILED"
        );
    }
}
