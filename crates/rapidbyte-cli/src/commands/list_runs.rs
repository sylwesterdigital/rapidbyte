//! Distributed run listing command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{ListRunsRequest, RunState};

pub async fn execute(
    controller_url: Option<&str>,
    limit: i32,
    state: Option<&str>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "list-runs requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);

    let filter_state = state.map(parse_state_filter).transpose()?;
    let resp = client
        .list_runs(request_with_bearer(
            ListRunsRequest {
                limit,
                filter_state,
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    if verbosity != Verbosity::Quiet {
        for run in resp.runs {
            eprintln!(
                "{}  {}  {}",
                run.run_id,
                state_label(run.state),
                run.pipeline_name
            );
        }
    }

    Ok(())
}

fn parse_state_filter(value: &str) -> Result<i32> {
    let state = match value {
        "pending" => RunState::Pending,
        "assigned" => RunState::Assigned,
        "running" => RunState::Running,
        "reconciling" => RunState::Reconciling,
        "recovery_failed" => RunState::RecoveryFailed,
        "preview_ready" => RunState::PreviewReady,
        "completed" => RunState::Completed,
        "failed" => RunState::Failed,
        "cancelled" => RunState::Cancelled,
        _ => anyhow::bail!("invalid run state filter: {value}"),
    };
    Ok(state.into())
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
    async fn list_runs_requires_controller() {
        let err = execute(None, 20, None, Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("list-runs requires --controller"));
    }

    #[test]
    fn parse_state_filter_accepts_reconciling() {
        assert_eq!(
            parse_state_filter("reconciling").unwrap(),
            RunState::Reconciling as i32
        );
    }

    #[test]
    fn state_label_includes_reconciling() {
        assert_eq!(state_label(RunState::Reconciling as i32), "RECONCILING");
    }

    #[test]
    fn parse_state_filter_accepts_recovery_failed() {
        assert_eq!(
            parse_state_filter("recovery_failed").unwrap(),
            RunState::RecoveryFailed as i32
        );
    }

    #[test]
    fn state_label_includes_recovery_failed() {
        assert_eq!(
            state_label(RunState::RecoveryFailed as i32),
            "RECOVERY_FAILED"
        );
    }
}
