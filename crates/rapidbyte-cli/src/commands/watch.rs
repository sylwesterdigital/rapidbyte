//! Distributed run watch command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{run_event, RunState, WatchRunRequest};

pub async fn execute(
    controller_url: Option<&str>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "watch requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);
    let mut stream = client
        .watch_run(request_with_bearer(
            WatchRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        match event.event {
            Some(run_event::Event::Progress(progress)) => {
                if verbosity != Verbosity::Quiet {
                    eprintln!(
                        "[{}] {} - {} records, {} bytes",
                        progress.stream, progress.phase, progress.records, progress.bytes
                    );
                }
            }
            Some(run_event::Event::Status(status)) => {
                if verbosity != Verbosity::Quiet {
                    let state = state_label(status.state);
                    if status.message.is_empty() {
                        eprintln!("State: {state}");
                    } else {
                        eprintln!("State: {state} - {}", status.message);
                    }
                }
            }
            Some(run_event::Event::Completed(done)) => {
                if verbosity != Verbosity::Quiet {
                    eprintln!(
                        "Completed: {} records, {} bytes in {:.1}s",
                        done.total_records, done.total_bytes, done.elapsed_seconds
                    );
                }
                return Ok(());
            }
            Some(run_event::Event::Failed(failed)) => {
                let msg = failed.error.map(|e| e.message).unwrap_or_default();
                anyhow::bail!("Run failed (attempt {}): {msg}", failed.attempt);
            }
            Some(run_event::Event::Cancelled(_)) => {
                anyhow::bail!("Run was cancelled");
            }
            None => {}
        }
    }

    anyhow::bail!("WatchRun stream ended before a terminal event was received");
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
    async fn watch_requires_controller() {
        let err = execute(None, "run-1", Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("watch requires --controller"));
    }
}
