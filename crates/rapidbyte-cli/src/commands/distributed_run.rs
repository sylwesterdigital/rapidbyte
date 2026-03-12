//! Distributed pipeline execution via controller.

use std::path::Path;

use anyhow::{Context, Result};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use tonic::transport::Channel;

use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    run_event, ExecutionOptions, GetRunRequest, SubmitPipelineRequest, WatchRunRequest,
};

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
) -> Result<()> {
    let yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;

    let channel = Channel::from_shared(controller_url.to_string())?
        .connect()
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = PipelineServiceClient::new(channel);

    // Submit
    let resp = client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml_utf8: yaml,
            execution: Some(ExecutionOptions { dry_run, limit }),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
        })
        .await?;
    let run_id = resp.into_inner().run_id;

    if verbosity != Verbosity::Quiet {
        eprintln!("Submitted run: {run_id}");
    }

    // Watch
    let mut stream = client
        .watch_run(WatchRunRequest {
            run_id: run_id.clone(),
        })
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        if let Some(evt) = event.event {
            match evt {
                run_event::Event::Progress(p) => {
                    if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
                        eprintln!(
                            "  [{}] {} — {} records, {} bytes",
                            p.stream, p.phase, p.records, p.bytes
                        );
                    }
                }
                run_event::Event::Completed(c) => {
                    if verbosity != Verbosity::Quiet {
                        eprintln!(
                            "Completed: {} records, {} bytes in {:.1}s",
                            c.total_records, c.total_bytes, c.elapsed_seconds,
                        );
                    }

                    // If dry-run, fetch preview via Flight
                    if dry_run {
                        if let Err(e) =
                            fetch_and_display_preview(&mut client, &run_id, verbosity).await
                        {
                            if verbosity != Verbosity::Quiet {
                                eprintln!("Preview fetch failed: {e:#}");
                            }
                        }
                    }

                    return Ok(());
                }
                run_event::Event::Failed(f) => {
                    let msg = f.error.map(|e| e.message).unwrap_or_default();
                    anyhow::bail!("Run failed (attempt {}): {msg}", f.attempt);
                }
                run_event::Event::Cancelled(_) => {
                    anyhow::bail!("Run was cancelled");
                }
            }
        }
    }

    Ok(())
}

/// Fetch preview data from the agent's Flight endpoint and display it.
async fn fetch_and_display_preview(
    client: &mut PipelineServiceClient<Channel>,
    run_id: &str,
    verbosity: Verbosity,
) -> Result<()> {
    use arrow::util::pretty::pretty_format_batches;

    // Get run details to find preview access
    let resp = client
        .get_run(GetRunRequest {
            run_id: run_id.to_string(),
        })
        .await?
        .into_inner();

    let preview = match resp.preview {
        Some(p) if !p.flight_endpoint.is_empty() => p,
        _ => {
            if verbosity == Verbosity::Diagnostic {
                eprintln!("No preview available for this run");
            }
            return Ok(());
        }
    };

    // Connect to agent's Flight endpoint
    let flight_url = if preview.flight_endpoint.starts_with("http") {
        preview.flight_endpoint.clone()
    } else {
        format!("http://{}", preview.flight_endpoint)
    };

    let flight_channel = Channel::from_shared(flight_url)?
        .connect()
        .await
        .context("Failed to connect to agent Flight endpoint")?;

    let mut flight_client = FlightServiceClient::new(flight_channel);

    // DoGet with the ticket
    let ticket = Ticket {
        ticket: preview.ticket.into(),
    };
    let mut stream = flight_client.do_get(ticket).await?.into_inner();

    // Collect all FlightData messages
    let mut flight_data_vec = Vec::new();
    while let Some(flight_data) = stream.message().await? {
        flight_data_vec.push(flight_data);
    }

    // Decode all FlightData into RecordBatches
    let batches = arrow_flight::utils::flight_data_to_batches(&flight_data_vec)
        .context("Failed to decode Flight preview data")?;

    if batches.is_empty() {
        eprintln!("(no preview data)");
        return Ok(());
    }

    // Display using pretty_format_batches
    match pretty_format_batches(&batches) {
        Ok(table) => eprintln!("{table}"),
        Err(e) => eprintln!("(display error: {e})"),
    }

    Ok(())
}
