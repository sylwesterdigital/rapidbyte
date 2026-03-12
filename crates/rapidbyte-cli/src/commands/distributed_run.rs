//! Distributed pipeline execution via controller.

use std::future::Future;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use tonic::transport::Channel;

use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    run_event, ExecutionOptions, GetRunRequest, PreviewAccess, SubmitPipelineRequest,
    WatchRunRequest,
};

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
) -> Result<()> {
    let yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;

    let channel = Channel::from_shared(controller_url.to_string())?
        .connect()
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = PipelineServiceClient::new(channel);

    // --limit implies dry-run (preview-only), matching local execution semantics
    let effective_dry_run = dry_run || limit.is_some();

    // Submit
    let resp = client
        .submit_pipeline(request_with_bearer(
            SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml,
                execution: Some(ExecutionOptions {
                    dry_run: effective_dry_run,
                    limit,
                }),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
            },
            auth_token,
        ))
        .await?;
    let run_id = resp.into_inner().run_id;

    if verbosity != Verbosity::Quiet {
        eprintln!("Submitted run: {run_id}");
    }

    // Watch
    let mut stream = client
        .watch_run(request_with_bearer(
            WatchRunRequest {
                run_id: run_id.clone(),
            },
            auth_token,
        ))
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

                    if effective_dry_run {
                        handle_preview_result(
                            true,
                            fetch_and_display_preview(&mut client, &run_id, verbosity, auth_token)
                                .await,
                        )?;
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

    ensure_terminal_event_received(false)
}

/// Fetch preview data from the agent's Flight endpoint and display it.
async fn fetch_and_display_preview(
    client: &mut PipelineServiceClient<Channel>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
) -> Result<()> {
    use arrow::util::pretty::pretty_format_batches;

    // Get run details to find preview access
    let resp = client
        .get_run(request_with_bearer(
            GetRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        ))
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

    let previews = fetch_preview_batches(&preview, |stream, ticket| {
        let flight_channel = flight_channel.clone();
        let stream = stream.to_string();
        async move {
            let mut flight_client = FlightServiceClient::new(flight_channel);
            let mut stream_resp = flight_client
                .do_get(Ticket {
                    ticket: ticket.into(),
                })
                .await
                .with_context(|| format!("Flight DoGet failed for stream {stream}"))?
                .into_inner();

            let mut flight_data_vec = Vec::new();
            while let Some(flight_data) = stream_resp.message().await? {
                flight_data_vec.push(flight_data);
            }

            arrow_flight::utils::flight_data_to_batches(&flight_data_vec)
                .context("Failed to decode Flight preview data")
        }
    })
    .await?;

    if previews.is_empty() {
        eprintln!("(no preview data)");
        return Ok(());
    }

    let multiple_previews = previews.len() > 1;
    for (stream_name, batches) in previews {
        if multiple_previews && stream_name != "preview" && verbosity != Verbosity::Quiet {
            eprintln!("Stream: {stream_name}");
        }

        if batches.is_empty() {
            eprintln!("(no preview data)");
            continue;
        }

        match pretty_format_batches(&batches) {
            Ok(table) => eprintln!("{table}"),
            Err(e) => eprintln!("(display error: {e})"),
        }
    }

    Ok(())
}

async fn fetch_preview_batches<F, Fut>(
    preview: &PreviewAccess,
    mut fetcher: F,
) -> Result<Vec<(String, Vec<RecordBatch>)>>
where
    F: FnMut(&str, Vec<u8>) -> Fut,
    Fut: Future<Output = Result<Vec<RecordBatch>>>,
{
    let requests = if !preview.streams.is_empty() {
        preview
            .streams
            .iter()
            .map(|stream| (stream.stream.clone(), stream.ticket.clone()))
            .collect::<Vec<_>>()
    } else if !preview.ticket.is_empty() {
        vec![("preview".to_string(), preview.ticket.clone())]
    } else {
        Vec::new()
    };

    let mut results = Vec::with_capacity(requests.len());
    for (stream_name, ticket) in requests {
        let batches = fetcher(&stream_name, ticket).await?;
        results.push((stream_name, batches));
    }

    Ok(results)
}

fn handle_preview_result(effective_dry_run: bool, preview_result: Result<()>) -> Result<()> {
    if effective_dry_run {
        preview_result?;
    } else if let Err(e) = preview_result {
        tracing::warn!(error = %e, "Preview fetch failed");
    }
    Ok(())
}

fn ensure_terminal_event_received(seen_terminal: bool) -> Result<()> {
    if seen_terminal {
        Ok(())
    } else {
        anyhow::bail!("WatchRun stream ended before a terminal event was received");
    }
}

fn request_with_bearer<T>(message: T, auth_token: Option<&str>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = auth_token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_controller::proto::rapidbyte::v1::{PreviewState, StreamPreview};
    use std::sync::{Arc, Mutex};
    use tonic::metadata::MetadataValue;

    #[tokio::test]
    async fn fetch_preview_batches_prefers_stream_tickets() {
        let preview = PreviewAccess {
            state: PreviewState::Ready.into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: vec![9],
            expires_at: None,
            streams: vec![
                StreamPreview {
                    stream: "users".into(),
                    rows: 3,
                    ticket: vec![1],
                },
                StreamPreview {
                    stream: "orders".into(),
                    rows: 2,
                    ticket: vec![2],
                },
            ],
        };
        let seen = Arc::new(Mutex::new(Vec::new()));

        let results = fetch_preview_batches(&preview, {
            let seen = seen.clone();
            move |stream_name, ticket| {
                let seen = seen.clone();
                let stream_name = stream_name.to_string();
                async move {
                    seen.lock().unwrap().push((stream_name, ticket));
                    Ok(Vec::new())
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("users".to_string(), vec![1]),
                ("orders".to_string(), vec![2]),
            ]
        );
    }

    #[tokio::test]
    async fn fetch_preview_batches_falls_back_to_legacy_ticket() {
        let preview = PreviewAccess {
            state: PreviewState::Ready.into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: vec![7],
            expires_at: None,
            streams: vec![],
        };
        let seen = Arc::new(Mutex::new(Vec::new()));

        fetch_preview_batches(&preview, {
            let seen = seen.clone();
            move |stream_name, ticket| {
                let seen = seen.clone();
                let stream_name = stream_name.to_string();
                async move {
                    seen.lock().unwrap().push((stream_name, ticket));
                    Ok(Vec::new())
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            *seen.lock().unwrap(),
            vec![("preview".to_string(), vec![7])]
        );
    }

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer(
            WatchRunRequest {
                run_id: "run-1".into(),
            },
            Some("secret"),
        );

        assert_eq!(
            request.metadata().get("authorization"),
            Some(&MetadataValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn request_with_bearer_is_noop_without_token() {
        let request = request_with_bearer(
            WatchRunRequest {
                run_id: "run-1".into(),
            },
            None,
        );
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn watch_run_eof_before_terminal_is_error() {
        let err = ensure_terminal_event_received(false).unwrap_err();
        assert!(err
            .to_string()
            .contains("ended before a terminal event was received"));
    }

    #[test]
    fn dry_run_preview_failure_is_error() {
        let err = handle_preview_result(
            true,
            Err(anyhow::anyhow!("Flight DoGet failed for stream users")),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Flight DoGet failed"));
    }
}
