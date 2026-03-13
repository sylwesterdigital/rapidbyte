//! Distributed pipeline execution via controller.

use std::future::Future;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;

use crate::commands::transport::{
    build_endpoint, connect_channel, request_with_bearer, TlsClientConfig,
};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    run_event, ExecutionOptions, GetRunRequest, PreviewAccess, RunCompleted, RunState,
    SubmitPipelineRequest, WatchRunRequest,
};

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;

    let channel = connect_channel(controller_url, tls)
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
        )?)
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
        )?)
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
                run_event::Event::Status(status) => {
                    if verbosity != Verbosity::Quiet {
                        let state = state_label(status.state);
                        if status.message.is_empty() {
                            eprintln!("State: {state}");
                        } else {
                            eprintln!("State: {state} - {}", status.message);
                        }
                    }
                }
                run_event::Event::Completed(c) => {
                    if verbosity != Verbosity::Quiet {
                        eprintln!(
                            "Completed: {} records, {} bytes in {:.1}s",
                            c.total_records, c.total_bytes, c.elapsed_seconds,
                        );
                    }
                    emit_bench_json_from_completed(&c);

                    if effective_dry_run {
                        handle_preview_result(
                            true,
                            fetch_and_display_preview(
                                &mut client,
                                &run_id,
                                verbosity,
                                auth_token,
                                tls,
                            )
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

/// Fetch preview data from the agent's Flight endpoint and display it.
async fn fetch_and_display_preview(
    client: &mut PipelineServiceClient<tonic::transport::Channel>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    use arrow::util::pretty::pretty_format_batches;

    // Get run details to find preview access
    let resp = client
        .get_run(request_with_bearer(
            GetRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        )?)
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

    let flight_endpoint =
        build_endpoint(&flight_url, tls).context("Failed to configure agent Flight endpoint")?;

    let previews = fetch_preview_batches(&preview, |stream, ticket| {
        let flight_endpoint = flight_endpoint.clone();
        let stream = stream.to_string();
        async move {
            let channel = flight_endpoint
                .connect()
                .await
                .context("Failed to connect to agent Flight endpoint")?;
            let mut flight_client = FlightServiceClient::new(channel);
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

            decode_flight_batches(&flight_data_vec)
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

fn decode_flight_batches(flight_data: &[arrow_flight::FlightData]) -> Result<Vec<RecordBatch>> {
    if flight_data.is_empty() {
        return Ok(Vec::new());
    }

    arrow_flight::utils::flight_data_to_batches(flight_data)
        .context("Failed to decode Flight preview data")
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

fn emit_bench_json_from_completed(completed: &RunCompleted) {
    if std::env::var_os("RAPIDBYTE_BENCH").is_none() {
        return;
    }

    println!("@@BENCH_JSON@@{}", bench_json_from_completed(completed));
}

fn bench_json_from_completed(completed: &RunCompleted) -> serde_json::Value {
    serde_json::json!({
        "records_read": completed.total_records,
        "records_written": completed.total_records,
        "bytes_read": completed.total_bytes,
        "bytes_written": completed.total_bytes,
        "duration_secs": completed.elapsed_seconds,
        "source_duration_secs": completed.elapsed_seconds,
        "dest_duration_secs": completed.elapsed_seconds,
        "dest_recv_count": 1,
        "parallelism": 1,
        "retry_count": 0,
        "stream_metrics": [{
            "stream_name": "distributed.aggregate",
            "partition_index": 0,
            "partition_count": 1,
            "records_read": completed.total_records,
            "records_written": completed.total_records,
            "bytes_read": completed.total_bytes,
            "bytes_written": completed.total_bytes,
            "source_duration_secs": completed.elapsed_seconds,
            "dest_duration_secs": completed.elapsed_seconds,
            "dest_recv_secs": completed.elapsed_seconds
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_controller::proto::rapidbyte::v1::{PreviewState, StreamPreview};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
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
        )
        .unwrap();

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
        )
        .unwrap();
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

    #[test]
    fn decode_flight_batches_accepts_empty_stream() {
        let batches = decode_flight_batches(&[]).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn distributed_run_builds_tls_channel_when_configured() {
        let dir = tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, b"ca-pem").unwrap();

        let endpoint = build_endpoint(
            "https://controller.example:9090",
            Some(&TlsClientConfig {
                ca_cert_path: Some(ca_path),
                domain_name: Some("controller.example".into()),
            }),
        )
        .unwrap();

        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn emit_bench_json_from_completed_includes_required_fields() {
        let completed = RunCompleted {
            total_records: 123,
            total_bytes: 456,
            elapsed_seconds: 7.5,
            cursors_advanced: 1,
        };

        let json = bench_json_from_completed(&completed);

        assert_eq!(json["records_read"], 123);
        assert_eq!(json["records_written"], 123);
        assert_eq!(json["bytes_written"], 456);
        assert_eq!(json["duration_secs"], 7.5);
        assert_eq!(json["stream_metrics"][0]["records_written"], 123);
        assert_eq!(json["stream_metrics"][0]["bytes_written"], 456);
    }
}
