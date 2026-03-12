//! `PipelineService` gRPC handler implementations.

use std::pin::Pin;

use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::proto::rapidbyte::v1::{
    pipeline_service_server::PipelineService, run_event, CancelRunRequest, CancelRunResponse,
    GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, PreviewAccess, PreviewState,
    RunCancelled, RunCompleted, RunEvent, RunFailed, RunState, RunSummary, SubmitPipelineRequest,
    SubmitPipelineResponse, TaskError, WatchRunRequest,
};
use crate::run_state::RunState as InternalRunState;
use crate::state::ControllerState;

/// Maps internal `RunState` to proto `RunState` enum value.
#[must_use]
pub fn to_proto_state(s: InternalRunState) -> i32 {
    match s {
        InternalRunState::Pending => RunState::Pending.into(),
        InternalRunState::Assigned => RunState::Assigned.into(),
        InternalRunState::Running | InternalRunState::Cancelling => RunState::Running.into(),
        InternalRunState::PreviewReady => RunState::PreviewReady.into(),
        InternalRunState::Completed => RunState::Completed.into(),
        InternalRunState::Failed | InternalRunState::TimedOut => RunState::Failed.into(),
        InternalRunState::Cancelled => RunState::Cancelled.into(),
    }
}

/// Maps proto `RunState` i32 back to internal `RunState` for filtering.
fn from_proto_state(v: i32) -> Option<InternalRunState> {
    match RunState::try_from(v) {
        Ok(RunState::Pending) => Some(InternalRunState::Pending),
        Ok(RunState::Assigned) => Some(InternalRunState::Assigned),
        Ok(RunState::Running) => Some(InternalRunState::Running),
        Ok(RunState::PreviewReady) => Some(InternalRunState::PreviewReady),
        Ok(RunState::Completed) => Some(InternalRunState::Completed),
        Ok(RunState::Failed) => Some(InternalRunState::Failed),
        Ok(RunState::Cancelled) => Some(InternalRunState::Cancelled),
        _ => None,
    }
}

/// Build a terminal `RunEvent` for a run that is already in a terminal state,
/// using real data from the run record instead of placeholder values.
fn terminal_event_for_run(record: &crate::run_state::RunRecord) -> RunEvent {
    let event = match record.state {
        InternalRunState::Completed => run_event::Event::Completed(RunCompleted {
            total_records: record.total_records,
            total_bytes: record.total_bytes,
            elapsed_seconds: record.elapsed_seconds,
            cursors_advanced: record.cursors_advanced,
        }),
        InternalRunState::Cancelled => run_event::Event::Cancelled(RunCancelled {}),
        // Failed and TimedOut both map to Failed
        _ => run_event::Event::Failed(RunFailed {
            error: record.error_message.as_ref().map(|msg| TaskError {
                code: String::new(),
                message: msg.clone(),
                retryable: false,
                safe_to_retry: false,
                commit_state: String::new(),
            }),
            attempt: record.attempt,
        }),
    };
    RunEvent {
        run_id: record.run_id.clone(),
        event: Some(event),
    }
}

pub struct PipelineServiceImpl {
    state: ControllerState,
}

impl PipelineServiceImpl {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }
}

type WatchRunStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<RunEvent, Status>> + Send>>;

#[tonic::async_trait]
impl PipelineService for PipelineServiceImpl {
    type WatchRunStream = WatchRunStream;

    async fn submit_pipeline(
        &self,
        request: Request<SubmitPipelineRequest>,
    ) -> Result<Response<SubmitPipelineResponse>, Status> {
        let req = request.into_inner();

        // Parse YAML to validate and extract pipeline name
        let yaml_str = std::str::from_utf8(&req.pipeline_yaml_utf8).map_err(|e| {
            Status::invalid_argument(format!("Pipeline YAML is not valid UTF-8: {e}"))
        })?;

        let config: serde_yaml::Value = serde_yaml::from_str(yaml_str)
            .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {e}")))?;

        let pipeline_name = config
            .get("pipeline")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        // Reject SQLite backend in distributed mode
        if let Some(state_cfg) = config.get("state") {
            if let Some(backend) = state_cfg.get("backend") {
                if backend.as_str() == Some("sqlite") {
                    return Err(Status::invalid_argument(
                        "Distributed mode requires a shared state backend (postgres). \
                         SQLite is a local file and would be unreachable after agent reassignment.",
                    ));
                }
            }
        }

        // Check idempotency
        let idempotency_key = if req.idempotency_key.is_empty() {
            None
        } else {
            Some(req.idempotency_key.clone())
        };

        let run_id = uuid::Uuid::new_v4().to_string();

        let (actual_run_id, is_new) = {
            let mut runs = self.state.runs.write().await;
            runs.create_run(run_id, pipeline_name, idempotency_key)
        };

        if is_new {
            let dry_run = req.execution.as_ref().is_some_and(|e| e.dry_run);
            let limit = req.execution.as_ref().and_then(|e| e.limit);
            let task_id = {
                let mut tasks = self.state.tasks.write().await;
                tasks.enqueue(
                    actual_run_id.clone(),
                    req.pipeline_yaml_utf8,
                    dry_run,
                    limit,
                    1,
                )
            };

            tracing::info!(run_id = %actual_run_id, task_id, "Pipeline submitted");
            self.state.task_notify.notify_waiters();
        }

        Ok(Response::new(SubmitPipelineResponse {
            run_id: actual_run_id,
        }))
    }

    async fn get_run(
        &self,
        request: Request<GetRunRequest>,
    ) -> Result<Response<GetRunResponse>, Status> {
        let run_id = request.into_inner().run_id;

        // Extract run data and drop the runs lock before touching previews
        let (run_id_out, state, pipeline_name, last_error) = {
            let runs = self.state.runs.read().await;
            let record = runs
                .get_run(&run_id)
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
            (
                record.run_id.clone(),
                to_proto_state(record.state),
                record.pipeline_name.clone(),
                record.error_message.as_ref().map(|msg| TaskError {
                    code: String::new(),
                    message: msg.clone(),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: String::new(),
                }),
            )
        };

        // Preview lookup with runs lock already released
        let preview = {
            let previews = self.state.previews.read().await;
            previews.get(&run_id).map(|p| PreviewAccess {
                state: PreviewState::Ready.into(),
                flight_endpoint: p.flight_endpoint.clone(),
                ticket: p.ticket.to_vec(),
                expires_at: None,
                streams: vec![],
            })
        };

        Ok(Response::new(GetRunResponse {
            run_id: run_id_out,
            state,
            pipeline_name,
            submitted_at: None,
            started_at: None,
            completed_at: None,
            current_task: None,
            preview,
            last_error,
        }))
    }

    async fn watch_run(
        &self,
        request: Request<WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let run_id = request.into_inner().run_id;

        // Verify the run exists and check if already terminal.
        // If terminal, build the event while we still hold the read lock so we
        // can use the real outcome data from the run record.
        let terminal_event = {
            let runs = self.state.runs.read().await;
            let record = runs
                .get_run(&run_id)
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
            if record.state.is_terminal() {
                Some(terminal_event_for_run(record))
            } else {
                None
            }
        };

        // If the run is already terminal, return a single-event stream immediately.
        // The broadcast channel may already be removed by publish_terminal, so
        // subscribing would create an empty channel that never yields events.
        if let Some(event) = terminal_event {
            let stream = tokio_stream::once(Ok(event));
            return Ok(Response::new(Box::pin(stream)));
        }

        let rx = {
            let mut watchers = self.state.watchers.write().await;
            watchers.subscribe(&run_id)
        };

        let stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(event) => Some(Ok(event)),
            Err(_) => None, // Lag or closed — skip
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn cancel_run(
        &self,
        request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        let run_id = request.into_inner().run_id;

        // Read the current state and transition under a short-lived lock.
        // IMPORTANT: drop runs lock before acquiring tasks lock to maintain
        // consistent lock ordering (tasks → runs) with poll_task/lease-expiry.
        let current_state = {
            let runs = self.state.runs.read().await;
            let record = runs
                .get_run(&run_id)
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
            record.state
        };

        match current_state {
            InternalRunState::Pending => {
                // Transition run to Cancelled, then cancel the task in the queue
                {
                    let mut runs = self.state.runs.write().await;
                    runs.transition(&run_id, InternalRunState::Cancelled)
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
                // runs lock dropped — now safe to acquire tasks lock
                {
                    let mut tasks = self.state.tasks.write().await;
                    if let Some(task) = tasks.find_by_run_id(&run_id) {
                        let task_id = task.task_id.clone();
                        let _ = tasks.cancel(&task_id);
                    }
                }

                Ok(Response::new(CancelRunResponse {
                    accepted: true,
                    message: "Queued run cancelled".into(),
                }))
            }
            InternalRunState::Assigned | InternalRunState::Running => {
                // Set to Cancelling — delivered via heartbeat
                let mut runs = self.state.runs.write().await;
                runs.transition(&run_id, InternalRunState::Cancelling)
                    .map_err(|e| Status::internal(e.to_string()))?;

                Ok(Response::new(CancelRunResponse {
                    accepted: true,
                    message: "Running — cancel will be delivered via heartbeat".into(),
                }))
            }
            _ if current_state.is_terminal() => Ok(Response::new(CancelRunResponse {
                accepted: false,
                message: format!("Run is already in terminal state: {current_state:?}"),
            })),
            _ => Ok(Response::new(CancelRunResponse {
                accepted: false,
                message: format!("Cannot cancel run in state: {current_state:?}"),
            })),
        }
    }

    async fn list_runs(
        &self,
        request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        let req = request.into_inner();
        let state_filter = req.filter_state.and_then(from_proto_state);

        let runs = self.state.runs.read().await;
        let mut records = runs.list_runs(state_filter);

        // Clamp limit to a positive value; default to 20 if unset or zero
        let limit = if req.limit > 0 {
            req.limit.unsigned_abs() as usize
        } else {
            20
        };
        records.truncate(limit);

        let summaries = records
            .into_iter()
            .map(|r| RunSummary {
                run_id: r.run_id.clone(),
                pipeline_name: r.pipeline_name.clone(),
                state: to_proto_state(r.state),
                submitted_at: None,
            })
            .collect();

        Ok(Response::new(ListRunsResponse { runs: summaries }))
    }
}

/// Helper to build a `ControllerState` for tests with a default signing key.
#[cfg(test)]
fn test_state() -> ControllerState {
    ControllerState::new(b"test-key-for-pipeline-service!!")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rapidbyte::v1::ExecutionOptions;

    #[tokio::test]
    async fn test_submit_pipeline_returns_run_id() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let yaml = b"pipeline: test\nsource:\n  use: src\n  config: {}\ndestination:\n  use: dst\n  config: {}\nstate:\n  backend: postgres\n";
        let resp = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();

        assert!(!resp.into_inner().run_id.is_empty());
    }

    #[tokio::test]
    async fn test_submit_pipeline_rejects_sqlite_backend() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let yaml = b"pipeline: test\nstate:\n  backend: sqlite\n";
        let result = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("postgres"));
    }

    #[tokio::test]
    async fn test_submit_pipeline_idempotency_key_dedup() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let resp1 = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "key-1".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp2 = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "key-1".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        assert_eq!(resp1, resp2);
    }

    #[tokio::test]
    async fn test_get_run_returns_status() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let yaml = b"pipeline: mytest\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .get_run(Request::new(GetRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.run_id, run_id);
        assert_eq!(resp.state, RunState::Pending as i32);
        assert_eq!(resp.pipeline_name, "mytest");
    }

    #[tokio::test]
    async fn test_cancel_pending_run_removes_from_queue() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);

        // Verify run is cancelled
        let get_resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_resp.state, RunState::Cancelled as i32);
    }

    #[tokio::test]
    async fn test_cancel_completed_run_returns_not_accepted() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        // Manually transition to Completed
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Completed)
                .unwrap();
        }

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.accepted);
    }
}
