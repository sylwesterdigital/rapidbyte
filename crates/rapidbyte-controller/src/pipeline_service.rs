//! `PipelineService` gRPC handler implementations.

use std::pin::Pin;
use std::time::UNIX_EPOCH;

use prost_types::Timestamp;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::proto::rapidbyte::v1::{
    pipeline_service_server::PipelineService, run_event, CancelRunRequest, CancelRunResponse,
    GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, PreviewAccess, PreviewState,
    RunCancelled, RunCompleted, RunEvent, RunFailed, RunState, RunSummary, StreamPreview,
    SubmitPipelineRequest, SubmitPipelineResponse, TaskError, TaskRef, WatchRunRequest,
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

/// Maps proto `RunState` i32 back to internal `RunState`(s) for filtering.
/// `RUNNING` maps to both `Running` and `Cancelling` (externally both appear as `RUNNING`).
/// `FAILED` maps to both `Failed` and `TimedOut` (externally both appear as `FAILED`).
fn from_proto_states(v: i32) -> Option<Vec<InternalRunState>> {
    match RunState::try_from(v) {
        Ok(RunState::Pending) => Some(vec![InternalRunState::Pending]),
        Ok(RunState::Assigned) => Some(vec![InternalRunState::Assigned]),
        Ok(RunState::Running) => Some(vec![
            InternalRunState::Running,
            InternalRunState::Cancelling,
        ]),
        Ok(RunState::PreviewReady) => Some(vec![InternalRunState::PreviewReady]),
        Ok(RunState::Completed) => Some(vec![InternalRunState::Completed]),
        Ok(RunState::Failed) => Some(vec![InternalRunState::Failed, InternalRunState::TimedOut]),
        Ok(RunState::Cancelled) => Some(vec![InternalRunState::Cancelled]),
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

fn to_timestamp(time: std::time::SystemTime) -> Timestamp {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp {
        seconds: duration.as_secs().cast_signed(),
        nanos: duration.subsec_nanos().cast_signed(),
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

    async fn terminal_event_for_existing_run(
        &self,
        run_id: &str,
    ) -> Result<Option<RunEvent>, Status> {
        let runs = self.state.runs.read().await;
        let record = runs
            .get_run(run_id)
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
        Ok(record
            .state
            .is_terminal()
            .then(|| terminal_event_for_run(record)))
    }

    async fn watch_run_after_subscribe(
        &self,
        run_id: &str,
    ) -> Result<Response<WatchRunStream>, Status> {
        let rx = {
            let mut watchers = self.state.watchers.write().await;
            watchers.subscribe(run_id)
        };

        let terminal_event = match self.terminal_event_for_existing_run(run_id).await {
            Ok(event) => event,
            Err(err) => {
                self.state.watchers.write().await.remove(run_id);
                return Err(err);
            }
        };

        if let Some(event) = terminal_event {
            self.state.watchers.write().await.remove(run_id);
            let stream: WatchRunStream = Box::pin(tokio_stream::once(Ok(event)));
            return Ok(Response::new(stream));
        }

        let stream: WatchRunStream =
            Box::pin(BroadcastStream::new(rx).filter_map(|result| match result {
                Ok(event) => Some(Ok(event)),
                Err(_) => None, // Lag or closed — skip
            }));

        Ok(Response::new(stream))
    }

    async fn cancel_run_for_state(
        &self,
        run_id: &str,
        snapshot_state: InternalRunState,
    ) -> Result<CancelRunResponse, Status> {
        match snapshot_state {
            InternalRunState::Pending => self.cancel_queued_run(run_id, snapshot_state).await,
            InternalRunState::Assigned => self.cancel_assigned_run(run_id, snapshot_state).await,
            InternalRunState::Running => self.cancel_running_run(run_id, snapshot_state).await,
            InternalRunState::Cancelling => Ok(CancelRunResponse {
                accepted: true,
                message: "Run is already being cancelled".into(),
            }),
            _ if snapshot_state.is_terminal() => Ok(CancelRunResponse {
                accepted: false,
                message: format!("Run is already in terminal state: {snapshot_state:?}"),
            }),
            _ => Ok(CancelRunResponse {
                accepted: false,
                message: format!("Cannot cancel run in state: {snapshot_state:?}"),
            }),
        }
    }

    async fn cancel_queued_run(
        &self,
        run_id: &str,
        snapshot_state: InternalRunState,
    ) -> Result<CancelRunResponse, Status> {
        if let Some(response) = self
            .transition_or_retry(run_id, snapshot_state, InternalRunState::Cancelled)
            .await?
        {
            return Ok(response);
        }
        self.cancel_latest_task(run_id).await;
        self.publish_cancelled(run_id).await;
        Ok(CancelRunResponse {
            accepted: true,
            message: "Queued run cancelled".into(),
        })
    }

    async fn cancel_assigned_run(
        &self,
        run_id: &str,
        snapshot_state: InternalRunState,
    ) -> Result<CancelRunResponse, Status> {
        if let Some(response) = self
            .transition_or_retry(run_id, snapshot_state, InternalRunState::Cancelling)
            .await?
        {
            return Ok(response);
        }
        Ok(CancelRunResponse {
            accepted: true,
            message: "Assigned — cancel will be delivered via heartbeat".into(),
        })
    }

    async fn cancel_running_run(
        &self,
        run_id: &str,
        snapshot_state: InternalRunState,
    ) -> Result<CancelRunResponse, Status> {
        if let Some(response) = self
            .transition_or_retry(run_id, snapshot_state, InternalRunState::Cancelling)
            .await?
        {
            return Ok(response);
        }
        Ok(CancelRunResponse {
            accepted: true,
            message: "Running — cancel will be delivered via heartbeat".into(),
        })
    }

    async fn transition_or_retry(
        &self,
        run_id: &str,
        snapshot_state: InternalRunState,
        target_state: InternalRunState,
    ) -> Result<Option<CancelRunResponse>, Status> {
        let actual_state = {
            let mut runs = self.state.runs.write().await;
            match runs.transition(run_id, target_state) {
                Ok(()) => return Ok(None),
                Err(err) => {
                    let actual_state = runs
                        .get_run(run_id)
                        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
                        .state;
                    if actual_state == snapshot_state {
                        return Err(Status::internal(err.to_string()));
                    }
                    Some(actual_state)
                }
            }
        };

        let Some(actual_state) = actual_state else {
            unreachable!("transition_or_retry only falls through with a refreshed run state");
        };
        Ok(Some(
            Box::pin(self.cancel_run_for_state(run_id, actual_state)).await?,
        ))
    }

    async fn cancel_latest_task(&self, run_id: &str) {
        let mut tasks = self.state.tasks.write().await;
        if let Some(task) = tasks.find_by_run_id(run_id) {
            let task_id = task.task_id.clone();
            let _ = tasks.cancel(&task_id);
        }
    }

    async fn publish_cancelled(&self, run_id: &str) {
        self.state.watchers.write().await.publish_terminal(
            run_id,
            RunEvent {
                run_id: run_id.to_string(),
                event: Some(run_event::Event::Cancelled(RunCancelled {})),
            },
        );
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

        // Reject SQLite backend (explicit or implicit) in distributed mode.
        // Pipelines without a state section or without state.backend default to
        // SQLite in the engine, which is a local file unreachable after reassignment.
        let backend = config
            .get("state")
            .and_then(|s| s.get("backend"))
            .and_then(|b| b.as_str());
        if backend != Some("postgres") {
            return Err(Status::invalid_argument(
                "Distributed mode requires state.backend: postgres. \
                 SQLite (the default) is a local file and would be unreachable after agent reassignment.",
            ));
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
        let (
            run_id_out,
            state,
            pipeline_name,
            submitted_at,
            started_at,
            completed_at,
            current_task,
            last_error,
        ) = {
            let runs = self.state.runs.read().await;
            let record = runs
                .get_run(&run_id)
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
            (
                record.run_id.clone(),
                to_proto_state(record.state),
                record.pipeline_name.clone(),
                Some(to_timestamp(record.created_at)),
                record.started_at.map(to_timestamp),
                record.completed_at.map(to_timestamp),
                record.current_task.as_ref().map(|task| TaskRef {
                    task_id: task.task_id.clone(),
                    agent_id: task.agent_id.clone(),
                    attempt: task.attempt,
                    lease_epoch: task.lease_epoch,
                    assigned_at: Some(to_timestamp(task.assigned_at)),
                }),
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
                streams: p
                    .streams
                    .iter()
                    .map(|stream| StreamPreview {
                        stream: stream.stream.clone(),
                        rows: stream.rows,
                        ticket: stream.ticket.to_vec(),
                    })
                    .collect(),
            })
        };

        Ok(Response::new(GetRunResponse {
            run_id: run_id_out,
            state,
            pipeline_name,
            submitted_at,
            started_at,
            completed_at,
            current_task,
            preview,
            last_error,
        }))
    }

    async fn watch_run(
        &self,
        request: Request<WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let run_id = request.into_inner().run_id;
        self.watch_run_after_subscribe(&run_id).await
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

        Ok(Response::new(
            self.cancel_run_for_state(&run_id, current_state).await?,
        ))
    }

    async fn list_runs(
        &self,
        request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        let req = request.into_inner();
        let state_filter = req.filter_state.and_then(from_proto_states);

        let runs = self.state.runs.read().await;
        let mut records = runs.list_runs(state_filter.as_deref());
        records.sort_by(|a, b| b.created_at.cmp(&a.created_at));

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
                submitted_at: Some(to_timestamp(r.created_at)),
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
    use crate::proto::rapidbyte::v1::{
        agent_service_server::AgentService as _, ExecutionOptions, PollTaskRequest,
        RegisterAgentRequest,
    };
    use crate::run_state::RunState as InternalRunState;
    use tokio_stream::StreamExt;

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
    async fn get_run_returns_real_metadata() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

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

        let pending = svc
            .get_run(Request::new(GetRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(pending.submitted_at.is_some());
        assert!(pending.started_at.is_none());
        assert!(pending.completed_at.is_none());
        assert!(pending.current_task.is_none());

        let agent_svc = crate::agent_service::AgentServiceImpl::new(state.clone());
        let agent_id = agent_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let task = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            task.result,
            Some(crate::proto::rapidbyte::v1::poll_task_response::Result::Task(_))
        ));

        let assigned = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();
        let current_task = assigned.current_task.expect("assigned task metadata");
        assert_eq!(current_task.agent_id, agent_id);
        assert_eq!(current_task.attempt, 1);
        assert!(current_task.assigned_at.is_some());
    }

    #[tokio::test]
    async fn list_runs_returns_most_recent_first() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let first = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: b"pipeline: first\nstate:\n  backend: postgres\n".to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;
        std::thread::sleep(std::time::Duration::from_millis(10));
        let second = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: b"pipeline: second\nstate:\n  backend: postgres\n".to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 2,
                filter_state: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 2);
        assert_eq!(resp.runs[0].run_id, second);
        assert_eq!(resp.runs[1].run_id, first);
        assert!(resp.runs[0].submitted_at.is_some());
        assert!(resp.runs[1].submitted_at.is_some());
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

    #[tokio::test]
    async fn test_cancel_assigned_run_enters_cancelling_without_clearing_task() {
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

        let task_id = {
            let task = state.tasks.write().await.poll(
                "agent-1",
                std::time::Duration::from_secs(60),
                &state.epoch_gen,
            );
            let task = task.expect("expected assigned task");
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.set_current_task(
                &run_id,
                task.task_id.clone(),
                "agent-1".into(),
                task.attempt,
                task.lease_epoch,
            );
            task.task_id
        };

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);
        assert!(resp.message.contains("heartbeat"));

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Cancelling
        );
        drop(runs);

        let tasks = state.tasks.read().await;
        let task = tasks.get(&task_id).unwrap();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
    }

    #[tokio::test]
    async fn test_cancel_run_with_stale_running_snapshot_returns_not_accepted() {
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

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Completed)
                .unwrap();
        }

        let resp = svc
            .cancel_run_for_state(&run_id, InternalRunState::Running)
            .await
            .unwrap();

        assert!(!resp.accepted);
        assert!(resp.message.contains("terminal state"));
    }

    #[tokio::test]
    async fn test_watch_run_rechecks_terminal_state_after_subscribe() {
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

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Completed)
                .unwrap();
            if let Some(record) = runs.get_run_mut(&run_id) {
                record.total_records = 11;
            }
        }

        let response = svc.watch_run_after_subscribe(&run_id).await.unwrap();
        let mut stream = response.into_inner();
        let event = stream.next().await.unwrap().unwrap();

        match event.event {
            Some(run_event::Event::Completed(completed)) => {
                assert_eq!(completed.total_records, 11);
            }
            other => panic!("Expected completed event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_watch_run_missing_run_cleans_up_subscription() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

        let Err(err) = svc.watch_run_after_subscribe("missing-run").await else {
            panic!("expected missing run watch to fail");
        };

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(state.watchers.read().await.channel_count(), 0);
    }
}
