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
    RunCancelled, RunCompleted, RunEvent, RunFailed, RunState, RunStatus, RunSummary,
    StreamPreview, SubmitPipelineRequest, SubmitPipelineResponse, TaskError, TaskRef,
    WatchRunRequest,
};
use crate::run_state::{
    RunState as InternalRunState, ERROR_CODE_LEASE_EXPIRED, ERROR_CODE_RECOVERY_TIMEOUT,
};
use crate::state::ControllerState;

/// Maps internal `RunState` to proto `RunState` enum value.
#[must_use]
pub fn to_proto_state(s: InternalRunState) -> i32 {
    match s {
        InternalRunState::Pending => RunState::Pending.into(),
        InternalRunState::Assigned => RunState::Assigned.into(),
        InternalRunState::Reconciling => RunState::Reconciling.into(),
        InternalRunState::Running | InternalRunState::Cancelling => RunState::Running.into(),
        InternalRunState::PreviewReady => RunState::PreviewReady.into(),
        InternalRunState::Completed => RunState::Completed.into(),
        InternalRunState::RecoveryFailed => RunState::RecoveryFailed.into(),
        InternalRunState::Failed | InternalRunState::TimedOut => RunState::Failed.into(),
        InternalRunState::Cancelled => RunState::Cancelled.into(),
    }
}

/// Maps proto `RunState` i32 back to internal `RunState`(s) for filtering.
/// `RUNNING` maps to both `Running` and `Cancelling` (externally both appear as `RUNNING`).
/// `FAILED` includes normal failure plus ordinary lease timeouts.
/// `RECOVERY_FAILED` is reserved for reconciliation-specific terminal failure.
fn from_proto_states(v: i32) -> Option<Vec<InternalRunState>> {
    match RunState::try_from(v) {
        Ok(RunState::Pending) => Some(vec![InternalRunState::Pending]),
        Ok(RunState::Assigned) => Some(vec![InternalRunState::Assigned]),
        Ok(RunState::Running) => Some(vec![
            InternalRunState::Running,
            InternalRunState::Cancelling,
        ]),
        Ok(RunState::Reconciling) => Some(vec![InternalRunState::Reconciling]),
        Ok(RunState::PreviewReady) => Some(vec![InternalRunState::PreviewReady]),
        Ok(RunState::Completed) => Some(vec![InternalRunState::Completed]),
        Ok(RunState::Failed) => Some(vec![InternalRunState::Failed, InternalRunState::TimedOut]),
        Ok(RunState::RecoveryFailed) => Some(vec![InternalRunState::RecoveryFailed]),
        Ok(RunState::Cancelled) => Some(vec![InternalRunState::Cancelled]),
        _ => None,
    }
}

fn terminal_error_for_run(record: &crate::run_state::RunRecord) -> Option<TaskError> {
    let message = record.error_message.clone()?;
    let (code, retryable, safe_to_retry) = match record.state {
        InternalRunState::RecoveryFailed => (ERROR_CODE_RECOVERY_TIMEOUT.into(), false, false),
        InternalRunState::TimedOut => (ERROR_CODE_LEASE_EXPIRED.into(), true, true),
        _ => (
            record.error_code.clone().unwrap_or_default(),
            record.error_retryable.unwrap_or(false),
            record.error_safe_to_retry.unwrap_or(false),
        ),
    };
    Some(TaskError {
        code,
        message,
        retryable,
        safe_to_retry,
        commit_state: record.error_commit_state.clone().unwrap_or_default(),
    })
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
        _ => run_event::Event::Failed(RunFailed {
            error: terminal_error_for_run(record),
            attempt: record.attempt,
        }),
    };
    RunEvent {
        run_id: record.run_id.clone(),
        event: Some(event),
    }
}

fn status_event_for_run(record: &crate::run_state::RunRecord) -> Option<RunEvent> {
    match record.state {
        InternalRunState::Reconciling => Some(RunEvent {
            run_id: record.run_id.clone(),
            event: Some(run_event::Event::Status(RunStatus {
                state: RunState::Reconciling.into(),
                message: "Controller restarted while this run was in flight; waiting to reconcile the active lease.".into(),
            })),
        }),
        _ => None,
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

    async fn rollback_new_submission(&self, run_id: &str, task_id: &str) {
        {
            let mut tasks = self.state.tasks.write().await;
            let _ = tasks.remove_task(task_id);
        }
        {
            let mut runs = self.state.runs.write().await;
            let _ = runs.remove_run(run_id);
        }
    }

    async fn rollback_queued_cancel(
        &self,
        previous_run: crate::run_state::RunRecord,
        previous_task: Option<crate::scheduler::TaskRecord>,
    ) {
        if let Some(previous_task) = previous_task {
            let mut tasks = self.state.tasks.write().await;
            tasks.restore_task(previous_task);
        }
        let mut runs = self.state.runs.write().await;
        runs.restore_run(previous_run);
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

    async fn status_event_for_existing_run(
        &self,
        run_id: &str,
    ) -> Result<Option<RunEvent>, Status> {
        let runs = self.state.runs.read().await;
        let record = runs
            .get_run(run_id)
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
        Ok(status_event_for_run(record))
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

        let live_stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(event) => Some(Ok(event)),
            Err(_) => None, // Lag or closed — skip
        });

        let status_event = match self.status_event_for_existing_run(run_id).await {
            Ok(event) => event,
            Err(err) => {
                self.state.watchers.write().await.remove(run_id);
                return Err(err);
            }
        };

        let stream: WatchRunStream = match status_event {
            Some(event) => Box::pin(tokio_stream::once(Ok(event)).chain(live_stream)),
            None => Box::pin(live_stream),
        };

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
            InternalRunState::Reconciling | InternalRunState::Running => {
                self.cancel_running_run(run_id, snapshot_state).await
            }
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
        let previous_run = {
            self.state
                .runs
                .read()
                .await
                .get_run(run_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
        };
        if let Some(response) = self
            .transition_or_retry(run_id, snapshot_state, InternalRunState::Cancelled)
            .await?
        {
            return Ok(response);
        }
        let (previous_task, cancelled_task) = self.cancel_latest_task(run_id).await;
        if let Some(cancelled_task) = cancelled_task.as_ref() {
            if let Err(error) = self.state.persist_task_record(cancelled_task).await {
                self.rollback_queued_cancel(previous_run, previous_task)
                    .await;
                return Err(Status::internal(error.to_string()));
            }
        }
        let cancelled_run = self
            .state
            .runs
            .read()
            .await
            .get_run(run_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
        if let Err(error) = self.state.persist_run_record(&cancelled_run).await {
            if let Some(previous_task_record) = previous_task.clone() {
                let rollback_task_id = previous_task_record.task_id.clone();
                let rollback_error = self
                    .state
                    .persist_task_record(&previous_task_record)
                    .await
                    .err();
                self.rollback_queued_cancel(previous_run, Some(previous_task_record))
                    .await;
                return Err(Status::internal(match rollback_error {
                    Some(rollback_error) => {
                        format!(
                            "{error}; durable rollback for task {rollback_task_id} also failed: {rollback_error}"
                        )
                    }
                    None => error.to_string(),
                }));
            }
            self.rollback_queued_cancel(previous_run, None).await;
            return Err(Status::internal(error.to_string()));
        }
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
        self.state
            .persist_run(run_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
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
        self.state
            .persist_run(run_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
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

    async fn cancel_latest_task(
        &self,
        run_id: &str,
    ) -> (
        Option<crate::scheduler::TaskRecord>,
        Option<crate::scheduler::TaskRecord>,
    ) {
        let mut tasks = self.state.tasks.write().await;
        if let Some(task) = tasks.find_by_run_id(run_id) {
            let previous_task = task.clone();
            let task_id = previous_task.task_id.clone();
            let _ = tasks.cancel(&task_id);
            let cancelled_task = tasks.get(&task_id).cloned();
            return (Some(previous_task), cancelled_task);
        }
        (None, None)
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
            let run_snapshot = {
                self.state
                    .runs
                    .read()
                    .await
                    .get_run(&actual_run_id)
                    .cloned()
                    .expect("newly created run should exist")
            };
            let task_snapshot = {
                self.state
                    .tasks
                    .read()
                    .await
                    .get(&task_id)
                    .cloned()
                    .expect("newly created task should exist")
            };

            tracing::info!(run_id = %actual_run_id, task_id, "Pipeline submitted");
            if let Err(error) = self
                .state
                .create_run_with_task_records(&run_snapshot, &task_snapshot)
                .await
            {
                self.rollback_new_submission(&actual_run_id, &task_id).await;
                return Err(Status::internal(error.to_string()));
            }
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
                terminal_error_for_run(record),
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
        let state_filter = match req.filter_state {
            Some(state) => Some(
                from_proto_states(state)
                    .ok_or_else(|| Status::invalid_argument("Unknown filter_state"))?,
            ),
            None => None,
        };

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
    use crate::scheduler::TaskState;
    use crate::store::test_support::FailingMetadataStore;
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
    async fn test_submit_pipeline_rolls_back_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(1);
        let state = ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store);
        let svc = PipelineServiceImpl::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let first = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .expect_err("persistence failure should reject submit");
        assert_eq!(first.code(), tonic::Code::Internal);
        assert!(state.runs.read().await.all_runs().is_empty());
        assert!(state.tasks.read().await.all_tasks().is_empty());
        assert!(state
            .runs
            .read()
            .await
            .find_by_idempotency_key("dedup-key")
            .is_none());

        let second = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .expect("second submit should succeed after rollback")
            .into_inner();

        assert!(!second.run_id.is_empty());
        assert_eq!(state.runs.read().await.all_runs().len(), 1);
        assert_eq!(state.tasks.read().await.all_tasks().len(), 1);
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
    async fn list_runs_rejects_unknown_filter_state() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state);

        let err = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(999),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Unknown filter_state"));
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
    async fn test_cancel_queued_run_rolls_back_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(2);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store.clone());
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
        let task_id = state
            .tasks
            .read()
            .await
            .find_by_run_id(&run_id)
            .expect("submitted task should exist")
            .task_id
            .clone();

        let err = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .expect_err("cancel should fail when task persistence fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Pending
        );
        drop(runs);

        let tasks = state.tasks.read().await;
        assert_eq!(tasks.get(&task_id).unwrap().state, TaskState::Pending);
        drop(tasks);

        assert_eq!(
            store
                .persisted_run(&run_id)
                .expect("durable run should exist")
                .state,
            InternalRunState::Pending
        );
        assert_eq!(
            store
                .persisted_task(&task_id)
                .expect("durable task should exist")
                .state,
            TaskState::Pending
        );
    }

    #[tokio::test]
    async fn test_cancel_queued_run_rolls_back_when_run_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(2);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store.clone());
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
        let task_id = state
            .tasks
            .read()
            .await
            .find_by_run_id(&run_id)
            .expect("submitted task should exist")
            .task_id
            .clone();

        let err = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .expect_err("cancel should fail when run persistence fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Pending
        );
        drop(runs);

        let tasks = state.tasks.read().await;
        assert_eq!(tasks.get(&task_id).unwrap().state, TaskState::Pending);
        drop(tasks);

        assert_eq!(
            store
                .persisted_run(&run_id)
                .expect("durable run should exist")
                .state,
            InternalRunState::Pending
        );
        assert_eq!(
            store
                .persisted_task(&task_id)
                .expect("durable task should exist")
                .state,
            TaskState::Pending
        );
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
    async fn test_cancel_reconciling_run_enters_cancelling() {
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);
        assert!(resp.message.contains("heartbeat"));
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Cancelling
        );
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
    async fn test_get_run_returns_reconciling_state() {
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, RunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_list_runs_returns_reconciling_state() {
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let run = resp
            .runs
            .into_iter()
            .find(|run| run.run_id == run_id)
            .unwrap();
        assert_eq!(run.state, RunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_list_runs_filters_reconciling_state() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let reconciling_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let running_run_id = svc
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
            runs.transition(&reconciling_run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&reconciling_run_id, InternalRunState::Reconciling)
                .unwrap();
            runs.transition(&running_run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&running_run_id, InternalRunState::Running)
                .unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(RunState::Reconciling as i32),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 1);
        assert_eq!(resp.runs[0].run_id, reconciling_run_id);
        assert_eq!(resp.runs[0].state, RunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_get_run_returns_failed_state_for_timed_out_run() {
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
            runs.transition(&run_id, InternalRunState::TimedOut)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().error_message =
                Some("Task task-1 lease expired (agent unresponsive)".into());
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, RunState::Failed as i32);
        let error = resp
            .last_error
            .expect("timed out runs expose recovery error");
        assert_eq!(error.code, "LEASE_EXPIRED");
    }

    #[tokio::test]
    async fn test_get_run_returns_execution_error_code_for_failed_run() {
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
            runs.transition(&run_id, InternalRunState::Failed).unwrap();
            let run = runs.get_run_mut(&run_id).unwrap();
            run.error_code = Some("TEST_EXECUTION_FAILED".into());
            run.error_message = Some("TEST_EXECUTION_FAILED: injected failure".into());
            run.error_retryable = Some(false);
            run.error_safe_to_retry = Some(false);
            run.error_commit_state = Some("before_commit".into());
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, RunState::Failed as i32);
        let error = resp
            .last_error
            .expect("failed runs expose execution error metadata");
        assert_eq!(error.code, "TEST_EXECUTION_FAILED");
        assert_eq!(error.message, "TEST_EXECUTION_FAILED: injected failure");
        assert_eq!(error.commit_state, "before_commit");
    }

    #[tokio::test]
    async fn test_list_runs_filters_recovery_failed_state() {
        let state = test_state();
        let svc = PipelineServiceImpl::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let recovery_failed_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let failed_run_id = svc
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
            runs.transition(&recovery_failed_run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&recovery_failed_run_id, InternalRunState::RecoveryFailed)
                .unwrap();
            runs.transition(&failed_run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&failed_run_id, InternalRunState::Running)
                .unwrap();
            runs.transition(&failed_run_id, InternalRunState::Failed)
                .unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(RunState::RecoveryFailed as i32),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 1);
        assert_eq!(resp.runs[0].run_id, recovery_failed_run_id);
        assert_eq!(resp.runs[0].state, RunState::RecoveryFailed as i32);
    }

    #[tokio::test]
    async fn test_watch_run_surfaces_reconciling_status() {
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
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let response = svc.watch_run_after_subscribe(&run_id).await.unwrap();
        let mut stream = response.into_inner();
        let event = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("expected a reconciling event")
            .expect("stream should yield an event")
            .unwrap();

        match event.event {
            Some(run_event::Event::Status(status)) => {
                assert_eq!(status.state, RunState::Reconciling as i32);
                assert!(status.message.contains("waiting to reconcile"));
            }
            other => panic!("expected reconciling status event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_watch_run_returns_recovery_failed_terminal_event_for_timed_out_run() {
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
            runs.transition(&run_id, InternalRunState::RecoveryFailed)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().error_message =
                Some("Run recovery reconciliation timed out after controller restart".into());
        }

        let response = svc.watch_run_after_subscribe(&run_id).await.unwrap();
        let mut stream = response.into_inner();
        let event = stream.next().await.unwrap().unwrap();

        match event.event {
            Some(run_event::Event::Failed(failed)) => {
                let error = failed
                    .error
                    .expect("timed out watch exposes recovery error");
                assert_eq!(error.code, "RECOVERY_TIMEOUT");
                assert!(error.message.contains("reconciliation timed out"));
            }
            other => panic!("expected failed terminal event, got {other:?}"),
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
