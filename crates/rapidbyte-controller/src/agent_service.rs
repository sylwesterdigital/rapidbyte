//! `AgentService` gRPC handler implementations.

use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::proto::rapidbyte::v1::{
    agent_directive, agent_service_server::AgentService, poll_task_response, run_event,
    AgentDirective, CancelTask, CompleteTaskRequest, CompleteTaskResponse, ExecutionOptions,
    HeartbeatRequest, HeartbeatResponse, NoTask, PollTaskRequest, PollTaskResponse,
    RegisterAgentRequest, RegisterAgentResponse, ReportProgressRequest, ReportProgressResponse,
    RunCancelled, RunCompleted, RunEvent, RunFailed, TaskAssignment, TaskOutcome,
};
use crate::run_state::RunState as InternalRunState;
use crate::scheduler::{TaskState, TerminalTaskOutcome};
use crate::state::ControllerState;

/// Default lease TTL for assigned tasks.
const LEASE_TTL: Duration = Duration::from_secs(300);

pub struct AgentServiceImpl {
    state: ControllerState,
}

impl AgentServiceImpl {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
#[allow(clippy::too_many_lines)]
impl AgentService for AgentServiceImpl {
    async fn register_agent(
        &self,
        request: Request<RegisterAgentRequest>,
    ) -> Result<Response<RegisterAgentResponse>, Status> {
        let req = request.into_inner();
        let agent_id = uuid::Uuid::new_v4().to_string();

        let bundle_hash = req.plugin_bundle_hash.clone();

        let mut registry = self.state.registry.write().await;

        // Log bundle hash mismatch warnings before registering
        if !bundle_hash.is_empty() {
            for other in registry.list() {
                if !other.plugin_bundle_hash.is_empty() && other.plugin_bundle_hash != bundle_hash {
                    tracing::warn!(
                        new_agent = agent_id,
                        existing_agent = other.agent_id,
                        new_hash = bundle_hash,
                        existing_hash = other.plugin_bundle_hash,
                        "Bundle hash mismatch across agent pool"
                    );
                    break;
                }
            }
        }

        registry.register(
            agent_id.clone(),
            req.max_tasks,
            req.flight_advertise_endpoint,
            req.plugin_bundle_hash,
            req.available_plugins,
            req.memory_bytes,
        );

        tracing::info!(agent_id, "Agent registered");
        Ok(Response::new(RegisterAgentResponse { agent_id }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        // Update heartbeat in registry
        {
            let mut registry = self.state.registry.write().await;
            registry
                .heartbeat(&req.agent_id, req.active_tasks)
                .map_err(|e| Status::not_found(e.to_string()))?;
        }

        // Renew leases for active tasks reported by the agent
        if !req.active_leases.is_empty() {
            let mut tasks = self.state.tasks.write().await;
            for active_lease in &req.active_leases {
                tasks.renew_lease(
                    &active_lease.task_id,
                    &req.agent_id,
                    active_lease.lease_epoch,
                    LEASE_TTL,
                );
            }
        }

        // Check for cancel directives — look up active leases and see if any
        // of their runs are in Cancelling state
        let mut directives = Vec::new();
        {
            let runs = self.state.runs.read().await;
            let tasks = self.state.tasks.read().await;

            for active_lease in &req.active_leases {
                if let Some(task) = tasks.get(&active_lease.task_id) {
                    let lease_matches = task.lease.as_ref().is_some_and(|lease| {
                        lease.is_valid(active_lease.lease_epoch)
                            && task.assigned_agent_id.as_deref() == Some(req.agent_id.as_str())
                    });
                    if !lease_matches {
                        continue;
                    }

                    if let Some(run) = runs.get_run(&task.run_id) {
                        if run.state == InternalRunState::Cancelling {
                            directives.push(AgentDirective {
                                directive: Some(agent_directive::Directive::CancelTask(
                                    CancelTask {
                                        task_id: active_lease.task_id.clone(),
                                        lease_epoch: active_lease.lease_epoch,
                                    },
                                )),
                            });
                        }
                    }
                }
            }
        }

        Ok(Response::new(HeartbeatResponse { directives }))
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        let req = request.into_inner();
        let wait = Duration::from_secs(u64::from(req.wait_seconds).min(60));
        let max_tasks = {
            let registry = self.state.registry.read().await;
            registry
                .get(&req.agent_id)
                .ok_or_else(|| Status::not_found("Unknown agent"))?
                .max_tasks
        };

        // Try immediate poll
        {
            let mut tasks = self.state.tasks.write().await;
            if tasks.active_tasks_for_agent(&req.agent_id)
                >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
            {
                return Ok(Response::new(PollTaskResponse {
                    result: Some(poll_task_response::Result::NoTask(NoTask {})),
                }));
            }
            if let Some(assignment) = tasks.poll(&req.agent_id, LEASE_TTL, &self.state.epoch_gen) {
                let claimed = {
                    let mut runs = self.state.runs.write().await;
                    if runs
                        .transition(&assignment.run_id, InternalRunState::Assigned)
                        .is_ok()
                    {
                        runs.set_current_task(
                            &assignment.run_id,
                            assignment.task_id.clone(),
                            req.agent_id.clone(),
                            assignment.attempt,
                            assignment.lease_epoch,
                        );
                        true
                    } else {
                        false
                    }
                };
                if claimed {
                    return Ok(Response::new(make_task_response(assignment)));
                }

                let _ = tasks.reject_assignment(&assignment.task_id, assignment.lease_epoch);
            }
        }

        // Long-poll: wait for notification or timeout
        let notified = self.state.task_notify.notified();
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep(wait) => {},
        }

        // Try again after wakeup
        {
            let registry = self.state.registry.read().await;
            if registry.get(&req.agent_id).is_none() {
                return Err(Status::not_found("Unknown agent"));
            }
        }
        let mut tasks = self.state.tasks.write().await;
        if tasks.active_tasks_for_agent(&req.agent_id)
            >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
        {
            return Ok(Response::new(PollTaskResponse {
                result: Some(poll_task_response::Result::NoTask(NoTask {})),
            }));
        }
        if let Some(assignment) = tasks.poll(&req.agent_id, LEASE_TTL, &self.state.epoch_gen) {
            let claimed = {
                let mut runs = self.state.runs.write().await;
                if runs
                    .transition(&assignment.run_id, InternalRunState::Assigned)
                    .is_ok()
                {
                    runs.set_current_task(
                        &assignment.run_id,
                        assignment.task_id.clone(),
                        req.agent_id.clone(),
                        assignment.attempt,
                        assignment.lease_epoch,
                    );
                    true
                } else {
                    false
                }
            };
            if claimed {
                return Ok(Response::new(make_task_response(assignment)));
            }

            let _ = tasks.reject_assignment(&assignment.task_id, assignment.lease_epoch);
        }

        Ok(Response::new(PollTaskResponse {
            result: Some(poll_task_response::Result::NoTask(NoTask {})),
        }))
    }

    async fn report_progress(
        &self,
        request: Request<ReportProgressRequest>,
    ) -> Result<Response<ReportProgressResponse>, Status> {
        let req = request.into_inner();

        // Validate lease and update scheduler state while holding the task lock.
        let run_id = {
            let mut tasks = self.state.tasks.write().await;
            let task = tasks
                .get(&req.task_id)
                .ok_or_else(|| Status::not_found("Task not found"))?;
            match (&task.lease, task.state) {
                (Some(lease), TaskState::Assigned | TaskState::Running)
                    if lease.is_valid(req.lease_epoch) => {}
                _ => {
                    return Err(Status::failed_precondition("Stale lease epoch"));
                }
            }
            if task.assigned_agent_id.as_deref() != Some(req.agent_id.as_str()) {
                return Err(Status::permission_denied(
                    "Task lease belongs to a different agent",
                ));
            }

            let run_id = task.run_id.clone();
            let task_state = task.state;
            if task_state == TaskState::Assigned {
                tasks
                    .report_running(&req.task_id, &req.agent_id, req.lease_epoch)
                    .map_err(|err| match err {
                        crate::scheduler::SchedulerError::AgentMismatch(_, _) => {
                            Status::permission_denied("Task lease belongs to a different agent")
                        }
                        _ => Status::failed_precondition("Stale lease epoch"),
                    })?;
            }
            run_id
        };

        self.state.runs.write().await.ensure_running(&run_id);

        if let Some(progress) = req.progress {
            let watchers = self.state.watchers.read().await;
            watchers.publish(
                &run_id,
                RunEvent {
                    run_id: run_id.clone(),
                    event: Some(run_event::Event::Progress(progress)),
                },
            );
        }

        Ok(Response::new(ReportProgressResponse {}))
    }

    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> Result<Response<CompleteTaskResponse>, Status> {
        let req = request.into_inner();

        let outcome = match TaskOutcome::try_from(req.outcome) {
            Ok(TaskOutcome::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument("Unknown task outcome"));
            }
            Ok(outcome) => outcome,
        };

        // Complete the task in the scheduler (validates lease epoch).
        // Returns run_id and attempt alongside acknowledgement to avoid a second lock.
        let scheduler_outcome = match outcome {
            TaskOutcome::Completed => TerminalTaskOutcome::Completed,
            TaskOutcome::Failed => TerminalTaskOutcome::Failed,
            TaskOutcome::Cancelled => TerminalTaskOutcome::Cancelled,
            TaskOutcome::Unspecified => unreachable!("invalid task outcome rejected above"),
        };
        let (run_id, attempt) = {
            let mut tasks = self.state.tasks.write().await;
            match tasks
                .complete(
                    &req.task_id,
                    &req.agent_id,
                    req.lease_epoch,
                    scheduler_outcome,
                )
                .map_err(|e| Status::not_found(e.to_string()))?
            {
                Some(info) => info,
                None => {
                    return Ok(Response::new(CompleteTaskResponse {
                        acknowledged: false,
                    }));
                }
            }
        };

        // Transition run state and publish events
        match outcome {
            TaskOutcome::Completed => {
                let has_preview = req.preview.is_some();
                {
                    let mut runs = self.state.runs.write().await;
                    runs.ensure_running(&run_id);
                    // Dry-run tasks with preview data pass through PreviewReady
                    // so clients can discover previews via GetRun/ListRuns.
                    if has_preview {
                        let _ = runs.transition(&run_id, InternalRunState::PreviewReady);
                    }
                    let _ = runs.transition(&run_id, InternalRunState::Completed);
                    if let Some(record) = runs.get_run_mut(&run_id) {
                        let metrics = req.metrics.as_ref();
                        record.total_records = metrics.map_or(0, |m| m.records_processed);
                        record.total_bytes = metrics.map_or(0, |m| m.bytes_processed);
                        record.elapsed_seconds = metrics.map_or(0.0, |m| m.elapsed_seconds);
                        record.cursors_advanced = metrics.map_or(0, |m| m.cursors_advanced);
                    }
                }

                // Store preview if provided (runs lock dropped first).
                // Controller signs the ticket — agent sends flight_endpoint only.
                if let Some(preview) = &req.preview {
                    let expires_at_unix = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + 300;
                    let signed_streams = preview
                        .streams
                        .iter()
                        .map(|stream| {
                            let payload = crate::preview::TicketPayload {
                                run_id: run_id.clone(),
                                task_id: req.task_id.clone(),
                                stream_name: stream.stream.clone(),
                                lease_epoch: req.lease_epoch,
                                expires_at_unix,
                            };
                            crate::preview::PreviewStreamEntry {
                                stream: stream.stream.clone(),
                                rows: stream.rows,
                                ticket: self.state.ticket_signer.sign(&payload),
                            }
                        })
                        .collect::<Vec<_>>();
                    let signed_ticket = signed_streams
                        .first()
                        .map_or_else(bytes::Bytes::new, |stream| stream.ticket.clone());

                    let mut previews = self.state.previews.write().await;
                    previews.store(crate::preview::PreviewEntry {
                        run_id: run_id.clone(),
                        task_id: req.task_id.clone(),
                        flight_endpoint: preview.flight_endpoint.clone(),
                        ticket: signed_ticket,
                        streams: signed_streams,
                        created_at: std::time::Instant::now(),
                        ttl: Duration::from_secs(300),
                    });
                }

                let metrics = req.metrics.as_ref();
                self.state.watchers.write().await.publish_terminal(
                    &run_id,
                    RunEvent {
                        run_id: run_id.clone(),
                        event: Some(run_event::Event::Completed(RunCompleted {
                            total_records: metrics.map_or(0, |m| m.records_processed),
                            total_bytes: metrics.map_or(0, |m| m.bytes_processed),
                            elapsed_seconds: metrics.map_or(0.0, |m| m.elapsed_seconds),
                            cursors_advanced: metrics.map_or(0, |m| m.cursors_advanced),
                        })),
                    },
                );
            }
            TaskOutcome::Failed => {
                let error = req.error.as_ref();
                let safe_to_retry = error.is_some_and(|e| e.safe_to_retry);
                let retryable = error.is_some_and(|e| e.retryable);
                let commit_state = error.map_or("before_commit", |e| e.commit_state.as_str());
                let is_cancelling = self
                    .state
                    .runs
                    .read()
                    .await
                    .get_run(&run_id)
                    .is_some_and(|run| run.state == InternalRunState::Cancelling);

                // Retry safety policy: only auto-requeue if safe_to_retry AND retryable
                // AND not after any commit state
                let should_retry = safe_to_retry
                    && retryable
                    && !is_cancelling
                    && commit_state != "after_commit_unknown"
                    && commit_state != "after_commit_confirmed";

                if should_retry {
                    // Extract retry-specific task data (only cloned when needed)
                    let (yaml, dry_run, limit) = {
                        let tasks = self.state.tasks.read().await;
                        let task = tasks.get(&req.task_id).unwrap();
                        (task.pipeline_yaml.clone(), task.dry_run, task.limit)
                    };

                    {
                        let mut runs = self.state.runs.write().await;
                        let _ = runs.transition(&run_id, InternalRunState::Failed);
                        if let Some(record) = runs.get_run_mut(&run_id) {
                            record.attempt += 1;
                        }
                        runs.prepare_retry(&run_id);
                    }

                    {
                        let mut tasks = self.state.tasks.write().await;
                        tasks.enqueue(run_id.clone(), yaml, dry_run, limit, attempt + 1);
                    }
                    self.state.task_notify.notify_waiters();

                    tracing::info!(run_id, attempt = attempt + 1, "Auto-requeued failed task");
                } else {
                    {
                        let mut runs = self.state.runs.write().await;
                        runs.ensure_running(&run_id);
                        let _ = runs.transition(&run_id, InternalRunState::Failed);
                        if let Some(record) = runs.get_run_mut(&run_id) {
                            record.error_message =
                                error.map(|e| format!("{}: {}", e.code, e.message));
                        }
                    }

                    self.state.watchers.write().await.publish_terminal(
                        &run_id,
                        RunEvent {
                            run_id: run_id.clone(),
                            event: Some(run_event::Event::Failed(RunFailed {
                                error: req.error,
                                attempt,
                            })),
                        },
                    );
                }
            }
            TaskOutcome::Cancelled => {
                {
                    let mut runs = self.state.runs.write().await;
                    runs.ensure_running(&run_id);
                    let _ = runs.transition(&run_id, InternalRunState::Cancelled);
                }

                self.state.watchers.write().await.publish_terminal(
                    &run_id,
                    RunEvent {
                        run_id: run_id.clone(),
                        event: Some(run_event::Event::Cancelled(RunCancelled {})),
                    },
                );
            }
            TaskOutcome::Unspecified => unreachable!("invalid task outcome rejected above"),
        }

        Ok(Response::new(CompleteTaskResponse { acknowledged: true }))
    }
}

fn make_task_response(assignment: crate::scheduler::TaskAssignment) -> PollTaskResponse {
    PollTaskResponse {
        result: Some(poll_task_response::Result::Task(TaskAssignment {
            task_id: assignment.task_id,
            run_id: assignment.run_id,
            attempt: assignment.attempt,
            lease_epoch: assignment.lease_epoch,
            lease_expires_at: None,
            pipeline_yaml_utf8: assignment.pipeline_yaml,
            execution: Some(ExecutionOptions {
                dry_run: assignment.dry_run,
                limit: assignment.limit,
            }),
        })),
    }
}

#[cfg(test)]
fn test_state() -> ControllerState {
    ControllerState::new(b"test-key-for-agent-service!!!!")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::manual_let_else, clippy::match_same_arms)]

    use super::*;
    use crate::proto::rapidbyte::v1::{
        pipeline_service_server::PipelineService as _, ActiveLease, ProgressUpdate,
        SubmitPipelineRequest, TaskError,
    };

    /// Helper to submit a pipeline and return the `run_id`.
    async fn submit_pipeline(state: &ControllerState) -> String {
        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        svc.submit_pipeline(Request::new(SubmitPipelineRequest {
            pipeline_yaml_utf8: yaml.to_vec(),
            execution: Some(ExecutionOptions {
                dry_run: false,
                limit: None,
            }),
            idempotency_key: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .run_id
    }

    #[tokio::test]
    async fn test_register_agent_returns_uuid() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state);

        let resp = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 2,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: "hash".into(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.agent_id.is_empty());
        // Should be a valid UUID
        assert!(uuid::Uuid::parse_str(&resp.agent_id).is_ok());
    }

    #[tokio::test]
    async fn test_poll_task_returns_pending_task() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        // Register agent
        let agent_id = svc
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

        // Submit a pipeline
        let _run_id = submit_pipeline(&state).await;

        // Poll — should get the task immediately (wait_seconds=0)
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::Task(t)) => {
                assert!(!t.task_id.is_empty());
                assert!(t.lease_epoch > 0);
            }
            _ => panic!("Expected a task assignment"),
        }
    }

    #[tokio::test]
    async fn poll_task_does_not_return_cancelled_assignment() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Cancelled)
                .unwrap();
        }

        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(matches!(
            resp.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        let tasks = state.tasks.read().await;
        let task = tasks.find_by_run_id(&run_id).unwrap();
        assert_eq!(task.state, TaskState::Cancelled);
        assert!(task.lease.is_none());
    }

    #[tokio::test]
    async fn test_poll_task_rejects_unknown_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let _run_id = submit_pipeline(&state).await;

        let err = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: "unknown-agent".into(),
                wait_seconds: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_poll_task_respects_agent_capacity() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let second_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;
        let second_run_id = submit_pipeline(&state).await;

        let first = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            first.result,
            Some(poll_task_response::Result::Task(_))
        ));

        let capped = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            capped.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        let second = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: second_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        match second.result {
            Some(poll_task_response::Result::Task(task)) => {
                assert_eq!(task.run_id, second_run_id);
            }
            _ => panic!("Expected second agent to receive queued task"),
        }
    }

    #[tokio::test]
    async fn test_complete_task_with_stale_epoch_returns_unacknowledged() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        // Register + submit + poll
        let agent_id = svc
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

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with wrong epoch
        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch + 999,
                outcome: TaskOutcome::Completed.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.acknowledged);
    }

    #[tokio::test]
    async fn test_complete_task_rejects_wrong_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id: wrong_agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Completed.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.acknowledged);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert_eq!(record.assigned_agent_id.as_deref(), Some(agent_id.as_str()));
    }

    #[tokio::test]
    async fn test_complete_task_rejects_unknown_outcome() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: 999,
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert!(record.lease.is_some());

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_complete_task_rejects_unspecified_outcome() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Unspecified.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert!(record.lease.is_some());

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_complete_task_cancelled_from_assigned_transitions_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Cancelled.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Cancelled);
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Cancelled);
    }

    #[tokio::test]
    async fn test_complete_task_cancelled_from_cancelling_transitions_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Cancelled.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Cancelled);
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Cancelled);
    }

    #[tokio::test]
    async fn test_complete_task_safe_to_retry_requeues() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with retryable + safe_to_retry
        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id,
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "CONN_RESET".into(),
                message: "connection reset".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        // Should be requeued — poll again
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::Task(t)) => {
                assert_eq!(t.run_id, run_id);
                assert_eq!(t.attempt, 2);
            }
            _ => panic!("Expected requeued task"),
        }
    }

    #[tokio::test]
    async fn test_complete_task_unsafe_does_not_requeue() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with safe_to_retry=false
        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id,
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "DATA_ERROR".into(),
                message: "schema mismatch".into(),
                retryable: true,
                safe_to_retry: false,
                commit_state: "after_commit_unknown".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        // Should NOT be requeued — poll returns empty
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::NoTask(_)) => {} // expected
            None => {}                                        // also fine
            _ => panic!("Expected no task"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_returns_cancel_directive_for_cancelling_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Transition to Running then Cancelling
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        // Heartbeat should return a cancel directive
        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id.clone(),
                    lease_epoch: task.lease_epoch,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.directives.len(), 1);
        match &resp.directives[0].directive {
            Some(agent_directive::Directive::CancelTask(ct)) => {
                assert_eq!(ct.task_id, task.task_id);
            }
            _ => panic!("Expected CancelTask directive"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_return_cancel_directive_for_foreign_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
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

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id: wrong_agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id,
                    lease_epoch: task.lease_epoch,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.directives.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_return_cancel_directive_for_stale_epoch() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id,
                    lease_epoch: task.lease_epoch + 1,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.directives.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_renew_other_agents_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
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

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let before = {
            let tasks = state.tasks.read().await;
            tasks
                .get(&task.task_id)
                .unwrap()
                .lease
                .as_ref()
                .unwrap()
                .expires_at
        };

        tokio::time::sleep(Duration::from_millis(5)).await;

        svc.heartbeat(Request::new(HeartbeatRequest {
            agent_id: wrong_agent_id,
            active_leases: vec![ActiveLease {
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
            }],
            active_tasks: 1,
            cpu_usage: 0.0,
            memory_used_bytes: 0,
        }))
        .await
        .unwrap();

        let after = {
            let tasks = state.tasks.read().await;
            tasks
                .get(&task.task_id)
                .unwrap()
                .lease
                .as_ref()
                .unwrap()
                .expires_at
        };

        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn test_report_progress_rejects_missing_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        state.tasks.write().await.cancel(&task.task_id).unwrap();

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn test_report_progress_rejects_wrong_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
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

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id: wrong_agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        {
            let tasks = state.tasks.read().await;
            let record = tasks.get(&task.task_id).unwrap();
            assert_eq!(record.state, TaskState::Assigned);
            assert_eq!(
                record.assigned_agent_id.as_deref(),
                Some(owner_agent_id.as_str())
            );
        }

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_report_progress_does_not_flip_reassigned_attempt() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let first_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected first task"),
        };

        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: first_task.task_id.clone(),
            lease_epoch: first_task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "RETRY".into(),
                message: "try again".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        let second_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected requeued task"),
        };

        assert_ne!(second_task.task_id, first_task.task_id);
        assert_eq!(second_task.run_id, run_id);
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Assigned
        );

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id,
                task_id: first_task.task_id,
                lease_epoch: first_task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Assigned
        );
    }

    #[tokio::test]
    async fn test_complete_task_from_cancelling_transitions_run_to_completed() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Completed.into(),
                metrics: Some(crate::proto::rapidbyte::v1::TaskMetrics {
                    records_processed: 7,
                    bytes_processed: 42,
                    elapsed_seconds: 1.5,
                    cursors_advanced: 1,
                }),
                error: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Completed);
        assert_eq!(record.total_records, 7);
    }

    #[tokio::test]
    async fn test_complete_task_failure_from_cancelling_transitions_run_to_failed() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Failed.into(),
                metrics: None,
                error: Some(TaskError {
                    code: "PLUGIN".into(),
                    message: "boom".into(),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Failed);
        assert_eq!(record.error_message.as_deref(), Some("PLUGIN: boom"));
    }

    #[tokio::test]
    async fn test_complete_task_retryable_failure_from_cancelling_does_not_requeue() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
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

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Failed.into(),
                metrics: None,
                error: Some(TaskError {
                    code: "RETRY".into(),
                    message: "try again".into(),
                    retryable: true,
                    safe_to_retry: true,
                    commit_state: "before_commit".into(),
                }),
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Failed);
        assert_eq!(record.error_message.as_deref(), Some("RETRY: try again"));
        drop(runs);

        let tasks = state.tasks.read().await;
        let latest = tasks.find_by_run_id(&run_id).unwrap();
        assert_eq!(latest.task_id, task.task_id);
        assert_eq!(latest.attempt, 1);
        assert_eq!(latest.state, TaskState::Failed);
    }
}
