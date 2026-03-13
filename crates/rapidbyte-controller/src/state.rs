//! Shared controller state accessed by gRPC service handlers.

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use tokio::sync::{Notify, RwLock};

use crate::lease::EpochGenerator;
use crate::preview::{PreviewStore, TicketSigner};
use crate::registry::AgentRegistry;
use crate::run_state::RunStore;
use crate::scheduler::{TaskQueue, TaskState};
use crate::store::{MetadataSnapshot, MetadataStore};
use crate::watcher::RunWatchers;

/// Shared state container for the controller.
///
/// All gRPC service implementations hold an `Arc<ControllerState>`.
#[derive(Clone)]
pub struct ControllerState {
    pub runs: Arc<RwLock<RunStore>>,
    pub tasks: Arc<RwLock<TaskQueue>>,
    pub registry: Arc<RwLock<AgentRegistry>>,
    pub watchers: Arc<RwLock<RunWatchers>>,
    pub previews: Arc<RwLock<PreviewStore>>,
    pub epoch_gen: Arc<EpochGenerator>,
    pub ticket_signer: Arc<TicketSigner>,
    pub metadata_store: Option<Arc<MetadataStore>>,
    /// Notified when a new task is enqueued, waking long-poll waiters.
    pub task_notify: Arc<Notify>,
}

impl ControllerState {
    #[must_use]
    pub fn new(signing_key: &[u8]) -> Self {
        Self::from_snapshot(signing_key, None, MetadataSnapshot::default())
    }

    fn from_snapshot(
        signing_key: &[u8],
        metadata_store: Option<Arc<MetadataStore>>,
        snapshot: MetadataSnapshot,
    ) -> Self {
        let snapshot = normalize_recovery_snapshot(snapshot);
        let mut runs = RunStore::new();
        for run in snapshot.runs {
            runs.restore_run(run);
        }

        let mut tasks = TaskQueue::new();
        for task in snapshot.tasks {
            tasks.restore_task(task);
        }

        let mut registry = AgentRegistry::new();
        for agent in snapshot.agents {
            registry.restore_agent(agent);
        }

        let mut previews = PreviewStore::new();
        for preview in snapshot.previews {
            previews.restore(preview);
        }

        Self {
            runs: Arc::new(RwLock::new(runs)),
            tasks: Arc::new(RwLock::new(tasks)),
            registry: Arc::new(RwLock::new(registry)),
            watchers: Arc::new(RwLock::new(RunWatchers::new())),
            previews: Arc::new(RwLock::new(previews)),
            epoch_gen: Arc::new(EpochGenerator::with_start(snapshot.max_lease_epoch)),
            ticket_signer: Arc::new(TicketSigner::new(signing_key)),
            metadata_store,
            task_notify: Arc::new(Notify::new()),
        }
    }

    /// Build controller state by loading the latest durable metadata snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read.
    pub async fn from_metadata_store(
        signing_key: &[u8],
        metadata_store: MetadataStore,
    ) -> anyhow::Result<Self> {
        let metadata_store = Arc::new(metadata_store);
        let snapshot = metadata_store.load_snapshot().await?;
        Ok(Self::from_snapshot(
            signing_key,
            Some(metadata_store),
            snapshot,
        ))
    }

    /// Persist a run record when durable metadata storage is configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable metadata write fails.
    pub async fn persist_run(&self, run_id: &str) -> anyhow::Result<()> {
        let Some(metadata_store) = &self.metadata_store else {
            return Ok(());
        };
        let run = { self.runs.read().await.get_run(run_id).cloned() };
        if let Some(run) = run {
            metadata_store.upsert_run(&run).await?;
        }
        Ok(())
    }

    /// Persist a task record when durable metadata storage is configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable metadata write fails.
    pub async fn persist_task(&self, task_id: &str) -> anyhow::Result<()> {
        let Some(metadata_store) = &self.metadata_store else {
            return Ok(());
        };
        let task = { self.tasks.read().await.get(task_id).cloned() };
        if let Some(task) = task {
            metadata_store.upsert_task(&task).await?;
        }
        Ok(())
    }

    /// Persist preview metadata when durable metadata storage is configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable metadata write fails.
    pub async fn persist_preview(&self, run_id: &str) -> anyhow::Result<()> {
        let Some(metadata_store) = &self.metadata_store else {
            return Ok(());
        };

        let preview = { self.previews.write().await.get(run_id).cloned() };
        if let Some(preview) = preview {
            metadata_store
                .upsert_preview(
                    &preview.run_id,
                    &preview.task_id,
                    &preview.flight_endpoint,
                    preview.ticket.as_ref(),
                    &preview.streams,
                    preview_created_at(preview.created_at),
                    preview.ttl,
                )
                .await?;
        }
        Ok(())
    }

    /// Persist an agent record when durable metadata storage is configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable metadata write fails.
    pub async fn persist_agent(&self, agent_id: &str) -> anyhow::Result<()> {
        let Some(metadata_store) = &self.metadata_store else {
            return Ok(());
        };
        let agent = { self.registry.read().await.get(agent_id).cloned() };
        if let Some(agent) = agent {
            metadata_store.upsert_agent(&agent).await?;
        }
        Ok(())
    }

    /// Delete an agent record from durable metadata storage when configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable metadata write fails.
    pub async fn delete_agent(&self, agent_id: &str) -> anyhow::Result<()> {
        let Some(metadata_store) = &self.metadata_store else {
            return Ok(());
        };
        metadata_store.delete_agent(agent_id).await
    }
}

fn normalize_recovery_snapshot(mut snapshot: MetadataSnapshot) -> MetadataSnapshot {
    let inflight_run_ids = snapshot
        .tasks
        .iter()
        .filter(|task| matches!(task.state, TaskState::Assigned | TaskState::Running))
        .map(|task| task.run_id.clone())
        .collect::<std::collections::HashSet<_>>();

    for run in &mut snapshot.runs {
        if inflight_run_ids.contains(&run.run_id)
            && matches!(
                run.state,
                crate::run_state::RunState::Assigned | crate::run_state::RunState::Running
            )
        {
            run.state = crate::run_state::RunState::Reconciling;
            run.updated_at = SystemTime::now();
        }
    }

    snapshot
}

fn preview_created_at(created_at: Instant) -> SystemTime {
    SystemTime::now()
        .checked_sub(created_at.elapsed())
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preview::{PreviewEntry, PreviewStreamEntry};
    use crate::registry::AgentRecord;
    use crate::run_state::{RunRecord, RunState};
    use crate::scheduler::{TaskRecord, TaskState};
    use std::time::Duration;

    #[test]
    fn from_snapshot_rebuilds_runs_tasks_and_epoch_seed() {
        let now = SystemTime::now();
        let snapshot = MetadataSnapshot {
            runs: vec![RunRecord {
                run_id: "run-1".into(),
                pipeline_name: "pipe".into(),
                state: RunState::Assigned,
                created_at: now,
                updated_at: now,
                started_at: None,
                completed_at: None,
                current_task: None,
                error_message: None,
                attempt: 1,
                idempotency_key: Some("idem".into()),
                total_records: 0,
                total_bytes: 0,
                elapsed_seconds: 0.0,
                cursors_advanced: 0,
            }],
            tasks: vec![TaskRecord {
                task_id: "task-1".into(),
                run_id: "run-1".into(),
                attempt: 1,
                lease: None,
                state: TaskState::Pending,
                pipeline_yaml: b"pipeline: test".to_vec(),
                dry_run: false,
                limit: None,
                assigned_agent_id: None,
            }],
            agents: vec![],
            previews: vec![],
            max_lease_epoch: 7,
        };

        let state = ControllerState::from_snapshot(b"signing-key", None, snapshot);

        assert!(state.runs.blocking_read().get_run("run-1").is_some());
        assert!(state.tasks.blocking_read().get("task-1").is_some());
        assert_eq!(state.epoch_gen.next(), 8);
    }

    #[test]
    fn from_snapshot_marks_inflight_runs_reconciling() {
        let now = SystemTime::now();
        let snapshot = MetadataSnapshot {
            runs: vec![RunRecord {
                run_id: "run-1".into(),
                pipeline_name: "pipe".into(),
                state: RunState::Assigned,
                created_at: now,
                updated_at: now,
                started_at: None,
                completed_at: None,
                current_task: Some(crate::run_state::CurrentTask {
                    task_id: "task-1".into(),
                    agent_id: "agent-1".into(),
                    attempt: 1,
                    lease_epoch: 11,
                    assigned_at: now,
                }),
                error_message: None,
                attempt: 1,
                idempotency_key: None,
                total_records: 0,
                total_bytes: 0,
                elapsed_seconds: 0.0,
                cursors_advanced: 0,
            }],
            tasks: vec![TaskRecord {
                task_id: "task-1".into(),
                run_id: "run-1".into(),
                attempt: 1,
                lease: Some(crate::lease::Lease::new(
                    11,
                    std::time::Duration::from_secs(60),
                )),
                state: TaskState::Assigned,
                pipeline_yaml: b"pipeline: test".to_vec(),
                dry_run: false,
                limit: None,
                assigned_agent_id: Some("agent-1".into()),
            }],
            agents: vec![],
            previews: vec![],
            max_lease_epoch: 11,
        };

        let state = ControllerState::from_snapshot(b"signing-key", None, snapshot);

        assert_eq!(
            state.runs.blocking_read().get_run("run-1").unwrap().state,
            RunState::Reconciling
        );
        assert_eq!(state.epoch_gen.next(), 12);
    }

    #[test]
    fn from_snapshot_rehydrates_agents_and_previews() {
        let snapshot = MetadataSnapshot {
            runs: vec![],
            tasks: vec![],
            agents: vec![AgentRecord {
                agent_id: "agent-1".into(),
                max_tasks: 2,
                active_tasks: 1,
                flight_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: "hash123".into(),
                last_heartbeat: Instant::now(),
                available_plugins: vec!["source-postgres".into()],
                memory_bytes: 1024,
            }],
            previews: vec![PreviewEntry {
                run_id: "run-1".into(),
                task_id: "task-1".into(),
                flight_endpoint: "localhost:9091".into(),
                ticket: bytes::Bytes::from_static(b"ticket"),
                streams: vec![PreviewStreamEntry {
                    stream: "users".into(),
                    rows: 4,
                    ticket: bytes::Bytes::from_static(b"users-ticket"),
                }],
                created_at: Instant::now(),
                ttl: Duration::from_secs(60),
            }],
            max_lease_epoch: 0,
        };

        let state = ControllerState::from_snapshot(b"signing-key", None, snapshot);

        assert!(state.registry.blocking_read().get("agent-1").is_some());
        let preview = state.previews.blocking_write().get("run-1").cloned();
        assert!(preview.is_some());
        assert_eq!(preview.unwrap().streams[0].stream, "users");
    }
}
