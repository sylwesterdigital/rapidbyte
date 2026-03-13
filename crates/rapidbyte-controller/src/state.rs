//! Shared controller state accessed by gRPC service handlers.

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use tokio::sync::{Notify, RwLock};

use crate::lease::EpochGenerator;
use crate::preview::{PreviewStore, TicketSigner};
use crate::registry::AgentRegistry;
use crate::run_state::RunStore;
use crate::scheduler::TaskQueue;
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
        let mut runs = RunStore::new();
        for run in snapshot.runs {
            runs.restore_run(run);
        }

        let mut tasks = TaskQueue::new();
        for task in snapshot.tasks {
            tasks.restore_task(task);
        }

        Self {
            runs: Arc::new(RwLock::new(runs)),
            tasks: Arc::new(RwLock::new(tasks)),
            registry: Arc::new(RwLock::new(AgentRegistry::new())),
            watchers: Arc::new(RwLock::new(RunWatchers::new())),
            previews: Arc::new(RwLock::new(PreviewStore::new())),
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
}

fn preview_created_at(created_at: Instant) -> SystemTime {
    SystemTime::now()
        .checked_sub(created_at.elapsed())
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_state::{RunRecord, RunState};
    use crate::scheduler::{TaskRecord, TaskState};

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
            max_lease_epoch: 7,
        };

        let state = ControllerState::from_snapshot(b"signing-key", None, snapshot);

        assert!(state.runs.blocking_read().get_run("run-1").is_some());
        assert!(state.tasks.blocking_read().get("task-1").is_some());
        assert_eq!(state.epoch_gen.next(), 8);
    }
}
