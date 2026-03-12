//! Run state machine with attempt tracking and idempotency dedup.

use std::collections::HashMap;
use std::time::Instant;

use thiserror::Error;

/// Run lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Pending,
    Assigned,
    Running,
    PreviewReady,
    Completed,
    Failed,
    Cancelling,
    Cancelled,
    TimedOut,
}

impl RunState {
    /// Whether this state is terminal (no further transitions allowed except internal).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

#[derive(Debug, Error)]
#[error("invalid transition from {from:?} to {to:?}")]
pub struct InvalidTransition {
    pub from: RunState,
    pub to: RunState,
}

/// Record for a single pipeline run.
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: String,
    pub pipeline_name: String,
    pub state: RunState,
    pub created_at: Instant,
    pub updated_at: Instant,
    pub current_task_id: Option<String>,
    pub error_message: Option<String>,
    pub attempt: u32,
    pub idempotency_key: Option<String>,
}

/// In-memory store for run records with idempotency dedup.
pub struct RunStore {
    runs: HashMap<String, RunRecord>,
    idempotency_index: HashMap<String, String>, // key -> run_id
}

impl RunStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            runs: HashMap::new(),
            idempotency_index: HashMap::new(),
        }
    }

    /// Create a new run. If an idempotency key is provided and already exists,
    /// returns the existing `run_id` instead.
    pub fn create_run(
        &mut self,
        run_id: String,
        pipeline_name: String,
        idempotency_key: Option<String>,
    ) -> String {
        // Check idempotency
        if let Some(key) = &idempotency_key {
            if let Some(existing_id) = self.idempotency_index.get(key) {
                return existing_id.clone();
            }
        }

        let now = Instant::now();
        let record = RunRecord {
            run_id: run_id.clone(),
            pipeline_name,
            state: RunState::Pending,
            created_at: now,
            updated_at: now,
            current_task_id: None,
            error_message: None,
            attempt: 1,
            idempotency_key: idempotency_key.clone(),
        };

        self.runs.insert(run_id.clone(), record);
        if let Some(key) = idempotency_key {
            self.idempotency_index.insert(key, run_id.clone());
        }
        run_id
    }

    #[must_use]
    pub fn get_run(&self, run_id: &str) -> Option<&RunRecord> {
        self.runs.get(run_id)
    }

    pub fn get_run_mut(&mut self, run_id: &str) -> Option<&mut RunRecord> {
        self.runs.get_mut(run_id)
    }

    /// Transition a run to a new state. Returns error if the transition is invalid.
    ///
    /// # Errors
    ///
    /// Returns `InvalidTransition` if the requested state change violates the
    /// run lifecycle (e.g. `Completed -> Running`) or the run does not exist.
    pub fn transition(&mut self, run_id: &str, to: RunState) -> Result<(), InvalidTransition> {
        let record = self.runs.get_mut(run_id).ok_or(InvalidTransition {
            from: RunState::Pending,
            to,
        })?;

        if !is_valid_transition(record.state, to) {
            return Err(InvalidTransition {
                from: record.state,
                to,
            });
        }

        record.state = to;
        record.updated_at = Instant::now();
        Ok(())
    }

    /// List runs, optionally filtered by state.
    #[must_use]
    pub fn list_runs(&self, state_filter: Option<RunState>) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| state_filter.is_none_or(|s| r.state == s))
            .collect()
    }

    /// Find a run by idempotency key.
    #[must_use]
    pub fn find_by_idempotency_key(&self, key: &str) -> Option<&RunRecord> {
        let run_id = self.idempotency_index.get(key)?;
        self.runs.get(run_id)
    }
}

impl Default for RunStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a state transition is valid.
fn is_valid_transition(from: RunState, to: RunState) -> bool {
    matches!(
        (from, to),
        (RunState::Pending, RunState::Assigned | RunState::Cancelled)
            | (RunState::Assigned, RunState::Running)
            | (
                RunState::Running | RunState::PreviewReady,
                RunState::Completed
            )
            | (
                RunState::Running,
                RunState::Failed
                    | RunState::Cancelled
                    | RunState::TimedOut
                    | RunState::PreviewReady
                    | RunState::Cancelling,
            )
            | (RunState::Cancelling, RunState::Cancelled)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_run_starts_pending() {
        let mut store = RunStore::new();
        let id = store.create_run("r1".into(), "pipe".into(), None);
        let run = store.get_run(&id).unwrap();
        assert_eq!(run.state, RunState::Pending);
    }

    #[test]
    fn valid_transition_pending_to_assigned() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        assert!(store.transition("r1", RunState::Assigned).is_ok());
        assert_eq!(store.get_run("r1").unwrap().state, RunState::Assigned);
    }

    #[test]
    fn valid_transition_assigned_to_running() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        assert!(store.transition("r1", RunState::Running).is_ok());
    }

    #[test]
    fn valid_transition_running_to_completed() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        assert!(store.transition("r1", RunState::Completed).is_ok());
    }

    #[test]
    fn valid_transition_running_to_preview_ready() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        assert!(store.transition("r1", RunState::PreviewReady).is_ok());
    }

    #[test]
    fn valid_transition_preview_ready_to_completed() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        store.transition("r1", RunState::PreviewReady).unwrap();
        assert!(store.transition("r1", RunState::Completed).is_ok());
    }

    #[test]
    fn invalid_transition_completed_to_running() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        store.transition("r1", RunState::Completed).unwrap();
        assert!(store.transition("r1", RunState::Running).is_err());
    }

    #[test]
    fn invalid_transition_failed_to_running() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        store.transition("r1", RunState::Failed).unwrap();
        assert!(store.transition("r1", RunState::Running).is_err());
    }

    #[test]
    fn cancelling_reachable_from_running() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();
        store.transition("r1", RunState::Running).unwrap();
        assert!(store.transition("r1", RunState::Cancelling).is_ok());
        assert!(store.transition("r1", RunState::Cancelled).is_ok());
    }

    #[test]
    fn direct_cancel_from_pending() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe".into(), None);
        assert!(store.transition("r1", RunState::Cancelled).is_ok());
    }

    #[test]
    fn list_runs_with_state_filter() {
        let mut store = RunStore::new();
        store.create_run("r1".into(), "pipe1".into(), None);
        store.create_run("r2".into(), "pipe2".into(), None);
        store.transition("r1", RunState::Assigned).unwrap();

        let pending = store.list_runs(Some(RunState::Pending));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].run_id, "r2");

        let all = store.list_runs(None);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn get_run_returns_none_for_unknown() {
        let store = RunStore::new();
        assert!(store.get_run("nonexistent").is_none());
    }

    #[test]
    fn idempotency_key_returns_existing_run() {
        let mut store = RunStore::new();
        let id1 = store.create_run("r1".into(), "pipe".into(), Some("key1".into()));
        let id2 = store.create_run("r2".into(), "pipe".into(), Some("key1".into()));
        assert_eq!(id1, id2);
        assert_eq!(id1, "r1");
    }

    #[test]
    fn different_idempotency_key_creates_new_run() {
        let mut store = RunStore::new();
        let id1 = store.create_run("r1".into(), "pipe".into(), Some("key1".into()));
        let id2 = store.create_run("r2".into(), "pipe".into(), Some("key2".into()));
        assert_ne!(id1, id2);
    }
}
