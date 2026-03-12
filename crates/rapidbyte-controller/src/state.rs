//! Shared controller state accessed by gRPC service handlers.

use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

use crate::lease::EpochGenerator;
use crate::preview::{PreviewStore, TicketSigner};
use crate::registry::AgentRegistry;
use crate::run_state::RunStore;
use crate::scheduler::TaskQueue;
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
    /// Notified when a new task is enqueued, waking long-poll waiters.
    pub task_notify: Arc<Notify>,
}

impl ControllerState {
    #[must_use]
    pub fn new(signing_key: &[u8]) -> Self {
        Self {
            runs: Arc::new(RwLock::new(RunStore::new())),
            tasks: Arc::new(RwLock::new(TaskQueue::new())),
            registry: Arc::new(RwLock::new(AgentRegistry::new())),
            watchers: Arc::new(RwLock::new(RunWatchers::new())),
            previews: Arc::new(RwLock::new(PreviewStore::new())),
            epoch_gen: Arc::new(EpochGenerator::new()),
            ticket_signer: Arc::new(TicketSigner::new(signing_key)),
            task_notify: Arc::new(Notify::new()),
        }
    }
}
