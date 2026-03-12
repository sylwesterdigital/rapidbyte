//! Broadcast channels for WatchRun streaming.

use std::collections::HashMap;

use tokio::sync::broadcast;

use crate::proto::rapidbyte::v1::RunEvent;

const CHANNEL_BUFFER: usize = 256;

/// Manages per-run broadcast channels for streaming events to watchers.
pub struct RunWatchers {
    channels: HashMap<String, broadcast::Sender<RunEvent>>,
}

impl RunWatchers {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    /// Subscribe to events for a run. Creates the channel if it doesn't exist.
    pub fn subscribe(&mut self, run_id: &str) -> broadcast::Receiver<RunEvent> {
        let sender = self
            .channels
            .entry(run_id.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_BUFFER).0);
        sender.subscribe()
    }

    /// Publish an event to all watchers of a run. No-op if no channel exists.
    pub fn publish(&self, run_id: &str, event: RunEvent) {
        if let Some(sender) = self.channels.get(run_id) {
            // Ignore send errors (no active receivers is fine)
            let _ = sender.send(event);
        }
    }

    /// Remove the channel for a run (cleanup after terminal state).
    pub fn remove(&mut self, run_id: &str) {
        self.channels.remove(run_id);
    }
}

impl Default for RunWatchers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rapidbyte::v1::{run_event, ProgressUpdate};

    fn make_progress_event(run_id: &str) -> RunEvent {
        RunEvent {
            run_id: run_id.to_string(),
            event: Some(run_event::Event::Progress(ProgressUpdate {
                stream: "s1".into(),
                phase: "running".into(),
                records: 100,
                bytes: 4096,
            })),
        }
    }

    #[tokio::test]
    async fn subscribe_then_publish_receives_event() {
        let mut watchers = RunWatchers::new();
        let mut rx = watchers.subscribe("r1");
        watchers.publish("r1", make_progress_event("r1"));

        let event = rx.recv().await.unwrap();
        assert_eq!(event.run_id, "r1");
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let mut watchers = RunWatchers::new();
        let mut rx1 = watchers.subscribe("r1");
        let mut rx2 = watchers.subscribe("r1");
        watchers.publish("r1", make_progress_event("r1"));

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.run_id, "r1");
        assert_eq!(e2.run_id, "r1");
    }

    #[test]
    fn publish_to_nonexistent_run_is_noop() {
        let watchers = RunWatchers::new();
        // Should not panic
        watchers.publish("nonexistent", make_progress_event("nonexistent"));
    }

    #[test]
    fn remove_cleans_up() {
        let mut watchers = RunWatchers::new();
        let _rx = watchers.subscribe("r1");
        watchers.remove("r1");
        // Channel is gone, publish is a no-op
        watchers.publish("r1", make_progress_event("r1"));
    }
}
