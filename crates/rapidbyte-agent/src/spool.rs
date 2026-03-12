//! Preview spool — holds dry-run results for Flight replay.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rapidbyte_engine::DryRunResult;

pub struct PreviewSpool {
    entries: HashMap<String, SpoolEntry>,
    default_ttl: Duration,
}

struct SpoolEntry {
    result: DryRunResult,
    created_at: Instant,
    ttl: Duration,
}

impl PreviewSpool {
    #[must_use]
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            default_ttl,
        }
    }

    pub fn store(&mut self, task_id: String, result: DryRunResult) {
        self.entries.insert(
            task_id,
            SpoolEntry {
                result,
                created_at: Instant::now(),
                ttl: self.default_ttl,
            },
        );
    }

    #[must_use]
    pub fn get(&mut self, task_id: &str) -> Option<&DryRunResult> {
        let expired = {
            let entry = self.entries.get(task_id)?;
            entry.created_at.elapsed() >= entry.ttl
        };
        if expired {
            self.entries.remove(task_id);
            return None;
        }

        self.entries.get(task_id).map(|entry| &entry.result)
    }

    pub fn cleanup_expired(&mut self) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, e| e.created_at.elapsed() < e.ttl);
        before - self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_engine::result::SourceTiming;

    fn make_dry_run_result() -> DryRunResult {
        DryRunResult {
            streams: vec![],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        }
    }

    #[test]
    fn store_and_get() {
        let mut spool = PreviewSpool::new(Duration::from_secs(60));
        spool.store("t1".into(), make_dry_run_result());
        assert!(spool.get("t1").is_some());
    }

    #[test]
    fn expired_entries_return_none() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        spool.store("t1".into(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));
        assert!(spool.get("t1").is_none());
    }

    #[test]
    fn expired_get_evicts_entry() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        spool.store("t1".into(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));

        assert!(spool.get("t1").is_none());
        assert_eq!(spool.cleanup_expired(), 0);
    }

    #[test]
    fn cleanup_removes_expired() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        spool.store("t1".into(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));

        // Store a fresh one
        spool.default_ttl = Duration::from_secs(60);
        spool.store("t2".into(), make_dry_run_result());

        let removed = spool.cleanup_expired();
        assert_eq!(removed, 1);
        assert!(spool.get("t1").is_none());
        assert!(spool.get("t2").is_some());
    }

    #[test]
    fn unknown_task_returns_none() {
        let mut spool = PreviewSpool::new(Duration::from_secs(60));
        assert!(spool.get("nonexistent").is_none());
    }
}
