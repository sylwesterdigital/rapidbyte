//! Lease epoch generation, validation, and expiry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Monotonically increasing epoch generator.
pub struct EpochGenerator {
    counter: AtomicU64,
}

impl EpochGenerator {
    #[must_use]
    pub fn new() -> Self {
        Self::with_start(0)
    }

    #[must_use]
    pub fn with_start(last_issued_epoch: u64) -> Self {
        Self {
            counter: AtomicU64::new(last_issued_epoch),
        }
    }

    /// Generate the next epoch value (starts at 1, never returns 0).
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

impl Default for EpochGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// A lease with an epoch and expiry time.
#[derive(Debug, Clone)]
pub struct Lease {
    pub epoch: u64,
    pub expires_at: Instant,
}

impl Lease {
    #[must_use]
    pub fn new(epoch: u64, ttl: Duration) -> Self {
        Self {
            epoch,
            expires_at: Instant::now() + ttl,
        }
    }

    /// Check if this lease is valid for the given epoch and has not expired.
    #[must_use]
    pub fn is_valid(&self, epoch: u64) -> bool {
        self.epoch == epoch && Instant::now() < self.expires_at
    }

    /// Check if this lease has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Extend the lease by the given duration from now.
    pub fn renew(&mut self, ttl: Duration) {
        self.expires_at = Instant::now() + ttl;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn epoch_generator_starts_at_one() {
        let gen = EpochGenerator::new();
        assert_eq!(gen.next(), 1);
    }

    #[test]
    fn epoch_generator_increments() {
        let gen = EpochGenerator::new();
        assert_eq!(gen.next(), 1);
        assert_eq!(gen.next(), 2);
        assert_eq!(gen.next(), 3);
    }

    #[test]
    fn lease_is_valid_before_expiry() {
        let lease = Lease::new(1, Duration::from_secs(60));
        assert!(lease.is_valid(1));
    }

    #[test]
    fn lease_rejects_stale_epoch() {
        let lease = Lease::new(2, Duration::from_secs(60));
        assert!(!lease.is_valid(1));
    }

    #[test]
    fn lease_rejects_after_expiry() {
        let lease = Lease::new(1, Duration::from_secs(0));
        std::thread::sleep(Duration::from_millis(10));
        assert!(!lease.is_valid(1));
    }

    #[test]
    fn lease_renew_extends_expiry() {
        let mut lease = Lease::new(1, Duration::from_secs(0));
        lease.renew(Duration::from_secs(60));
        assert!(lease.is_valid(1));
    }
}
