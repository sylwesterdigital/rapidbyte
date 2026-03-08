//! Pipeline error model and retry backoff policy helpers.

use std::time::Duration;

use rapidbyte_types::error::{BackoffClass, PluginError};

const BACKOFF_FAST_BASE_MS: u64 = 100;
const BACKOFF_NORMAL_BASE_MS: u64 = 1_000;
const BACKOFF_SLOW_BASE_MS: u64 = 5_000;
const BACKOFF_MAX_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// PipelineError — categorised errors for retry decisions
// ---------------------------------------------------------------------------

/// Categorized pipeline error for retry decisions.
///
/// `Plugin` wraps a typed `PluginError` with retry metadata
/// (`retryable`, `backoff_class`, `retry_after_ms`, etc.).
///
/// `Infrastructure` wraps opaque host-side errors (WASM load failures,
/// channel errors, state backend issues, etc.) that are never retryable
/// at the plugin level.
#[derive(Debug)]
pub enum PipelineError {
    /// Typed plugin error with retry metadata.
    Plugin(PluginError),
    /// Infrastructure error (WASM load, channel, state backend, etc.)
    Infrastructure(anyhow::Error),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plugin(e) => write!(f, "{e}"),
            Self::Infrastructure(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<anyhow::Error> for PipelineError {
    fn from(e: anyhow::Error) -> Self {
        Self::Infrastructure(e)
    }
}

impl PipelineError {
    /// Returns `true` if this is a typed plugin error that the plugin
    /// has marked as retryable.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Plugin(e) => e.retryable && e.safe_to_retry,
            Self::Infrastructure(_) => false,
        }
    }

    /// Returns the typed plugin error if this is a `Plugin` variant.
    #[must_use]
    pub fn as_plugin_error(&self) -> Option<&PluginError> {
        match self {
            Self::Plugin(e) => Some(e),
            Self::Infrastructure(_) => None,
        }
    }
}

/// Compute retry delay based on error hints and attempt number.
pub(crate) fn compute_backoff(err: &PluginError, attempt: u32) -> Duration {
    // If plugin specified a retry_after, use it
    if let Some(ms) = err.retry_after_ms {
        return Duration::from_millis(ms);
    }

    // Exponential backoff based on backoff_class
    let base_ms: u64 = match err.backoff_class {
        BackoffClass::Fast => BACKOFF_FAST_BASE_MS,
        BackoffClass::Normal => BACKOFF_NORMAL_BASE_MS,
        BackoffClass::Slow => BACKOFF_SLOW_BASE_MS,
    };

    let delay_ms = base_ms.saturating_mul(2u64.pow(attempt.saturating_sub(1)));
    Duration::from_millis(delay_ms.min(BACKOFF_MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::error::ErrorCategory;

    // -----------------------------------------------------------------------
    // PipelineError tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_error_plugin_is_retryable() {
        let err = PipelineError::Plugin(PluginError::transient_network(
            "CONN_RESET",
            "connection reset by peer",
        ));
        assert!(err.is_retryable());
        let ce = err.as_plugin_error().unwrap();
        assert_eq!(ce.category, ErrorCategory::TransientNetwork);
        assert_eq!(ce.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_pipeline_error_plugin_not_retryable() {
        let err = PipelineError::Plugin(PluginError::config("MISSING_HOST", "host is required"));
        assert!(!err.is_retryable());
        let ce = err.as_plugin_error().unwrap();
        assert_eq!(ce.category, ErrorCategory::Config);
    }

    #[test]
    fn test_pipeline_error_infrastructure_not_retryable() {
        let err = PipelineError::Infrastructure(anyhow::anyhow!("WASM module load failed"));
        assert!(!err.is_retryable());
        assert!(err.as_plugin_error().is_none());
    }

    #[test]
    fn test_pipeline_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something went wrong");
        let pe: PipelineError = anyhow_err.into();
        assert!(matches!(pe, PipelineError::Infrastructure(_)));
        assert!(!pe.is_retryable());
    }

    #[test]
    fn test_pipeline_error_display_plugin() {
        let err =
            PipelineError::Plugin(PluginError::rate_limit("TOO_MANY", "slow down", Some(5000)));
        let msg = format!("{err}");
        assert!(msg.contains("rate_limit"));
        assert!(msg.contains("TOO_MANY"));
        assert!(msg.contains("slow down"));
    }

    #[test]
    fn test_pipeline_error_display_infrastructure() {
        let err = PipelineError::Infrastructure(anyhow::anyhow!("Store::new failed"));
        let msg = format!("{err}");
        assert!(msg.contains("Store::new failed"));
    }

    // -----------------------------------------------------------------------
    // compute_backoff tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_backoff_fast() {
        let mut err = PluginError::transient_network("X", "y");
        err.backoff_class = BackoffClass::Fast;
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(100));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(200));
        assert_eq!(compute_backoff(&err, 3), Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_normal() {
        let err = PluginError::transient_network("X", "y");
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(1000));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(2000));
    }

    #[test]
    fn test_backoff_slow() {
        let err = PluginError::rate_limit("X", "y", None);
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(5000));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(10000));
    }

    #[test]
    fn test_backoff_respects_retry_after() {
        let err = PluginError::rate_limit("X", "y", Some(7500));
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(7500));
        assert_eq!(compute_backoff(&err, 5), Duration::from_millis(7500));
    }

    #[test]
    fn test_backoff_capped_at_60s() {
        let err = PluginError::transient_db("X", "y");
        assert_eq!(compute_backoff(&err, 20), Duration::from_millis(60_000));
    }

    // -----------------------------------------------------------------------
    // commit_state tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_error_commit_state_extraction() {
        use rapidbyte_types::error::CommitState;

        let err = PipelineError::Plugin(
            PluginError::transient_db("COMMIT_FAILED", "timeout")
                .with_commit_state(CommitState::AfterCommitUnknown),
        );
        let ce = err.as_plugin_error().unwrap();
        assert_eq!(ce.commit_state, Some(CommitState::AfterCommitUnknown));
    }

    #[test]
    fn test_pipeline_error_commit_unknown_is_not_retryable_when_unsafe() {
        use rapidbyte_types::error::CommitState;

        let err = PipelineError::Plugin(
            PluginError::transient_db("COMMIT_FAILED", "timeout")
                .with_commit_state(CommitState::AfterCommitUnknown),
        );
        assert!(!err.is_retryable());
        let ce = err.as_plugin_error().unwrap();
        assert_eq!(ce.commit_state, Some(CommitState::AfterCommitUnknown));
        assert!(!ce.safe_to_retry);
    }

    #[test]
    fn test_pipeline_error_unsafe_plugin_error_is_not_retryable() {
        use rapidbyte_types::error::CommitState;

        let err = PipelineError::Plugin(
            PluginError::transient_db("WRITE_FAILED", "partial commit")
                .with_commit_state(CommitState::AfterCommitConfirmed),
        );
        assert!(!err.is_retryable());
    }
}
