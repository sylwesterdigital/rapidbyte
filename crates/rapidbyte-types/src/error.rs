//! Structured error model for plugin operations.
//!
//! [`PluginError`] carries classification, retry metadata, and optional
//! diagnostic details. Construct via category-specific factory methods.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Broad classification of a plugin error.
///
/// Determines default retry behavior and operator-facing categorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Invalid plugin configuration.
    Config,
    /// Authentication failure.
    Auth,
    /// Insufficient permissions.
    Permission,
    /// Rate limit exceeded (retryable).
    RateLimit,
    /// Transient network error (retryable).
    TransientNetwork,
    /// Transient database error (retryable).
    TransientDb,
    /// Invalid or corrupt data.
    Data,
    /// Schema mismatch or incompatibility.
    Schema,
    /// Internal plugin error.
    Internal,
    /// Frame lifecycle error (alloc/write/seal/read).
    Frame,
}

impl ErrorCategory {
    /// Default error scope for this category.
    #[must_use]
    pub fn default_scope(self) -> ErrorScope {
        match self {
            Self::Data => ErrorScope::Record,
            Self::Frame => ErrorScope::Batch,
            _ => ErrorScope::Stream,
        }
    }

    /// Whether errors of this category are retryable by default.
    #[must_use]
    pub fn default_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimit | Self::TransientNetwork | Self::TransientDb
        )
    }

    /// Default backoff class for this category.
    #[must_use]
    pub fn default_backoff(self) -> BackoffClass {
        match self {
            Self::RateLimit => BackoffClass::Slow,
            _ => BackoffClass::Normal,
        }
    }

    /// Wire-format string for storage and display.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Auth => "auth",
            Self::Permission => "permission",
            Self::RateLimit => "rate_limit",
            Self::TransientNetwork => "transient_network",
            Self::TransientDb => "transient_db",
            Self::Data => "data",
            Self::Schema => "schema",
            Self::Internal => "internal",
            Self::Frame => "frame",
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ErrorCategory {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "config" => Ok(Self::Config),
            "auth" => Ok(Self::Auth),
            "permission" => Ok(Self::Permission),
            "rate_limit" => Ok(Self::RateLimit),
            "transient_network" => Ok(Self::TransientNetwork),
            "transient_db" => Ok(Self::TransientDb),
            "data" => Ok(Self::Data),
            "schema" => Ok(Self::Schema),
            "internal" => Ok(Self::Internal),
            "frame" => Ok(Self::Frame),
            _ => Err(()),
        }
    }
}

/// Blast radius of an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorScope {
    /// Affects the entire stream.
    Stream,
    /// Affects a single batch.
    Batch,
    /// Affects an individual record.
    Record,
}

impl fmt::Display for ErrorScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Stream => "stream",
            Self::Batch => "batch",
            Self::Record => "record",
        };
        f.write_str(s)
    }
}

/// Retry backoff strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffClass {
    /// Millisecond-scale retry.
    Fast,
    /// Second-scale retry.
    Normal,
    /// Minute-scale retry.
    Slow,
}

/// Transaction commit state at the time of error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitState {
    /// Error occurred before any commit attempt.
    BeforeCommit,
    /// Commit was attempted but outcome is unknown.
    AfterCommitUnknown,
    /// Commit was confirmed successful before the error.
    AfterCommitConfirmed,
}

/// Validation check outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Success,
    Failed,
    Warning,
}

/// Result of a plugin validation check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    pub status: ValidationStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Structured error from a plugin operation.
///
/// Carries classification, retry metadata, and optional diagnostic details.
/// Construct via category-specific factory methods (e.g., [`PluginError::config`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("[{category}] {code}: {message}")]
pub struct PluginError {
    pub category: ErrorCategory,
    pub scope: ErrorScope,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub backoff_class: BackoffClass,
    pub safe_to_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_state: Option<CommitState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl PluginError {
    /// Build an error using the category's default scope, retryable, and backoff.
    fn from_category(
        category: ErrorCategory,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let retryable = category.default_retryable();
        Self {
            category,
            scope: category.default_scope(),
            code: code.into(),
            message: message.into(),
            retryable,
            retry_after_ms: None,
            backoff_class: category.default_backoff(),
            safe_to_retry: retryable,
            commit_state: None,
            details: None,
        }
    }

    /// Configuration error (not retryable).
    #[must_use]
    pub fn config(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Config, code, message)
    }

    /// Authentication error (not retryable).
    #[must_use]
    pub fn auth(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Auth, code, message)
    }

    /// Permission error (not retryable).
    #[must_use]
    pub fn permission(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Permission, code, message)
    }

    /// Rate limit error (retryable, slow backoff).
    #[must_use]
    pub fn rate_limit(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after_ms: Option<u64>,
    ) -> Self {
        let mut err = Self::from_category(ErrorCategory::RateLimit, code, message);
        err.retry_after_ms = retry_after_ms;
        err
    }

    /// Transient network error (retryable, normal backoff).
    #[must_use]
    pub fn transient_network(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::TransientNetwork, code, message)
    }

    /// Transient database error (retryable, normal backoff).
    #[must_use]
    pub fn transient_db(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::TransientDb, code, message)
    }

    /// Data validation error (not retryable, record scope).
    #[must_use]
    pub fn data(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Data, code, message)
    }

    /// Schema mismatch error (not retryable).
    #[must_use]
    pub fn schema(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Schema, code, message)
    }

    /// Internal plugin error (not retryable).
    #[must_use]
    pub fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Internal, code, message)
    }

    /// Frame lifecycle error (not retryable).
    #[must_use]
    pub fn frame(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_category(ErrorCategory::Frame, code, message)
    }

    /// Attach structured diagnostic details.
    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Record transaction commit state at time of error.
    ///
    /// Setting any post-commit state also sets `safe_to_retry = false`.
    #[must_use]
    pub fn with_commit_state(mut self, state: CommitState) -> Self {
        if matches!(
            state,
            CommitState::AfterCommitUnknown | CommitState::AfterCommitConfirmed
        ) {
            self.safe_to_retry = false;
        }
        self.commit_state = Some(state);
        self
    }

    /// Override the default error scope.
    #[must_use]
    pub fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = scope;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_defaults() {
        let err = PluginError::config("MISSING_HOST", "host is required");
        assert_eq!(err.category, ErrorCategory::Config);
        assert_eq!(err.scope, ErrorScope::Stream);
        assert!(!err.retryable);
        assert!(!err.safe_to_retry);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn transient_errors_are_retryable() {
        let net = PluginError::transient_network("TIMEOUT", "timed out");
        assert!(net.retryable);
        assert!(net.safe_to_retry);

        let db = PluginError::transient_db("DEADLOCK", "deadlock");
        assert!(db.retryable);
        assert!(db.safe_to_retry);
    }

    #[test]
    fn after_commit_unknown_disables_safe_retry() {
        let err = PluginError::transient_db("UNKNOWN", "commit unknown")
            .with_commit_state(CommitState::AfterCommitUnknown);
        assert!(err.retryable);
        assert!(!err.safe_to_retry);
    }

    #[test]
    fn after_commit_confirmed_disables_safe_retry() {
        let err = PluginError::transient_db("PARTIAL", "partial commit")
            .with_commit_state(CommitState::AfterCommitConfirmed);
        assert!(err.retryable);
        assert!(!err.safe_to_retry);
    }

    #[test]
    fn serde_roundtrip() {
        let err = PluginError::rate_limit("THROTTLED", "slow down", Some(5000))
            .with_details(serde_json::json!({"endpoint": "/api/data"}));
        let json = serde_json::to_string(&err).unwrap();
        let back: PluginError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn display_format() {
        let err = PluginError::config("BAD_PORT", "port must be positive");
        assert_eq!(err.to_string(), "[config] BAD_PORT: port must be positive");
    }

    #[test]
    fn error_category_from_str_roundtrips_with_as_str() {
        let categories = [
            ErrorCategory::Config,
            ErrorCategory::Auth,
            ErrorCategory::Permission,
            ErrorCategory::RateLimit,
            ErrorCategory::TransientNetwork,
            ErrorCategory::TransientDb,
            ErrorCategory::Data,
            ErrorCategory::Schema,
            ErrorCategory::Internal,
            ErrorCategory::Frame,
        ];
        for cat in categories {
            assert_eq!(cat.as_str().parse::<ErrorCategory>(), Ok(cat));
        }
    }

    #[test]
    fn error_category_from_str_unknown_returns_err() {
        assert!("unknown_variant".parse::<ErrorCategory>().is_err());
    }
}
