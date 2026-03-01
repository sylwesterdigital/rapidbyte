//! State backend error types.

/// Errors produced by [`StateBackend`](crate::StateBackend) operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Underlying storage backend failure (`SQLite`, `Postgres`, etc.).
    #[error("state backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),

    /// Backend failure with operation context (preserves error source chain).
    #[error("{context}: {source}")]
    BackendContext {
        context: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// File-system I/O failure (e.g. creating the database directory).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal mutex was poisoned by a panicked thread.
    #[error("state backend lock poisoned")]
    LockPoisoned,
}

impl StateError {
    /// Wrap any backend-specific error into a [`StateError::Backend`].
    #[must_use]
    pub fn backend(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(err))
    }

    /// Wrap backend errors with operation context, preserving the error chain.
    #[must_use]
    pub fn backend_context(
        context: &str,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::BackendContext {
            context: context.to_string(),
            source: Box::new(err),
        }
    }
}

/// Convenience alias used throughout this crate.
pub type Result<T> = std::result::Result<T, StateError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_error_displays_inner() {
        let inner = std::io::Error::other("db broke");
        let err = StateError::backend(inner);
        assert!(err.to_string().contains("db broke"));
    }

    #[test]
    fn lock_poisoned_displays() {
        let err = StateError::LockPoisoned;
        assert_eq!(err.to_string(), "state backend lock poisoned");
    }

    #[test]
    fn io_error_wraps() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err = StateError::Io(inner);
        assert!(err.to_string().contains("i/o"));
    }

    #[test]
    fn backend_helper_boxes_error() {
        let inner = std::io::Error::other("test");
        let err = StateError::backend(inner);
        assert!(matches!(err, StateError::Backend(_)));
    }

    #[test]
    fn backend_context_includes_operation_name() {
        let err = StateError::backend_context("insert_dlq_records", std::io::Error::other("boom"));
        assert!(err.to_string().contains("insert_dlq_records"));
    }
}
