//! Lightweight progress events emitted during pipeline execution.

/// Execution phase of the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    Resolving,
    Loading,
    Running,
    Finished,
}

/// Progress event sent from engine to CLI during execution.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Pipeline entered a new execution phase.
    PhaseChange { phase: Phase },
    /// A batch was emitted by the source plugin.
    BatchEmitted { bytes: u64 },
    /// A stream finished processing.
    StreamCompleted { stream: String },
    /// A retryable error occurred; pipeline will retry after delay.
    Retry {
        attempt: u32,
        max_retries: u32,
        message: String,
        delay_secs: f64,
    },
}
