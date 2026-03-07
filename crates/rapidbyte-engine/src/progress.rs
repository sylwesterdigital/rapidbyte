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
    /// A batch completed flowing through the pipeline.
    BatchCompleted {
        stream: String,
        records: u64,
        bytes: u64,
    },
    /// A stream finished processing.
    StreamCompleted { stream: String },
    /// A non-fatal error occurred.
    Error { message: String },
}
