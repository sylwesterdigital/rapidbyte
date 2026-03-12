//! Task execution wrapper around engine::run_pipeline.

use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use rapidbyte_engine::progress::ProgressEvent;
use rapidbyte_engine::{orchestrator, PipelineError};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Result of executing a task on the agent.
pub struct TaskExecutionResult {
    pub outcome: TaskOutcomeKind,
    pub metrics: TaskMetrics,
    pub dry_run_result: Option<rapidbyte_engine::DryRunResult>,
}

pub enum TaskOutcomeKind {
    Completed,
    Failed(TaskErrorInfo),
    Cancelled,
}

pub struct TaskErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub safe_to_retry: bool,
    pub commit_state: String,
}

pub struct TaskMetrics {
    pub records_processed: u64,
    pub bytes_processed: u64,
    pub elapsed_seconds: f64,
    pub cursors_advanced: u64,
}

impl TaskMetrics {
    fn zero() -> Self {
        Self {
            records_processed: 0,
            bytes_processed: 0,
            elapsed_seconds: 0.0,
            cursors_advanced: 0,
        }
    }
}

/// Execute a pipeline task.
///
/// Parses the YAML, runs the pipeline, and returns structured results.
pub async fn execute_task(
    pipeline_yaml: &[u8],
    dry_run: bool,
    limit: Option<u64>,
    progress_tx: Option<mpsc::UnboundedSender<ProgressEvent>>,
    cancel_token: CancellationToken,
) -> TaskExecutionResult {
    // Check for early cancellation before doing any work
    if cancel_token.is_cancelled() {
        return TaskExecutionResult {
            outcome: TaskOutcomeKind::Cancelled,
            metrics: TaskMetrics::zero(),
            dry_run_result: None,
        };
    }

    let yaml_str = match std::str::from_utf8(pipeline_yaml) {
        Ok(s) => s,
        Err(e) => {
            return TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "INVALID_YAML".into(),
                    message: format!("Pipeline YAML is not valid UTF-8: {e}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                metrics: TaskMetrics::zero(),
                dry_run_result: None,
            };
        }
    };

    let config = match parser::parse_pipeline_str(yaml_str) {
        Ok(c) => c,
        Err(e) => {
            return TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "PARSE_FAILED".into(),
                    message: format!("{e:#}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                metrics: TaskMetrics::zero(),
                dry_run_result: None,
            };
        }
    };

    if let Err(e) = validator::validate_pipeline(&config) {
        return TaskExecutionResult {
            outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                code: "VALIDATION_FAILED".into(),
                message: format!("{e:#}"),
                retryable: false,
                safe_to_retry: false,
                commit_state: "before_commit".into(),
            }),
            metrics: TaskMetrics::zero(),
            dry_run_result: None,
        };
    }

    let options = ExecutionOptions { dry_run, limit };
    let start = std::time::Instant::now();

    // Race pipeline execution against cancellation
    let pipeline_result = tokio::select! {
        result = orchestrator::run_pipeline(&config, &options, progress_tx) => result,
        () = cancel_token.cancelled() => {
            let elapsed = start.elapsed().as_secs_f64();
            return TaskExecutionResult {
                outcome: TaskOutcomeKind::Cancelled,
                metrics: TaskMetrics {
                    records_processed: 0,
                    bytes_processed: 0,
                    elapsed_seconds: elapsed,
                    cursors_advanced: 0,
                },
                dry_run_result: None,
            };
        }
    };

    match pipeline_result {
        Ok(outcome) => {
            let elapsed = start.elapsed().as_secs_f64();
            match outcome {
                PipelineOutcome::Run(result) => TaskExecutionResult {
                    outcome: TaskOutcomeKind::Completed,
                    metrics: TaskMetrics {
                        records_processed: result.counts.records_written,
                        bytes_processed: result.counts.bytes_written,
                        elapsed_seconds: elapsed,
                        cursors_advanced: 0,
                    },
                    dry_run_result: None,
                },
                PipelineOutcome::DryRun(dr) => TaskExecutionResult {
                    outcome: TaskOutcomeKind::Completed,
                    metrics: TaskMetrics {
                        records_processed: dr.streams.iter().map(|s| s.total_rows).sum(),
                        bytes_processed: dr.streams.iter().map(|s| s.total_bytes).sum(),
                        elapsed_seconds: elapsed,
                        cursors_advanced: 0,
                    },
                    dry_run_result: Some(dr),
                },
            }
        }
        Err(e) => {
            let elapsed = start.elapsed().as_secs_f64();
            let error_info = match &e {
                PipelineError::Plugin(pe) => TaskErrorInfo {
                    code: pe.code.clone(),
                    message: pe.message.clone(),
                    retryable: pe.retryable,
                    safe_to_retry: pe.safe_to_retry,
                    commit_state: pe
                        .commit_state
                        .map(|cs| format!("{cs:?}").to_lowercase())
                        .unwrap_or_else(|| "before_commit".into()),
                },
                PipelineError::Infrastructure(e) => TaskErrorInfo {
                    code: "INFRASTRUCTURE".into(),
                    message: format!("{e:#}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                },
            };
            TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(error_info),
                metrics: TaskMetrics {
                    records_processed: 0,
                    bytes_processed: 0,
                    elapsed_seconds: elapsed,
                    cursors_advanced: 0,
                },
                dry_run_result: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_utf8_returns_failed() {
        let result = execute_task(&[0xFF, 0xFE], false, None, None, CancellationToken::new()).await;
        assert!(matches!(result.outcome, TaskOutcomeKind::Failed(_)));
        if let TaskOutcomeKind::Failed(info) = &result.outcome {
            assert_eq!(info.code, "INVALID_YAML");
            assert!(!info.retryable);
        }
    }

    #[tokio::test]
    async fn test_invalid_yaml_returns_failed() {
        let result = execute_task(
            b"not: [valid: yaml",
            false,
            None,
            None,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(result.outcome, TaskOutcomeKind::Failed(_)));
        if let TaskOutcomeKind::Failed(info) = &result.outcome {
            assert_eq!(info.code, "PARSE_FAILED");
        }
    }

    #[tokio::test]
    async fn test_zero_metrics_on_early_failure() {
        let result = execute_task(&[0xFF], false, None, None, CancellationToken::new()).await;
        assert_eq!(result.metrics.records_processed, 0);
        assert_eq!(result.metrics.bytes_processed, 0);
        assert_eq!(result.metrics.elapsed_seconds, 0.0);
    }

    #[tokio::test]
    async fn test_pre_cancelled_token_returns_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let result = execute_task(b"pipeline: test\n", false, None, None, token).await;
        assert!(matches!(result.outcome, TaskOutcomeKind::Cancelled));
    }
}
