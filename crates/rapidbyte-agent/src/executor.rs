//! Task execution wrapper around `engine::run_pipeline`.

use std::future::Future;
use std::pin::Pin;

use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use rapidbyte_engine::progress::ProgressEvent;
use rapidbyte_engine::{orchestrator, PipelineError};
use rapidbyte_types::prelude::CommitState;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

type PipelineRunFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PipelineOutcome, PipelineError>> + Send + 'a>>;

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
#[allow(clippy::too_many_lines)]
pub async fn execute_task(
    pipeline_yaml: &[u8],
    dry_run: bool,
    limit: Option<u64>,
    progress_tx: Option<mpsc::UnboundedSender<ProgressEvent>>,
    cancel_token: CancellationToken,
) -> TaskExecutionResult {
    execute_task_with_runner(
        pipeline_yaml,
        dry_run,
        limit,
        progress_tx,
        cancel_token,
        |config, options, progress_tx, cancel_token| {
            Box::pin(orchestrator::run_pipeline(
                config,
                options,
                progress_tx,
                cancel_token,
            ))
        },
    )
    .await
}

async fn execute_task_with_runner<R>(
    pipeline_yaml: &[u8],
    dry_run: bool,
    limit: Option<u64>,
    progress_tx: Option<mpsc::UnboundedSender<ProgressEvent>>,
    cancel_token: CancellationToken,
    run_pipeline: R,
) -> TaskExecutionResult
where
    R: for<'a> FnOnce(
        &'a rapidbyte_engine::config::types::PipelineConfig,
        &'a ExecutionOptions,
        Option<mpsc::UnboundedSender<ProgressEvent>>,
        CancellationToken,
    ) -> PipelineRunFuture<'a>,
{
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

    // Cancellation is only honored before execution begins. Once the engine
    // starts, we wait for its real terminal outcome so commit-state metadata
    // is preserved instead of fabricating a clean cancellation.
    if cancel_token.is_cancelled() {
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

    let pipeline_result = run_pipeline(&config, &options, progress_tx, cancel_token.clone()).await;

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
                    commit_state: pe.commit_state.map_or_else(
                        || "before_commit".into(),
                        |cs| {
                            match cs {
                                CommitState::BeforeCommit => "before_commit",
                                CommitState::AfterCommitUnknown => "after_commit_unknown",
                                CommitState::AfterCommitConfirmed => "after_commit_confirmed",
                            }
                            .into()
                        },
                    ),
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
    use rapidbyte_engine::execution::PipelineOutcome;
    use rapidbyte_engine::result::{DestTiming, PipelineCounts, PipelineResult, SourceTiming};
    use rapidbyte_engine::PipelineError;
    use rapidbyte_types::error::{CommitState, PluginError};
    use std::sync::Arc;
    use tokio::sync::Notify;

    fn valid_yaml() -> &'static [u8] {
        br#"
version: "1.0"
pipeline: test_pipeline
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#
    }

    fn completed_outcome() -> PipelineOutcome {
        PipelineOutcome::Run(PipelineResult {
            counts: PipelineCounts {
                records_written: 7,
                bytes_written: 128,
                ..PipelineCounts::default()
            },
            source: SourceTiming::default(),
            dest: DestTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            transform_module_load_ms: Vec::new(),
            duration_secs: 0.5,
            wasm_overhead_secs: 0.0,
            retry_count: 0,
            parallelism: 1,
            stream_metrics: Vec::new(),
        })
    }

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

    #[tokio::test]
    async fn test_cancellation_after_start_waits_for_pipeline_outcome() {
        let token = CancellationToken::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let handle = {
            let token = token.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                execute_task_with_runner(
                    valid_yaml(),
                    false,
                    None,
                    None,
                    token,
                    move |_, _, _, _cancel_token| {
                        let started = started.clone();
                        let release = release.clone();
                        Box::pin(async move {
                            started.notify_one();
                            release.notified().await;
                            Ok(completed_outcome())
                        })
                    },
                )
                .await
            })
        };

        started.notified().await;
        token.cancel();
        release.notify_one();

        let result = handle.await.unwrap();
        assert!(matches!(result.outcome, TaskOutcomeKind::Completed));
        assert_eq!(result.metrics.records_processed, 7);
        assert_eq!(result.metrics.bytes_processed, 128);
    }

    #[tokio::test]
    async fn cancellation_before_destination_write_returns_cancelled_failure() {
        let token = CancellationToken::new();
        let started = Arc::new(Notify::new());

        let handle = {
            let token = token.clone();
            let started = started.clone();
            tokio::spawn(async move {
                execute_task_with_runner(
                    valid_yaml(),
                    false,
                    None,
                    None,
                    token,
                    move |_, _, _, cancel_token| {
                        let started = started.clone();
                        Box::pin(async move {
                            let mut cancelled = PluginError::internal(
                                "CANCELLED",
                                "Pipeline cancelled before destination write",
                            );
                            cancelled.safe_to_retry = true;
                            started.notify_one();
                            cancel_token.cancelled().await;
                            Err(PipelineError::Plugin(
                                cancelled.with_commit_state(CommitState::BeforeCommit),
                            ))
                        })
                    },
                )
                .await
            })
        };

        started.notified().await;
        token.cancel();

        let result = handle.await.unwrap();
        match result.outcome {
            TaskOutcomeKind::Failed(info) => {
                assert_eq!(info.code, "CANCELLED");
                assert!(info.safe_to_retry);
                assert_eq!(info.commit_state, "before_commit");
            }
            _ => panic!("expected cancelled failure outcome"),
        }
    }

    #[tokio::test]
    async fn cancellation_after_destination_start_preserves_real_outcome() {
        let token = CancellationToken::new();
        let started_destination = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let handle = {
            let token = token.clone();
            let started_destination = started_destination.clone();
            let release = release.clone();
            tokio::spawn(async move {
                execute_task_with_runner(
                    valid_yaml(),
                    false,
                    None,
                    None,
                    token,
                    move |_, _, _, _cancel_token| {
                        let started_destination = started_destination.clone();
                        let release = release.clone();
                        Box::pin(async move {
                            started_destination.notify_one();
                            release.notified().await;
                            Err(PipelineError::Plugin(
                                PluginError::transient_db(
                                    "WRITE_FAILED",
                                    "destination write failed after cancellation",
                                )
                                .with_commit_state(CommitState::AfterCommitUnknown),
                            ))
                        })
                    },
                )
                .await
            })
        };

        started_destination.notified().await;
        token.cancel();
        release.notify_one();

        let result = handle.await.unwrap();
        match result.outcome {
            TaskOutcomeKind::Failed(info) => {
                assert_eq!(info.code, "WRITE_FAILED");
                assert!(!info.safe_to_retry);
                assert_eq!(info.commit_state, "after_commit_unknown");
            }
            _ => panic!("expected real post-commit failure outcome"),
        }
    }
}
