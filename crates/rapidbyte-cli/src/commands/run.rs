//! Pipeline execution subcommand (run).

// u64/i64/usize → f64 casts are intentional lossy conversions for display and
// timing computations where sub-millisecond precision loss is acceptable.
#![allow(clippy::cast_precision_loss)]

use std::path::Path;

use anyhow::Result;

use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use rapidbyte_engine::orchestrator;

use crate::output::{format::format_bytes, progress, summary};
use crate::Verbosity;

#[derive(Debug, Clone)]
struct ProcessCpuMetrics {
    cpu_secs: f64,
    cpu_pct_one_core: f64,
    cpu_pct_of_available_cores: f64,
    available_cores: usize,
}

/// Execute the `run` command: parse, validate, and run a pipeline.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or execution fails.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;

    // Build execution options (--limit implies --dry-run)
    let dry_run = dry_run || limit.is_some();
    let options = ExecutionOptions { dry_run, limit };

    tracing::info!(
        pipeline = config.pipeline,
        source = config.source.use_ref,
        destination = config.destination.use_ref,
        streams = config.source.streams.len(),
        "Pipeline validated"
    );

    // Spawn progress spinner (unless quiet or dry-run)
    let is_tty = console::Term::stderr().is_term();
    let stream_count = config.source.streams.len() as u64;
    let show_progress = !matches!(verbosity, Verbosity::Quiet) && !dry_run;
    let (progress_tx, spinner_handle) = if show_progress {
        let (tx, handle) = progress::spawn_progress_spinner(stream_count, is_tty);
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Run the pipeline
    let cpu_start = process_cpu_seconds();
    let outcome = orchestrator::run_pipeline(&config, &options, progress_tx).await;
    let cpu_end = process_cpu_seconds();

    // Wait for spinner to finish before printing results
    if let Some(handle) = spinner_handle {
        let _ = handle.await;
    }

    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => return Err(e.into()),
    };

    match outcome {
        PipelineOutcome::Run(result) => {
            let cpu_metrics = process_cpu_metrics(cpu_start, cpu_end, result.duration_secs);
            let peak_rss_mb = process_peak_rss_mb();

            // Human-readable summary to stderr
            summary::print_success(&result, &config.pipeline, verbosity);

            // Process-level diagnostics for -vv
            if verbosity == Verbosity::Diagnostic {
                if let Some(cpu) = &cpu_metrics {
                    eprintln!(
                        "    {:<20}CPU {:.1}s | RSS {:.0} MB",
                        "Process",
                        cpu.cpu_secs,
                        peak_rss_mb.unwrap_or(0.0),
                    );
                }
            }

            // Machine-readable JSON for benchmarking tools (stdout, opt-in)
            if std::env::var_os("RAPIDBYTE_BENCH").is_some() {
                let json = bench_json_from_result(&result, cpu_metrics.as_ref(), peak_rss_mb);
                println!("@@BENCH_JSON@@{json}");
            }
        }
        PipelineOutcome::DryRun(result) => {
            use arrow::util::pretty::pretty_format_batches;

            eprintln!(
                "Dry run: '{}' ({} stream{})",
                config.pipeline,
                result.streams.len(),
                if result.streams.len() == 1 { "" } else { "s" },
            );
            eprintln!();

            for stream in &result.streams {
                eprintln!("Stream: {}", stream.stream_name);

                if stream.batches.is_empty() {
                    eprintln!("  (no data)");
                } else {
                    // Print schema
                    let schema = stream.batches[0].schema();
                    eprintln!("  Columns:");
                    for field in schema.fields() {
                        eprintln!(
                            "    {}: {:?}{}",
                            field.name(),
                            field.data_type(),
                            if field.is_nullable() {
                                " (nullable)"
                            } else {
                                ""
                            }
                        );
                    }
                    eprintln!();

                    // Print table (pretty_format_batches returns a Display impl)
                    match pretty_format_batches(&stream.batches) {
                        Ok(table) => eprintln!("{table}"),
                        Err(e) => eprintln!("  (display error: {e})"),
                    }
                }

                eprintln!(
                    "{} rows ({}, {} batch{})",
                    stream.total_rows,
                    format_bytes(stream.total_bytes),
                    stream.batches.len(),
                    if stream.batches.len() == 1 { "" } else { "es" },
                );
                eprintln!();
            }

            eprintln!("Duration: {:.2}s", result.duration_secs);
            if result.transform_count > 0 {
                eprintln!(
                    "Transforms: {} applied ({:.2}s)",
                    result.transform_count, result.transform_duration_secs,
                );
            }
        }
    }

    Ok(())
}

fn process_cpu_metrics(
    cpu_start_secs: Option<f64>,
    cpu_end_secs: Option<f64>,
    wall_duration_secs: f64,
) -> Option<ProcessCpuMetrics> {
    let start = cpu_start_secs?;
    let end = cpu_end_secs?;
    if wall_duration_secs <= 0.0 || end < start {
        return None;
    }

    let cpu_secs = end - start;
    let cpu_pct_one_core = (cpu_secs / wall_duration_secs) * 100.0;
    let available_cores = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let cpu_pct_of_available_cores = cpu_pct_one_core / available_cores as f64;

    Some(ProcessCpuMetrics {
        cpu_secs,
        cpu_pct_one_core,
        cpu_pct_of_available_cores,
        available_cores,
    })
}

/// Single `getrusage(RUSAGE_SELF)` wrapper to avoid duplicate unsafe blocks.
#[cfg(unix)]
fn getrusage_self() -> Option<libc::rusage> {
    // Safety: getrusage writes into the provided `rusage` struct.
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // Safety: pointer is valid for writes and initialized by successful call.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // Safety: call succeeded and initialized `usage`.
    Some(unsafe { usage.assume_init() })
}

#[cfg(unix)]
#[allow(clippy::cast_precision_loss)]
fn cpu_seconds_from_rusage(usage: &libc::rusage) -> f64 {
    let user_secs = usage.ru_utime.tv_sec as f64 + (usage.ru_utime.tv_usec as f64 / 1e6);
    let sys_secs = usage.ru_stime.tv_sec as f64 + (usage.ru_stime.tv_usec as f64 / 1e6);
    user_secs + sys_secs
}

fn process_cpu_seconds() -> Option<f64> {
    #[cfg(unix)]
    {
        getrusage_self().map(|u| cpu_seconds_from_rusage(&u))
    }

    #[cfg(not(unix))]
    {
        None
    }
}

fn process_peak_rss_mb() -> Option<f64> {
    #[cfg(unix)]
    {
        getrusage_self().map(|u| {
            #[cfg(target_os = "macos")]
            {
                u.ru_maxrss as f64 / (1024.0 * 1024.0)
            }
            #[cfg(not(target_os = "macos"))]
            {
                u.ru_maxrss as f64 / 1024.0
            }
        })
    }

    #[cfg(not(unix))]
    {
        None
    }
}

fn bench_json_from_result(
    result: &rapidbyte_engine::result::PipelineResult,
    cpu_metrics: Option<&ProcessCpuMetrics>,
    peak_rss_mb: Option<f64>,
) -> serde_json::Value {
    let counts = &result.counts;
    let source = &result.source;
    let dest = &result.dest;

    let stream_metrics: Vec<_> = result
        .stream_metrics
        .iter()
        .map(|m| {
            serde_json::json!({
                "stream_name": m.stream_name,
                "partition_index": m.partition_index,
                "partition_count": m.partition_count,
                "records_read": m.records_read,
                "records_written": m.records_written,
                "bytes_read": m.bytes_read,
                "bytes_written": m.bytes_written,
                "source_duration_secs": m.source_duration_secs,
                "dest_duration_secs": m.dest_duration_secs,
                "dest_vm_setup_secs": m.dest_vm_setup_secs,
                "dest_recv_secs": m.dest_recv_secs,
            })
        })
        .collect();

    let partitioned = || {
        result
            .stream_metrics
            .iter()
            .filter(|m| m.partition_count.unwrap_or(1) > 1)
    };
    let worker_records_min = partitioned().map(|m| m.records_written).min();
    let worker_records_max = partitioned().map(|m| m.records_written).max();
    let worker_records_skew_ratio = worker_records_min
        .zip(worker_records_max)
        .and_then(|(min, max)| (max > 0).then_some(min as f64 / max as f64));

    let worker_dest_total_secs: f64 = partitioned().map(|m| m.dest_duration_secs).sum();
    let worker_dest_recv_secs: f64 = partitioned().map(|m| m.dest_recv_secs).sum();
    let worker_dest_vm_setup_secs: f64 = partitioned().map(|m| m.dest_vm_setup_secs).sum();
    let worker_dest_active_secs =
        (worker_dest_total_secs - worker_dest_recv_secs - worker_dest_vm_setup_secs).max(0.0);

    serde_json::json!({
        "records_read": counts.records_read,
        "records_written": counts.records_written,
        "bytes_read": counts.bytes_read,
        "bytes_written": counts.bytes_written,
        "duration_secs": result.duration_secs,
        "source_duration_secs": source.duration_secs,
        "dest_duration_secs": dest.duration_secs,
        "dest_connect_secs": dest.connect_secs,
        "dest_flush_secs": dest.flush_secs,
        "dest_commit_secs": dest.commit_secs,
        "dest_vm_setup_secs": dest.vm_setup_secs,
        "dest_recv_secs": dest.recv_secs,
        "wasm_overhead_secs": result.wasm_overhead_secs,
        "source_connect_secs": source.connect_secs,
        "source_query_secs": source.query_secs,
        "source_fetch_secs": source.fetch_secs,
        "source_arrow_encode_secs": source.arrow_encode_secs,
        "dest_arrow_decode_secs": dest.arrow_decode_secs,
        "source_module_load_ms": source.module_load_ms,
        "dest_module_load_ms": dest.module_load_ms,
        "source_emit_nanos": source.emit_nanos,
        "source_compress_nanos": source.compress_nanos,
        "source_emit_count": source.emit_count,
        "dest_recv_nanos": dest.recv_nanos,
        "dest_recv_wait_nanos": dest.recv_wait_nanos,
        "dest_recv_process_nanos": dest.recv_process_nanos,
        "dest_decompress_nanos": dest.decompress_nanos,
        "dest_recv_count": dest.recv_count,
        "transform_count": result.transform_count,
        "transform_duration_secs": result.transform_duration_secs,
        "transform_module_load_ms": result.transform_module_load_ms,
        "retry_count": result.retry_count,
        "parallelism": result.parallelism,
        "stream_metrics": stream_metrics,
        "worker_records_min": worker_records_min,
        "worker_records_max": worker_records_max,
        "worker_records_skew_ratio": worker_records_skew_ratio,
        "worker_dest_total_secs": worker_dest_total_secs,
        "worker_dest_recv_secs": worker_dest_recv_secs,
        "worker_dest_vm_setup_secs": worker_dest_vm_setup_secs,
        "worker_dest_active_secs": worker_dest_active_secs,
        "process_cpu_secs": cpu_metrics.map(|m| m.cpu_secs),
        "process_cpu_pct_one_core": cpu_metrics.map(|m| m.cpu_pct_one_core),
        "process_cpu_pct_available_cores": cpu_metrics.map(|m| m.cpu_pct_of_available_cores),
        "available_cores": cpu_metrics.map(|m| m.available_cores),
        "process_peak_rss_mb": peak_rss_mb,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_engine::result::{
        DestTiming, PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric,
    };

    #[test]
    fn bench_json_includes_stream_shard_metrics() {
        let result = PipelineResult {
            counts: PipelineCounts {
                records_read: 10,
                records_written: 10,
                bytes_read: 100,
                bytes_written: 100,
            },
            source: SourceTiming::default(),
            dest: DestTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            transform_module_load_ms: vec![],
            duration_secs: 1.0,
            wasm_overhead_secs: 0.0,
            retry_count: 0,
            parallelism: 4,
            stream_metrics: vec![StreamShardMetric {
                stream_name: "bench_events".to_string(),
                partition_index: Some(1),
                partition_count: Some(4),
                records_read: 3,
                records_written: 3,
                bytes_read: 30,
                bytes_written: 30,
                source_duration_secs: 0.4,
                dest_duration_secs: 0.6,
                dest_vm_setup_secs: 0.1,
                dest_recv_secs: 0.2,
            }],
        };

        let cpu = ProcessCpuMetrics {
            cpu_secs: 1.2,
            cpu_pct_one_core: 120.0,
            cpu_pct_of_available_cores: 30.0,
            available_cores: 4,
        };
        let json = bench_json_from_result(&result, Some(&cpu), Some(64.0));
        assert!(json["stream_metrics"].is_array());
        assert_eq!(json["stream_metrics"][0]["partition_index"], 1);
        assert_eq!(json["stream_metrics"][0]["partition_count"], 4);
        assert_eq!(json["stream_metrics"][0]["records_read"], 3);
        assert_eq!(json["stream_metrics"][0]["dest_vm_setup_secs"], 0.1);
        assert_eq!(json["stream_metrics"][0]["dest_recv_secs"], 0.2);
        assert_eq!(json["parallelism"], 4);
        assert_eq!(json["process_cpu_secs"], 1.2);
        assert_eq!(json["available_cores"], 4);
        assert_eq!(json["process_cpu_pct_available_cores"], 30.0);
        assert_eq!(json["process_peak_rss_mb"], 64.0);
    }
}
