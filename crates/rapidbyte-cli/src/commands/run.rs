//! Pipeline execution subcommand (run).

// u64/i64/usize → f64 casts are intentional lossy conversions for display and
// timing computations where sub-millisecond precision loss is acceptable.
#![allow(clippy::cast_precision_loss)]

use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use rapidbyte_engine::orchestrator;

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
#[allow(clippy::too_many_lines)] // Output formatting requires many sequential println calls.
pub async fn execute(pipeline_path: &Path, dry_run: bool, limit: Option<u64>) -> Result<()> {
    // 1. Parse pipeline YAML
    let config = parser::parse_pipeline(pipeline_path)
        .with_context(|| format!("Failed to parse pipeline: {}", pipeline_path.display()))?;

    // 2. Validate
    validator::validate_pipeline(&config)?;

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

    // 3. Run
    let cpu_start = process_cpu_seconds();
    let outcome = orchestrator::run_pipeline(&config, &options).await?;
    let cpu_end = process_cpu_seconds();
    match outcome {
        PipelineOutcome::Run(result) => {
            let counts = &result.counts;
            let source = &result.source;
            let dest = &result.dest;
            let cpu_metrics = process_cpu_metrics(cpu_start, cpu_end, result.duration_secs);
            let peak_rss_mb = process_peak_rss_mb();

            println!("Pipeline '{}' completed successfully.", config.pipeline);
            println!("  Records read:    {}", counts.records_read);
            println!("  Records written: {}", counts.records_written);
            println!("  Bytes read:      {}", format_bytes(counts.bytes_read));
            println!("  Bytes written:   {}", format_bytes(counts.bytes_written));
            if counts.records_read > 0 {
                let avg_row_bytes = counts.bytes_read / counts.records_read;
                println!("  Avg row size:    {avg_row_bytes} B");
            }
            println!("  Duration:        {:.2}s", result.duration_secs);
            println!(
                "  Throughput:      {:.0} rows/sec, {:.2} MB/s",
                counts.records_read as f64 / result.duration_secs,
                counts.bytes_read as f64 / result.duration_secs / 1_048_576.0,
            );
            println!("  Source duration:  {:.2}s", source.duration_secs);
            println!("    Connect:       {:.3}s", source.connect_secs);
            println!("    Query:         {:.3}s", source.query_secs);
            println!("    Fetch:         {:.3}s", source.fetch_secs);
            println!("    Arrow encode:  {:.3}s", source.arrow_encode_secs);
            println!("  Dest duration:   {:.2}s", dest.duration_secs);
            println!("    VM setup:      {:.3}s", dest.vm_setup_secs);
            println!("    Recv loop:     {:.3}s", dest.recv_secs);
            println!("    Connect:       {:.3}s", dest.connect_secs);
            println!("    Flush:         {:.3}s", dest.flush_secs);
            println!("    Arrow decode:  {:.3}s", dest.arrow_decode_secs);
            println!("    Commit:        {:.3}s", dest.commit_secs);
            println!("    WASM overhead: {:.3}s", result.wasm_overhead_secs);
            println!(
                "  Host emit_batch: {:.3}s ({} calls)",
                source.emit_nanos as f64 / 1e9,
                source.emit_count
            );
            if source.compress_nanos > 0 {
                println!(
                    "    Compression:   {:.3}s",
                    source.compress_nanos as f64 / 1e9
                );
            }
            println!(
                "  Host next_batch: {:.3}s ({} calls)",
                dest.recv_nanos as f64 / 1e9,
                dest.recv_count
            );
            println!(
                "    Wait in recv:  {:.3}s",
                dest.recv_wait_nanos as f64 / 1e9
            );
            println!(
                "    Work in recv:  {:.3}s",
                dest.recv_process_nanos as f64 / 1e9
            );
            if dest.decompress_nanos > 0 {
                println!(
                    "    Decompression: {:.3}s",
                    dest.decompress_nanos as f64 / 1e9
                );
            }
            if result.transform_count > 0 {
                println!(
                    "  Transforms:      {} stage(s), {:.2}s total",
                    result.transform_count, result.transform_duration_secs,
                );
                for (i, ms) in result.transform_module_load_ms.iter().enumerate() {
                    println!("    Transform[{i}] load: {ms}ms");
                }
            }
            println!("  Source load:     {}ms", source.module_load_ms);
            println!("  Dest load:       {}ms", dest.module_load_ms);
            if result.retry_count > 0 {
                println!("  Retries:         {}", result.retry_count);
            }
            if let Some(cpu) = &cpu_metrics {
                println!("  CPU time:        {:.3}s", cpu.cpu_secs);
                println!("  CPU use (1 core): {:.1}%", cpu.cpu_pct_one_core);
                println!(
                    "  CPU use ({0} cores): {1:.1}%",
                    cpu.available_cores, cpu.cpu_pct_of_available_cores
                );
            }

            // Machine-readable JSON for benchmarking tools
            let json = bench_json_from_result(&result, cpu_metrics.as_ref(), peak_rss_mb);
            println!("@@BENCH_JSON@@{json}");
        }
        PipelineOutcome::DryRun(result) => {
            use arrow::util::pretty::pretty_format_batches;

            println!(
                "Dry run: '{}' ({} stream{})",
                config.pipeline,
                result.streams.len(),
                if result.streams.len() == 1 { "" } else { "s" },
            );
            println!();

            for stream in &result.streams {
                println!("Stream: {}", stream.stream_name);

                if stream.batches.is_empty() {
                    println!("  (no data)");
                } else {
                    // Print schema
                    let schema = stream.batches[0].schema();
                    println!("  Columns:");
                    for field in schema.fields() {
                        println!(
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
                    println!();

                    // Print table
                    match pretty_format_batches(&stream.batches) {
                        Ok(table) => println!("{table}"),
                        Err(e) => println!("  (display error: {e})"),
                    }
                }

                println!(
                    "{} rows ({}, {} batch{})",
                    stream.total_rows,
                    format_bytes(stream.total_bytes),
                    stream.batches.len(),
                    if stream.batches.len() == 1 { "" } else { "es" },
                );
                println!();
            }

            println!("Duration: {:.2}s", result.duration_secs);
            if result.transform_count > 0 {
                println!(
                    "Transforms: {} applied ({:.2}s)",
                    result.transform_count, result.transform_duration_secs,
                );
            }
        }
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
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

fn process_cpu_seconds() -> Option<f64> {
    #[cfg(unix)]
    {
        // Safety: getrusage writes into the provided `rusage` struct.
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // Safety: pointer is valid for writes and initialized by successful call.
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }
        // Safety: call succeeded and initialized `usage`.
        let usage = unsafe { usage.assume_init() };
        let user_secs = usage.ru_utime.tv_sec as f64 + (usage.ru_utime.tv_usec as f64 / 1e6);
        let sys_secs = usage.ru_stime.tv_sec as f64 + (usage.ru_stime.tv_usec as f64 / 1e6);
        Some(user_secs + sys_secs)
    }

    #[cfg(not(unix))]
    {
        None
    }
}

fn process_peak_rss_mb() -> Option<f64> {
    #[cfg(unix)]
    {
        // Safety: getrusage writes into the provided `rusage` struct.
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // Safety: pointer is valid for writes and initialized by successful call.
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }

        // Safety: call succeeded and initialized `usage`.
        let usage = unsafe { usage.assume_init() };
        let rss_mb = {
            #[cfg(target_os = "macos")]
            {
                usage.ru_maxrss as f64 / (1024.0 * 1024.0)
            }
            #[cfg(not(target_os = "macos"))]
            {
                usage.ru_maxrss as f64 / 1024.0
            }
        };
        Some(rss_mb)
    }

    #[cfg(not(unix))]
    {
        None
    }
}

#[allow(clippy::too_many_lines)] // Flat JSON serialization requires one insert per field.
fn bench_json_from_result(
    result: &rapidbyte_engine::result::PipelineResult,
    cpu_metrics: Option<&ProcessCpuMetrics>,
    peak_rss_mb: Option<f64>,
) -> serde_json::Value {
    let counts = &result.counts;
    let source = &result.source;
    let dest = &result.dest;
    let stream_metrics = result
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
        .collect::<Vec<_>>();

    let partitioned_workers = result
        .stream_metrics
        .iter()
        .filter(|m| m.partition_count.unwrap_or(1) > 1)
        .collect::<Vec<_>>();
    let worker_records = partitioned_workers
        .iter()
        .map(|m| m.records_written)
        .collect::<Vec<_>>();
    let worker_records_min = worker_records.iter().min().copied();
    let worker_records_max = worker_records.iter().max().copied();
    let worker_records_skew_ratio = worker_records_min
        .zip(worker_records_max)
        .and_then(|(min, max)| (max > 0).then_some(min as f64 / max as f64));

    let worker_dest_total_secs: f64 = partitioned_workers
        .iter()
        .map(|m| m.dest_duration_secs)
        .sum();
    let worker_dest_recv_secs: f64 = partitioned_workers.iter().map(|m| m.dest_recv_secs).sum();
    let worker_dest_vm_setup_secs: f64 = partitioned_workers
        .iter()
        .map(|m| m.dest_vm_setup_secs)
        .sum();
    let worker_dest_active_secs =
        (worker_dest_total_secs - worker_dest_recv_secs - worker_dest_vm_setup_secs).max(0.0);

    let mut json = serde_json::Map::new();
    json.insert(
        "records_read".to_string(),
        serde_json::json!(counts.records_read),
    );
    json.insert(
        "records_written".to_string(),
        serde_json::json!(counts.records_written),
    );
    json.insert(
        "bytes_read".to_string(),
        serde_json::json!(counts.bytes_read),
    );
    json.insert(
        "bytes_written".to_string(),
        serde_json::json!(counts.bytes_written),
    );
    json.insert(
        "duration_secs".to_string(),
        serde_json::json!(result.duration_secs),
    );
    json.insert(
        "source_duration_secs".to_string(),
        serde_json::json!(source.duration_secs),
    );
    json.insert(
        "dest_duration_secs".to_string(),
        serde_json::json!(dest.duration_secs),
    );
    json.insert(
        "dest_connect_secs".to_string(),
        serde_json::json!(dest.connect_secs),
    );
    json.insert(
        "dest_flush_secs".to_string(),
        serde_json::json!(dest.flush_secs),
    );
    json.insert(
        "dest_commit_secs".to_string(),
        serde_json::json!(dest.commit_secs),
    );
    json.insert(
        "dest_vm_setup_secs".to_string(),
        serde_json::json!(dest.vm_setup_secs),
    );
    json.insert(
        "dest_recv_secs".to_string(),
        serde_json::json!(dest.recv_secs),
    );
    json.insert(
        "wasm_overhead_secs".to_string(),
        serde_json::json!(result.wasm_overhead_secs),
    );
    json.insert(
        "source_connect_secs".to_string(),
        serde_json::json!(source.connect_secs),
    );
    json.insert(
        "source_query_secs".to_string(),
        serde_json::json!(source.query_secs),
    );
    json.insert(
        "source_fetch_secs".to_string(),
        serde_json::json!(source.fetch_secs),
    );
    json.insert(
        "source_arrow_encode_secs".to_string(),
        serde_json::json!(source.arrow_encode_secs),
    );
    json.insert(
        "dest_arrow_decode_secs".to_string(),
        serde_json::json!(dest.arrow_decode_secs),
    );
    json.insert(
        "source_module_load_ms".to_string(),
        serde_json::json!(source.module_load_ms),
    );
    json.insert(
        "dest_module_load_ms".to_string(),
        serde_json::json!(dest.module_load_ms),
    );
    json.insert(
        "source_emit_nanos".to_string(),
        serde_json::json!(source.emit_nanos),
    );
    json.insert(
        "source_compress_nanos".to_string(),
        serde_json::json!(source.compress_nanos),
    );
    json.insert(
        "source_emit_count".to_string(),
        serde_json::json!(source.emit_count),
    );
    json.insert(
        "dest_recv_nanos".to_string(),
        serde_json::json!(dest.recv_nanos),
    );
    json.insert(
        "dest_recv_wait_nanos".to_string(),
        serde_json::json!(dest.recv_wait_nanos),
    );
    json.insert(
        "dest_recv_process_nanos".to_string(),
        serde_json::json!(dest.recv_process_nanos),
    );
    json.insert(
        "dest_decompress_nanos".to_string(),
        serde_json::json!(dest.decompress_nanos),
    );
    json.insert(
        "dest_recv_count".to_string(),
        serde_json::json!(dest.recv_count),
    );
    json.insert(
        "transform_count".to_string(),
        serde_json::json!(result.transform_count),
    );
    json.insert(
        "transform_duration_secs".to_string(),
        serde_json::json!(result.transform_duration_secs),
    );
    json.insert(
        "transform_module_load_ms".to_string(),
        serde_json::json!(result.transform_module_load_ms),
    );
    json.insert(
        "retry_count".to_string(),
        serde_json::json!(result.retry_count),
    );
    json.insert(
        "parallelism".to_string(),
        serde_json::json!(result.parallelism),
    );
    json.insert(
        "stream_metrics".to_string(),
        serde_json::json!(stream_metrics),
    );
    json.insert(
        "worker_records_min".to_string(),
        serde_json::json!(worker_records_min),
    );
    json.insert(
        "worker_records_max".to_string(),
        serde_json::json!(worker_records_max),
    );
    json.insert(
        "worker_records_skew_ratio".to_string(),
        serde_json::json!(worker_records_skew_ratio),
    );
    json.insert(
        "worker_dest_total_secs".to_string(),
        serde_json::json!(worker_dest_total_secs),
    );
    json.insert(
        "worker_dest_recv_secs".to_string(),
        serde_json::json!(worker_dest_recv_secs),
    );
    json.insert(
        "worker_dest_vm_setup_secs".to_string(),
        serde_json::json!(worker_dest_vm_setup_secs),
    );
    json.insert(
        "worker_dest_active_secs".to_string(),
        serde_json::json!(worker_dest_active_secs),
    );
    json.insert(
        "process_cpu_secs".to_string(),
        serde_json::json!(cpu_metrics.map(|m| m.cpu_secs)),
    );
    json.insert(
        "process_cpu_pct_one_core".to_string(),
        serde_json::json!(cpu_metrics.map(|m| m.cpu_pct_one_core)),
    );
    json.insert(
        "process_cpu_pct_available_cores".to_string(),
        serde_json::json!(cpu_metrics.map(|m| m.cpu_pct_of_available_cores)),
    );
    json.insert(
        "available_cores".to_string(),
        serde_json::json!(cpu_metrics.map(|m| m.available_cores)),
    );
    json.insert(
        "process_peak_rss_mb".to_string(),
        serde_json::json!(peak_rss_mb),
    );

    serde_json::Value::Object(json)
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
