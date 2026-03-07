//! Summary formatter — prints pipeline results at layered verbosity levels.

#![allow(clippy::cast_precision_loss)]

use console::style;
use rapidbyte_engine::result::PipelineResult;

use super::format::{format_byte_rate, format_bytes, format_count, format_duration, format_rate};
use crate::Verbosity;

/// Print a successful pipeline result to stderr.
pub fn print_success(result: &PipelineResult, pipeline_name: &str, verbosity: Verbosity) {
    if verbosity == Verbosity::Quiet {
        return;
    }

    print_compact(result, pipeline_name);

    if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
        print_verbose(result);
    }

    if verbosity == Verbosity::Diagnostic {
        print_diagnostic(result);
    }

    eprintln!();
}

// ── Default compact block ────────────────────────────────────────────

fn print_compact(result: &PipelineResult, pipeline_name: &str) {
    let c = &result.counts;

    eprintln!(
        "{} Pipeline '{}' completed in {}",
        style("\u{2713}").green().bold(),
        pipeline_name,
        format_duration(result.duration_secs),
    );
    eprintln!();
    eprintln!(
        "  {:<12}{} read \u{2192} {} written",
        "Records",
        format_count(c.records_read),
        format_count(c.records_written),
    );
    eprintln!(
        "  {:<12}{} read \u{2192} {} written",
        "Data",
        format_bytes(c.bytes_read),
        format_bytes(c.bytes_written),
    );

    let rows_rate = format_rate(c.records_read, result.duration_secs);
    let bytes_rate = format_byte_rate(c.bytes_read, result.duration_secs);
    eprintln!(
        "  {:<12}{} rows/s | {}",
        "Throughput", rows_rate, bytes_rate,
    );

    let stream_count = unique_stream_count(&result.stream_metrics);
    eprintln!(
        "  {:<12}{} completed | parallelism: {}",
        "Streams", stream_count, result.parallelism,
    );
}

// ── Verbose block (-v) ──────────────────────────────────────────────

fn print_verbose(result: &PipelineResult) {
    // Per-stream table
    if !result.stream_metrics.is_empty() {
        eprintln!();
        eprintln!("  Streams");
        eprintln!(
            "  {:<24} {:>10} {:>12} {:>10} {:>10} {:>8}",
            "stream_name", "Read", "Written", "In", "Out", "Time",
        );
        eprintln!(
            "  {}",
            "\u{2500}".repeat(78),
        );

        // Aggregate shards per stream
        for agg in aggregate_streams(&result.stream_metrics) {
            eprintln!(
                "  {:<24} {:>10} {:>12} {:>10} {:>10} {:>8}",
                agg.name,
                format_count(agg.records_read),
                format_count(agg.records_written),
                format_bytes(agg.bytes_read),
                format_bytes(agg.bytes_written),
                format_duration(agg.duration_secs),
            );
        }
    }

    // Stage timing
    eprintln!();
    eprintln!("  Timing");

    let src = &result.source;
    eprintln!(
        "    {:<10} connect {} | query {} | fetch {} | encode {}",
        "Source",
        format_duration(src.connect_secs),
        format_duration(src.query_secs),
        format_duration(src.fetch_secs),
        format_duration(src.arrow_encode_secs),
    );

    let dst = &result.dest;
    eprintln!(
        "    {:<10} connect {} | flush {} | commit {} | decode {}",
        "Dest",
        format_duration(dst.connect_secs),
        format_duration(dst.flush_secs),
        format_duration(dst.commit_secs),
        format_duration(dst.arrow_decode_secs),
    );

    if result.transform_count > 0 {
        eprintln!(
            "    {:<10} {} ({} stages)",
            "Transform",
            format_duration(result.transform_duration_secs),
            result.transform_count,
        );
    }
}

// ── Diagnostic block (-vv) ──────────────────────────────────────────

fn print_diagnostic(result: &PipelineResult) {
    let src = &result.source;
    let dst = &result.dest;

    let compress_secs = src.compress_nanos as f64 / 1_000_000_000.0;
    let decompress_secs = dst.decompress_nanos as f64 / 1_000_000_000.0;
    let emit_secs = src.emit_nanos as f64 / 1_000_000_000.0;
    let recv_secs = dst.recv_nanos as f64 / 1_000_000_000.0;

    eprintln!();
    eprintln!("  Diagnostics");
    eprintln!(
        "    {:<17}{}",
        "WASM overhead",
        format_duration(result.wasm_overhead_secs),
    );
    eprintln!(
        "    {:<17}encode {} | decode {}",
        "Compression",
        format_duration(compress_secs),
        format_duration(decompress_secs),
    );
    eprintln!(
        "    {:<17}emit_batch {} ({} calls) | next_batch {}",
        "Host ops",
        format_duration(emit_secs),
        format_count(src.emit_count),
        format_duration(recv_secs),
    );
    eprintln!(
        "    {:<17}source {}ms | dest {}ms",
        "Module load",
        src.module_load_ms,
        dst.module_load_ms,
    );

    // Shard skew table (only if partitioned streams exist)
    let partitioned: Vec<_> = result
        .stream_metrics
        .iter()
        .filter(|m| m.partition_count.map_or(false, |c| c > 1))
        .collect();

    if !partitioned.is_empty() {
        eprintln!();
        eprintln!("  Shard skew");
        eprintln!(
            "  {:<24} {:>5} {:>10} {:>12} {:>10} {:>10} {:>8}",
            "stream_name", "shard", "Read", "Written", "In", "Out", "Time",
        );
        eprintln!(
            "  {}",
            "\u{2500}".repeat(84),
        );

        for m in partitioned {
            let shard_label = m
                .partition_index
                .map_or_else(|| "-".to_string(), |i| i.to_string());
            let total_secs = m.source_duration_secs + m.dest_duration_secs;
            eprintln!(
                "  {:<24} {:>5} {:>10} {:>12} {:>10} {:>10} {:>8}",
                m.stream_name,
                shard_label,
                format_count(m.records_read),
                format_count(m.records_written),
                format_bytes(m.bytes_read),
                format_bytes(m.bytes_written),
                format_duration(total_secs),
            );
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn unique_stream_count(
    metrics: &[rapidbyte_engine::result::StreamShardMetric],
) -> usize {
    let mut names: Vec<&str> = metrics.iter().map(|m| m.stream_name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    names.len()
}

struct StreamAggregate {
    name: String,
    records_read: u64,
    records_written: u64,
    bytes_read: u64,
    bytes_written: u64,
    duration_secs: f64,
}

fn aggregate_streams(
    metrics: &[rapidbyte_engine::result::StreamShardMetric],
) -> Vec<StreamAggregate> {
    use std::collections::BTreeMap;

    let mut map: BTreeMap<&str, StreamAggregate> = BTreeMap::new();

    for m in metrics {
        let entry = map.entry(m.stream_name.as_str()).or_insert_with(|| {
            StreamAggregate {
                name: m.stream_name.clone(),
                records_read: 0,
                records_written: 0,
                bytes_read: 0,
                bytes_written: 0,
                duration_secs: 0.0,
            }
        });
        entry.records_read += m.records_read;
        entry.records_written += m.records_written;
        entry.bytes_read += m.bytes_read;
        entry.bytes_written += m.bytes_written;
        // Use max duration across shards (they run in parallel)
        let shard_dur = m.source_duration_secs + m.dest_duration_secs;
        if shard_dur > entry.duration_secs {
            entry.duration_secs = shard_dur;
        }
    }

    map.into_values().collect()
}
