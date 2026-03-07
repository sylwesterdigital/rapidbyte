# CLI Output Redesign — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace all `println!` output with a modern CLI using `indicatif` + `console`, layered verbosity (`-v`/`-vv`/`-q`), live progress spinner, and formatted summary blocks.

**Architecture:** Progress events flow from engine to CLI via an optional `mpsc` channel. All display logic lives in a new `output` module in `rapidbyte-cli`. Engine remains library-friendly with no display dependencies.

**Tech Stack:** `indicatif 0.17`, `console 0.15`, `number_prefix` (for human-readable byte formatting)

---

### Task 1: Add workspace dependencies

**Files:**
- Modify: `Cargo.toml` (workspace root, lines 27-42)
- Modify: `crates/rapidbyte-cli/Cargo.toml` (lines 10-22)

**Step 1: Add indicatif and console to workspace deps**

In `Cargo.toml` (workspace root), add to `[workspace.dependencies]`:

```toml
indicatif = "0.17"
console = "0.15"
```

**Step 2: Add deps to CLI crate**

In `crates/rapidbyte-cli/Cargo.toml`, add to `[dependencies]`:

```toml
indicatif = { workspace = true }
console = { workspace = true }
```

**Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles with no errors

**Step 4: Commit**

```bash
git add Cargo.toml crates/rapidbyte-cli/Cargo.toml
git commit -m "deps: add indicatif and console for CLI output"
```

---

### Task 2: Add `ProgressEvent` types to engine

**Files:**
- Create: `crates/rapidbyte-engine/src/progress.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (line 30, add module + re-export)

**Step 1: Create progress event types**

Create `crates/rapidbyte-engine/src/progress.rs`:

```rust
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
```

**Step 2: Register module and add re-exports**

In `crates/rapidbyte-engine/src/lib.rs`, add after line 30 (`pub mod result;`):

```rust
pub mod progress;
```

And add re-export at end:

```rust
pub use progress::{Phase, ProgressEvent};
```

**Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-engine`
Expected: compiles with no errors

**Step 4: Commit**

```bash
git add crates/rapidbyte-engine/src/progress.rs crates/rapidbyte-engine/src/lib.rs
git commit -m "feat(engine): add ProgressEvent types for live CLI updates"
```

---

### Task 3: Thread progress sender through orchestrator

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` (lines 327-394, ~396-410, ~800-870)
- Modify: `crates/rapidbyte-engine/src/lib.rs` (re-export update)

**Step 1: Update `run_pipeline` signature**

Change `run_pipeline` (line 327) to accept an optional progress sender:

```rust
pub async fn run_pipeline(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    progress_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::progress::ProgressEvent>>,
) -> Result<PipelineOutcome, PipelineError> {
```

Pass `progress_tx.clone()` to `execute_pipeline_once`.

**Step 2: Update `execute_pipeline_once` signature**

Add `progress_tx: Option<...>` parameter. Emit `PhaseChange` events at key points:

- `Phase::Resolving` at start (before connector resolution)
- `Phase::Loading` before module loads
- `Phase::Running` before stream execution loop
- `Phase::Finished` after aggregation completes

Emit `StreamCompleted` in `collect_stream_task_results` (line 287) when a stream succeeds.

**Step 3: Emit `BatchCompleted` events**

In the stream spawn block (line 981), after the source/dest tasks complete and a `StreamResult` is constructed, the batch-level progress is already summarized in `ReadSummary`. Since batches flow through `mpsc::channel<Frame>` synchronously in spawn_blocking, the cleanest injection point is after each stream completes — emit one `BatchCompleted` with the stream's totals:

```rust
if let Some(tx) = &progress_tx {
    let _ = tx.send(ProgressEvent::BatchCompleted {
        stream: stream_ctx.stream_name.clone(),
        records: sr.read_summary.records_read,
        bytes: sr.read_summary.bytes_read,
    });
    let _ = tx.send(ProgressEvent::StreamCompleted {
        stream: stream_ctx.stream_name.clone(),
    });
}
```

Note: For finer-grained per-batch progress (rows ticking up during a single stream), a future enhancement can add a callback into the runner's frame-relay loop. For now, per-stream granularity is sufficient and avoids touching the runner hot path.

**Step 4: Update `run_pipeline` re-export in lib.rs**

The existing re-export `pub use orchestrator::run_pipeline;` already covers this since it re-exports the function. No change needed if the signature change is the only modification.

**Step 5: Fix all call sites**

The only call site for `run_pipeline` is in `crates/rapidbyte-cli/src/commands/run.rs` (line 45). Pass `None` for now:

```rust
let outcome = orchestrator::run_pipeline(&config, &options, None).await?;
```

Also check `tests/bench/src/main.rs` — if it calls `run_pipeline`, update there too.

**Step 6: Verify it compiles and tests pass**

Run: `cargo check --workspace && cargo test --workspace`
Expected: compiles and all tests pass

**Step 7: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(engine): thread progress sender through orchestrator"
```

---

### Task 4: Add Verbosity enum and CLI flags

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs` (lines 13-61)

**Step 1: Add verbosity flags to Cli struct**

Replace the `--log-level` as the only global flag. Add `--verbose` and `--quiet`:

```rust
#[derive(Parser)]
#[command(
    name = "rapidbyte",
    version,
    about = "The single-binary data ingestion engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    /// Increase output verbosity (-v for detailed, -vv for diagnostic)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress all output (exit code only, errors on stderr)
    #[arg(short, long, global = true)]
    quiet: bool,
}
```

**Step 2: Derive Verbosity enum**

Add above `main()`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verbosity {
    Quiet,
    Default,
    Verbose,
    Diagnostic,
}

impl Verbosity {
    fn from_flags(quiet: bool, verbose: u8) -> Self {
        if quiet {
            return Self::Quiet;
        }
        match verbose {
            0 => Self::Default,
            1 => Self::Verbose,
            _ => Self::Diagnostic,
        }
    }
}
```

**Step 3: Pass verbosity to commands**

In `main()`, compute verbosity and pass to command handlers:

```rust
let verbosity = Verbosity::from_flags(cli.quiet, cli.verbose);
```

For now, pass it to `commands::run::execute` — update the signature to accept `Verbosity`. Other commands will be updated in later tasks.

**Step 4: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(cli): add -v/-vv/--quiet verbosity flags"
```

---

### Task 5: Create the output module — format helpers

**Files:**
- Create: `crates/rapidbyte-cli/src/output/mod.rs`
- Create: `crates/rapidbyte-cli/src/output/format.rs`

**Step 1: Create the output module**

Create `crates/rapidbyte-cli/src/output/mod.rs`:

```rust
//! CLI output formatting — progress, summaries, and display helpers.

pub mod format;
pub mod progress;
pub mod summary;
```

**Step 2: Create format helpers**

Create `crates/rapidbyte-cli/src/output/format.rs` with:

- `format_bytes(bytes: u64) -> String` — human-readable (B/KB/MB/GB)
- `format_count(n: u64) -> String` — comma-separated thousands
- `format_rate(count: u64, duration_secs: f64) -> String` — "375K rows/s"
- `format_duration(secs: f64) -> String` — "3.2s" or "1m 23s"

```rust
//! Number formatting helpers for CLI display.

#![allow(clippy::cast_precision_loss)]

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_count(n: u64) -> String {
    if n < 1000 {
        return n.to_string();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn format_rate(count: u64, duration_secs: f64) -> String {
    if duration_secs <= 0.0 {
        return "N/A".to_string();
    }
    let rate = count as f64 / duration_secs;
    if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1000.0 {
        format!("{:.0}K", rate / 1000.0)
    } else {
        format!("{:.0}", rate)
    }
}

pub fn format_duration(secs: f64) -> String {
    if secs >= 60.0 {
        let mins = (secs / 60.0).floor() as u64;
        let remaining = secs - (mins as f64 * 60.0);
        format!("{mins}m {remaining:.1}s")
    } else {
        format!("{secs:.1}s")
    }
}
```

**Step 3: Write tests for format helpers**

Add `#[cfg(test)]` block in `format.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(1_200_000), "1,200,000");
    }

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(1_000_000, 1.0), "1.0M");
        assert_eq!(format_rate(375_000, 1.0), "375K");
        assert_eq!(format_rate(50, 1.0), "50");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(3.2), "3.2s");
        assert_eq!(format_duration(0.1), "0.1s");
        assert_eq!(format_duration(90.5), "1m 30.5s");
    }
}
```

**Step 4: Register module in main**

In `crates/rapidbyte-cli/src/main.rs`, add `mod output;` alongside existing `mod commands;`.

**Step 5: Run tests**

Run: `cargo test -p rapidbyte-cli`
Expected: all format tests pass

**Step 6: Commit**

```bash
git add crates/rapidbyte-cli/src/output/ crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): add output module with format helpers"
```

---

### Task 6: Create the progress spinner

**Files:**
- Create: `crates/rapidbyte-cli/src/output/progress.rs`

**Step 1: Create progress spinner handler**

Create `crates/rapidbyte-cli/src/output/progress.rs`:

```rust
//! Live progress spinner driven by ProgressEvent channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

use rapidbyte_engine::progress::{Phase, ProgressEvent};

use super::format;

/// Shared counters updated by progress events, read by spinner tick.
struct Counters {
    total_records: AtomicU64,
    total_bytes: AtomicU64,
    streams_done: AtomicU64,
    total_streams: AtomicU64,
}

/// Spawn a background task that drives the progress spinner.
///
/// Returns the receiver end — caller should pass the sender to the engine.
/// The spinner auto-finishes when the sender is dropped.
pub fn spawn_progress_spinner(
    total_streams: u64,
    is_tty: bool,
) -> (mpsc::UnboundedSender<ProgressEvent>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ProgressEvent>();

    let handle = tokio::spawn(async move {
        if !is_tty {
            // Drain events without displaying
            while rx.recv().await.is_some() {}
            return;
        }

        let counters = Arc::new(Counters {
            total_records: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            streams_done: AtomicU64::new(0),
            total_streams: AtomicU64::new(total_streams),
        });

        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        spinner.enable_steady_tick(std::time::Duration::from_millis(80));
        spinner.set_message("Resolving connectors...");

        while let Some(event) = rx.recv().await {
            match event {
                ProgressEvent::PhaseChange { phase } => match phase {
                    Phase::Resolving => spinner.set_message("Resolving connectors..."),
                    Phase::Loading => spinner.set_message("Loading connectors..."),
                    Phase::Running => {
                        update_running_message(&spinner, &counters);
                    }
                    Phase::Finished => break,
                },
                ProgressEvent::BatchCompleted {
                    records, bytes, ..
                } => {
                    counters
                        .total_records
                        .fetch_add(records, Ordering::Relaxed);
                    counters.total_bytes.fetch_add(bytes, Ordering::Relaxed);
                    update_running_message(&spinner, &counters);
                }
                ProgressEvent::StreamCompleted { .. } => {
                    counters.streams_done.fetch_add(1, Ordering::Relaxed);
                    update_running_message(&spinner, &counters);
                }
                ProgressEvent::Error { .. } => {}
            }
        }

        spinner.finish_and_clear();
    });

    (tx, handle)
}

fn update_running_message(spinner: &ProgressBar, counters: &Arc<Counters>) {
    let records = counters.total_records.load(Ordering::Relaxed);
    let bytes = counters.total_bytes.load(Ordering::Relaxed);
    let done = counters.streams_done.load(Ordering::Relaxed);
    let total = counters.total_streams.load(Ordering::Relaxed);

    let msg = format!(
        "Running \u{2014} {} rows | {} | {} of {} streams done",
        format::format_count(records),
        format::format_bytes(bytes),
        done,
        total,
    );
    spinner.set_message(msg);
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

**Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/output/progress.rs
git commit -m "feat(cli): add live progress spinner"
```

---

### Task 7: Create the summary formatter

**Files:**
- Create: `crates/rapidbyte-cli/src/output/summary.rs`

**Step 1: Create summary formatter**

Create `crates/rapidbyte-cli/src/output/summary.rs`. This module takes a `PipelineResult` and `Verbosity` and prints the formatted output.

```rust
//! Final pipeline summary formatting for default, -v, and -vv verbosity.

#![allow(clippy::cast_precision_loss)]

use console::style;

use rapidbyte_engine::result::PipelineResult;

use super::format::{format_bytes, format_count, format_duration, format_rate};

/// Verbosity level for summary output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verbosity {
    Quiet,
    Default,
    Verbose,
    Diagnostic,
}

/// Print the pipeline success summary at the given verbosity level.
pub fn print_success(result: &PipelineResult, pipeline_name: &str, verbosity: Verbosity) {
    if verbosity == Verbosity::Quiet {
        return;
    }

    let counts = &result.counts;

    // Header
    eprintln!(
        "{} Pipeline '{}' completed in {}",
        style("\u{2714}").green().bold(),
        pipeline_name,
        format_duration(result.duration_secs),
    );
    eprintln!();

    // Compact block (always shown in Default+)
    eprintln!(
        "  {:<14}{} read \u{2192} {} written",
        "Records",
        format_count(counts.records_read),
        format_count(counts.records_written),
    );
    eprintln!(
        "  {:<14}{} read \u{2192} {} written",
        "Data",
        format_bytes(counts.bytes_read),
        format_bytes(counts.bytes_written),
    );
    eprintln!(
        "  {:<14}{} rows/s | {}/s",
        "Throughput",
        format_rate(counts.records_read, result.duration_secs),
        format_bytes((counts.bytes_read as f64 / result.duration_secs) as u64),
    );

    let stream_count = result.stream_metrics.len();
    eprintln!(
        "  {:<14}{} completed | parallelism: {}",
        "Streams",
        stream_count,
        result.parallelism,
    );

    if verbosity == Verbosity::Default {
        return;
    }

    // -v: Per-stream table
    if !result.stream_metrics.is_empty() {
        eprintln!();
        eprintln!("  Streams");
        eprintln!(
            "  {:<20} {:>12} {:>12} {:>10} {:>8}",
            "Stream", "Read", "Written", "Data", "Time"
        );
        eprintln!("  {}", "\u{2500}".repeat(66));
        for m in &result.stream_metrics {
            let label = match m.partition_index {
                Some(idx) => format!("{}[{}]", m.stream_name, idx),
                None => m.stream_name.clone(),
            };
            eprintln!(
                "  {:<20} {:>12} {:>12} {:>10} {:>8}",
                label,
                format_count(m.records_read),
                format_count(m.records_written),
                format_bytes(m.bytes_read),
                format_duration(m.source_duration_secs + m.dest_duration_secs),
            );
        }
    }

    // -v: Stage timing breakdown
    let source = &result.source;
    let dest = &result.dest;
    eprintln!();
    eprintln!("  Timing");
    eprintln!(
        "    {:<12}connect {:.2}s | query {:.2}s | fetch {:.2}s | encode {:.2}s",
        "Source", source.connect_secs, source.query_secs, source.fetch_secs, source.arrow_encode_secs,
    );
    eprintln!(
        "    {:<12}connect {:.2}s | flush {:.2}s | commit {:.2}s | decode {:.2}s",
        "Dest", dest.connect_secs, dest.flush_secs, dest.commit_secs, dest.arrow_decode_secs,
    );
    if result.transform_count > 0 {
        eprintln!(
            "    {:<12}{:.2}s ({} stages)",
            "Transform", result.transform_duration_secs, result.transform_count,
        );
    }

    if verbosity == Verbosity::Verbose {
        return;
    }

    // -vv: Full diagnostics
    eprintln!();
    eprintln!("  Diagnostics");
    eprintln!("    {:<20}{:.3}s", "WASM overhead", result.wasm_overhead_secs);
    if source.compress_nanos > 0 || dest.decompress_nanos > 0 {
        eprintln!(
            "    {:<20}encode {:.3}s | decode {:.3}s",
            "Compression",
            source.compress_nanos as f64 / 1e9,
            dest.decompress_nanos as f64 / 1e9,
        );
    }
    eprintln!(
        "    {:<20}emit_batch {:.3}s ({} calls) | next_batch {:.3}s ({} calls)",
        "Host ops",
        source.emit_nanos as f64 / 1e9,
        source.emit_count,
        dest.recv_nanos as f64 / 1e9,
        dest.recv_count,
    );
    eprintln!(
        "    {:<20}load source {}ms | dest {}ms",
        "Module load",
        source.module_load_ms,
        dest.module_load_ms,
    );

    // Shard skew (only if partitioned streams exist)
    let partitioned: Vec<_> = result
        .stream_metrics
        .iter()
        .filter(|m| m.partition_count.unwrap_or(1) > 1)
        .collect();
    if !partitioned.is_empty() {
        eprintln!();
        eprintln!("  Shard skew");
        eprintln!(
            "  {:<16} {:>6} {:>12} {:>12} {:>8}",
            "Stream", "Shard", "Read", "Written", "Time"
        );
        eprintln!("  {}", "\u{2500}".repeat(60));
        for m in &partitioned {
            let shard = format!(
                "{}/{}",
                m.partition_index.unwrap_or(0),
                m.partition_count.unwrap_or(1)
            );
            eprintln!(
                "  {:<16} {:>6} {:>12} {:>12} {:>8}",
                m.stream_name,
                shard,
                format_count(m.records_read),
                format_count(m.records_written),
                format_duration(m.source_duration_secs + m.dest_duration_secs),
            );
        }
    }
}

/// Print a pipeline failure message.
pub fn print_failure(error: &dyn std::fmt::Display, pipeline_name: &str, verbosity: Verbosity) {
    if verbosity == Verbosity::Quiet {
        // Error already on stderr via anyhow
        return;
    }
    eprintln!(
        "{} Pipeline '{}' failed: {}",
        style("\u{2718}").red().bold(),
        pipeline_name,
        error,
    );
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

**Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/output/summary.rs
git commit -m "feat(cli): add summary formatter with layered verbosity"
```

---

### Task 8: Wire up `run` command with new output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs` (full rewrite of output section)
- Modify: `crates/rapidbyte-cli/src/main.rs` (pass verbosity)

**Step 1: Rewrite `run::execute`**

Update `run::execute` to:
1. Accept `Verbosity` parameter
2. If not quiet and is TTY: spawn progress spinner, pass sender to engine
3. Print summary using `summary::print_success`
4. Keep `@@BENCH_JSON@@` output (move to only emit when not quiet, or always — bench harness captures it regardless)
5. Keep `ProcessCpuMetrics` and `process_peak_rss_mb` logic (used in bench JSON and in `-vv` diagnostics)
6. Keep dry-run output (schema/table printing) but use `console::style` for headers

The key changes to `execute`:

```rust
pub async fn execute(
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: crate::Verbosity,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;
    let dry_run = dry_run || limit.is_some();
    let options = ExecutionOptions { dry_run, limit };

    let is_tty = console::Term::stderr().is_term();
    let summary_verbosity = match verbosity {
        crate::Verbosity::Quiet => crate::output::summary::Verbosity::Quiet,
        crate::Verbosity::Default => crate::output::summary::Verbosity::Default,
        crate::Verbosity::Verbose => crate::output::summary::Verbosity::Verbose,
        crate::Verbosity::Diagnostic => crate::output::summary::Verbosity::Diagnostic,
    };

    // Spawn progress spinner (non-quiet, non-dry-run only)
    let stream_count = config.source.streams.len() as u64;
    let show_progress = !matches!(verbosity, crate::Verbosity::Quiet) && !dry_run;
    let (progress_tx, spinner_handle) = if show_progress {
        let (tx, handle) =
            crate::output::progress::spawn_progress_spinner(stream_count, is_tty);
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    let cpu_start = process_cpu_seconds();
    let outcome = orchestrator::run_pipeline(&config, &options, progress_tx).await;

    // Wait for spinner to finish
    if let Some(handle) = spinner_handle {
        let _ = handle.await;
    }

    match outcome {
        Ok(PipelineOutcome::Run(result)) => {
            let cpu_end = process_cpu_seconds();
            // Print summary
            crate::output::summary::print_success(&result, &config.pipeline, summary_verbosity);

            // CPU/RSS for -vv
            if summary_verbosity == crate::output::summary::Verbosity::Diagnostic {
                let cpu_metrics = process_cpu_metrics(cpu_start, cpu_end, result.duration_secs);
                let peak_rss_mb = process_peak_rss_mb();
                if let Some(cpu) = &cpu_metrics {
                    eprintln!("    {:<20}CPU {:.1}s user+sys | RSS {:.0} MB",
                        "Process",
                        cpu.cpu_secs,
                        peak_rss_mb.unwrap_or(0.0),
                    );
                }
            }

            // Bench JSON (always emitted for tooling, on stdout)
            let cpu_end2 = process_cpu_seconds();
            let cpu_metrics = process_cpu_metrics(cpu_start, cpu_end2, result.duration_secs);
            let peak_rss_mb = process_peak_rss_mb();
            let json = bench_json_from_result(&result, cpu_metrics.as_ref(), peak_rss_mb);
            println!("@@BENCH_JSON@@{json}");

            Ok(())
        }
        Ok(PipelineOutcome::DryRun(result)) => {
            // Keep existing dry-run output (schema + table print)
            // ... (preserve current logic, just move println to eprintln for consistency)
            print_dry_run(&config.pipeline, &result);
            Ok(())
        }
        Err(e) => {
            crate::output::summary::print_failure(&e, &config.pipeline, summary_verbosity);
            Err(e.into())
        }
    }
}
```

Extract dry-run printing into a helper function `print_dry_run` to keep the main function clean.

**Step 2: Update main.rs to pass verbosity**

```rust
Commands::Run { pipeline, dry_run, limit } => {
    commands::run::execute(&pipeline, dry_run, limit, verbosity).await
}
```

**Step 3: Verify it compiles and existing test passes**

Run: `cargo test -p rapidbyte-cli && cargo check -p rapidbyte-cli`
Expected: compiles and `bench_json_includes_stream_shard_metrics` test passes

**Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/run.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): wire up run command with progress spinner and summary"
```

---

### Task 9: Update `check` command output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/check.rs`

**Step 1: Update check output with styled formatting**

Replace `println!` calls with `console::style`-based output on stderr. Accept verbosity.

```rust
pub async fn execute(pipeline_path: &Path, verbosity: crate::Verbosity) -> Result<()> {
    if matches!(verbosity, crate::Verbosity::Quiet) {
        // Run checks, return result via exit code
        let config = super::load_pipeline(pipeline_path)?;
        let result = orchestrator::check_pipeline(&config).await?;
        // ... check pass/fail, return Ok or bail
    }

    // Styled output:
    // ✓ source-postgres    valid    (green checkmark)
    // ✗ dest-postgres      failed   (red X + error message)
}
```

**Step 2: Update main.rs to pass verbosity**

**Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`

**Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/check.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): styled check command output"
```

---

### Task 10: Update `discover` command output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/discover.rs`

**Step 1: Update discover output**

- Styled header with stream count
- Table format for streams (name, sync mode, cursor, column count)
- `-v` shows full schema per stream
- `@@CATALOG_JSON@@` stays on stdout for tooling
- `--quiet` suppresses everything except JSON on stdout

**Step 2: Update main.rs to pass verbosity**

**Step 3: Verify**

Run: `cargo check -p rapidbyte-cli`

**Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/discover.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): styled discover command output"
```

---

### Task 11: Update `connectors` command output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/connectors.rs`

**Step 1: Update connectors listing**

Clean columnar output: name, role, description. Respect `--quiet`.

**Step 2: Update main.rs**

**Step 3: Verify and commit**

```bash
git add crates/rapidbyte-cli/src/commands/connectors.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): styled connectors command output"
```

---

### Task 12: Update bench harness call site

**Files:**
- Modify: `tests/bench/src/main.rs`

**Step 1: Check if bench calls `run_pipeline` directly**

If yes, update the call to pass `None` for progress sender.

**Step 2: Verify**

Run: `cargo check -p rapidbyte-bench` (or whatever the bench crate is named)

**Step 3: Commit**

```bash
git add tests/bench/src/main.rs
git commit -m "fix(bench): update run_pipeline call for new progress param"
```

---

### Task 13: Update logging to coexist with progress spinner

**Files:**
- Modify: `crates/rapidbyte-cli/src/logging.rs`

**Step 1: Integrate tracing with indicatif**

When a progress spinner is active, tracing log lines need to suspend/resume the spinner to avoid visual corruption. `indicatif` provides `ProgressBar::suspend()` for this.

Use `indicatif`'s `ProgressBar` integration with tracing-subscriber. The simplest approach: when verbosity is Default or above and TTY, set tracing to `warn` level (suppress info-level chattiness that duplicates our formatted output). When `-vv`, let tracing through at the configured level.

Alternative: use `indicatif::TermLike` with tracing's writer. For now, the simpler approach is sufficient.

**Step 2: Verify and commit**

```bash
git add crates/rapidbyte-cli/src/logging.rs crates/rapidbyte-cli/src/main.rs
git commit -m "fix(cli): adjust tracing levels to coexist with progress spinner"
```

---

### Task 14: End-to-end manual verification

**Step 1: Build everything**

Run: `just build && just build-connectors`

**Step 2: Test default mode**

Run a pipeline and verify:
- Spinner appears during execution
- Compact summary block appears on completion
- No duplicate output from tracing

**Step 3: Test `-v` mode**

Run with `-v` and verify per-stream table and timing breakdown appear.

**Step 4: Test `-vv` mode**

Run with `-vv` and verify full diagnostics (compression, WASM overhead, host ops, shard skew).

**Step 5: Test `--quiet` mode**

Run with `-q` and verify no output on success, only exit code.

**Step 6: Test non-TTY**

Run: `rapidbyte run pipeline.yaml | cat`
Verify: no spinner, plain text summary, no ANSI escape codes.

**Step 7: Test bench JSON still works**

Run bench and verify `@@BENCH_JSON@@` is still captured correctly.

**Step 8: Commit any fixes found during testing**

---

### Task 15: Update documentation

**Files:**
- Modify: `CLAUDE.md` (update CLI flags section)

**Step 1: Update CLAUDE.md commands section**

Add `-v`/`-vv`/`-q` flags to the commands documentation.

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document -v/-vv/--quiet CLI flags"
```
