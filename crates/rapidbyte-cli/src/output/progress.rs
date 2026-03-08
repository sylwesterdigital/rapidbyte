//! Live progress spinner driven by `ProgressEvent` channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

use rapidbyte_engine::progress::{Phase, ProgressEvent};

use super::format;

/// Minimum interval between spinner message updates.
const UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Shared counters updated by progress events, read by spinner tick.
struct Counters {
    total_batches: AtomicU64,
    total_bytes: AtomicU64,
    streams_done: AtomicU64,
    total_streams: AtomicU64,
}

/// Spawn a background task that drives the progress spinner.
///
/// Returns the sender and a join handle. Pass the sender to the engine.
/// The spinner auto-finishes when the sender is dropped.
pub fn spawn_progress_spinner(
    total_streams: u64,
    is_tty: bool,
) -> (
    mpsc::UnboundedSender<ProgressEvent>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ProgressEvent>();

    let handle = tokio::spawn(async move {
        if !is_tty {
            // Drain events without displaying
            while rx.recv().await.is_some() {}
            return;
        }

        let counters = Arc::new(Counters {
            total_batches: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            streams_done: AtomicU64::new(0),
            total_streams: AtomicU64::new(total_streams),
        });

        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&[
                    "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}",
                    "\u{2826}", "\u{2827}", "\u{2807}", "\u{280f}",
                ]),
        );
        spinner.enable_steady_tick(std::time::Duration::from_millis(80));
        spinner.set_message("Resolving plugins...");

        let mut last_update = Instant::now();

        while let Some(event) = rx.recv().await {
            match event {
                ProgressEvent::PhaseChange { phase } => match phase {
                    Phase::Resolving => spinner.set_message("Resolving plugins..."),
                    Phase::Loading => spinner.set_message("Loading plugins..."),
                    Phase::Running => {
                        update_running_message(&spinner, &counters);
                        last_update = Instant::now();
                    }
                    Phase::Finished => break,
                },
                ProgressEvent::BatchEmitted { bytes } => {
                    counters.total_batches.fetch_add(1, Ordering::Relaxed);
                    counters.total_bytes.fetch_add(bytes, Ordering::Relaxed);
                    if last_update.elapsed() >= UPDATE_INTERVAL {
                        update_running_message(&spinner, &counters);
                        last_update = Instant::now();
                    }
                }
                ProgressEvent::StreamCompleted { .. } => {
                    counters.streams_done.fetch_add(1, Ordering::Relaxed);
                    update_running_message(&spinner, &counters);
                    last_update = Instant::now();
                }
                ProgressEvent::Retry {
                    attempt,
                    max_retries,
                    message,
                    delay_secs,
                } => {
                    let msg = format!(
                        "Retry {attempt}/{max_retries} in {delay_secs:.0}s \u{2014} {message}",
                    );
                    spinner.set_message(msg);
                }
                ProgressEvent::Error { .. } => {}
            }
        }

        spinner.finish_and_clear();
    });

    (tx, handle)
}

fn update_running_message(spinner: &ProgressBar, counters: &Arc<Counters>) {
    let batches = counters.total_batches.load(Ordering::Relaxed);
    let bytes = counters.total_bytes.load(Ordering::Relaxed);
    let done = counters.streams_done.load(Ordering::Relaxed);
    let total = counters.total_streams.load(Ordering::Relaxed);

    let msg = format!(
        "Running \u{2014} {} | {} batches | {} of {} streams done",
        format::format_bytes(bytes),
        format::format_count(batches),
        done,
        total,
    );
    spinner.set_message(msg);
}
