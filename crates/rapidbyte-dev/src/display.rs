//! Display helpers for the dev shell — table rendering and styled output.

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use console::style;
use rapidbyte_types::format::{format_bytes_binary, format_count as shared_format_count};

/// Format bytes into human-readable form.
pub(crate) fn format_bytes(bytes: u64) -> String {
    format_bytes_binary(bytes)
}

/// Format a count with comma separators.
pub(crate) fn format_count(n: u64) -> String {
    shared_format_count(n)
}

/// Print Arrow `RecordBatch`es as a Unicode box-drawing table with row count footer.
pub(crate) fn print_batches(batches: &[RecordBatch]) {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        eprintln!("0 rows");
        return;
    }

    match pretty_format_batches(batches) {
        Ok(table) => {
            eprintln!("{table}");
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            eprintln!(
                "{} row{}",
                format_count(total_rows as u64),
                if total_rows == 1 { "" } else { "s" },
            );
        }
        Err(e) => {
            print_error(&format!("Failed to format results: {e}"));
        }
    }
}

/// Print a success message with green checkmark.
pub(crate) fn print_success(msg: &str) {
    eprintln!("{} {msg}", style("\u{2713}").green().bold());
}

/// Print an error message with red cross.
pub(crate) fn print_error(msg: &str) {
    eprintln!("{} {msg}", style("\u{2718}").red().bold());
}

/// Print an info/hint message (dim).
pub(crate) fn print_hint(msg: &str) {
    eprintln!("{}", style(msg).dim());
}
