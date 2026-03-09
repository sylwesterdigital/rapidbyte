//! Display helpers for the dev shell — table rendering and styled output.

#![allow(clippy::cast_precision_loss)]

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use console::style;

/// Format bytes into human-readable form.
pub(crate) fn format_bytes(bytes: u64) -> String {
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

/// Format a count with comma separators.
pub(crate) fn format_count(n: u64) -> String {
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
