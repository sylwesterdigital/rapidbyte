#![allow(clippy::cast_precision_loss)]

#[must_use]
pub fn format_bytes_binary(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[must_use]
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

#[cfg(test)]
mod tests {
    use super::{format_bytes_binary, format_count};

    #[test]
    fn format_bytes_binary_uses_binary_unit_labels() {
        assert_eq!(format_bytes_binary(0), "0 B");
        assert_eq!(format_bytes_binary(512), "512 B");
        assert_eq!(format_bytes_binary(1_048_576), "1.0 MiB");
        assert_eq!(format_bytes_binary(1_073_741_824), "1.0 GiB");
    }

    #[test]
    fn format_count_inserts_grouping_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(1_200_000), "1,200,000");
    }
}
