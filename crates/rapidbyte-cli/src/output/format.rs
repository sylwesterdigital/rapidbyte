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

pub fn format_byte_rate(bytes: u64, duration_secs: f64) -> String {
    if duration_secs <= 0.0 {
        return "N/A".to_string();
    }
    let bps = bytes as f64 / duration_secs;
    format!("{}/s", format_bytes(bps.round() as u64))
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
    fn test_format_byte_rate() {
        assert_eq!(format_byte_rate(1_048_576, 1.0), "1.0 MB/s");
        assert_eq!(format_byte_rate(500, 0.0), "N/A");
        assert_eq!(format_byte_rate(1_073_741_824, 2.0), "512.0 MB/s");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(3.2), "3.2s");
        assert_eq!(format_duration(0.1), "0.1s");
        assert_eq!(format_duration(90.5), "1m 30.5s");
    }
}
