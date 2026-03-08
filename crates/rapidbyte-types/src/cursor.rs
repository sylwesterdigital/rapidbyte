//! Cursor types for incremental sync position tracking.
//!
//! Cursors mark the last-read position in a source stream so that
//! subsequent runs can resume from where the previous run left off.

use serde::{Deserialize, Serialize};

/// Data type of a cursor value.
///
/// Determines how cursor values are compared and serialized for
/// incremental sync queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum CursorType {
    /// 64-bit signed integer cursor.
    Int64,
    /// UTF-8 string cursor.
    Utf8,
    /// Millisecond-precision timestamp cursor.
    TimestampMillis,
    /// Microsecond-precision timestamp cursor.
    TimestampMicros,
    /// Arbitrary-precision decimal cursor.
    Decimal,
    /// JSON value cursor.
    Json,
    /// `PostgreSQL` Log Sequence Number cursor.
    Lsn,
}

/// Typed cursor position value.
///
/// Each variant carries its value in a named `value` field for clean
/// JSON serialization: `{"type": "int64", "value": 42}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CursorValue {
    /// No cursor value (initial state).
    Null,
    /// 64-bit signed integer position.
    Int64 { value: i64 },
    /// UTF-8 string position.
    Utf8 { value: String },
    /// Millisecond-precision timestamp position.
    TimestampMillis { value: i64 },
    /// Microsecond-precision timestamp position.
    TimestampMicros { value: i64 },
    /// Arbitrary-precision decimal position.
    Decimal { value: String, scale: i32 },
    /// JSON value position.
    Json { value: serde_json::Value },
    /// `PostgreSQL` LSN position.
    Lsn { value: String },
}

/// Cursor tracking state for a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorInfo {
    /// Column used for cursor-based tracking.
    pub cursor_field: String,
    /// Optional deterministic tie-breaker field used when cursor values are not unique.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tie_breaker_field: Option<String>,
    /// Data type of the cursor column.
    pub cursor_type: CursorType,
    /// Last persisted cursor position (`None` on first run).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_value: Option<CursorValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_value_int64_json_format() {
        let cv = CursorValue::Int64 { value: 42 };
        let json = serde_json::to_value(&cv).unwrap();
        assert_eq!(json, serde_json::json!({"type": "int64", "value": 42}));
    }

    #[test]
    fn cursor_value_null_json_format() {
        let cv = CursorValue::Null;
        let json = serde_json::to_value(&cv).unwrap();
        assert_eq!(json, serde_json::json!({"type": "null"}));
    }

    #[test]
    fn cursor_info_roundtrip() {
        let info = CursorInfo {
            cursor_field: "updated_at".into(),
            tie_breaker_field: Some("id".into()),
            cursor_type: CursorType::TimestampMicros,
            last_value: Some(CursorValue::TimestampMicros {
                value: 1_700_000_000_000_000,
            }),
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: CursorInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn cursor_info_none_skips_last_value() {
        let info = CursorInfo {
            cursor_field: "id".into(),
            tie_breaker_field: None,
            cursor_type: CursorType::Int64,
            last_value: None,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert!(json.get("last_value").is_none());
        assert!(json.get("tie_breaker_field").is_none());
    }
}
