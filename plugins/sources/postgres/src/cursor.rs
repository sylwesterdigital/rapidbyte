//! Cursor extraction and max-value tracking for incremental reads.
//!
//! `CursorTracker` encapsulates deterministic resume tracking for incremental
//! reads. When configured with a tie-breaker field it tracks the maximum
//! `(cursor, tie_breaker)` pair observed during the read.

use std::cmp::Ordering;

use chrono::NaiveDateTime;
use rapidbyte_sdk::cursor::CursorType;
use rapidbyte_sdk::prelude::*;
use tokio_postgres::Row;

use crate::types::Column;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObservedValue {
    Int(i64),
    Text(String),
}

impl ObservedValue {
    fn into_cursor_value(self) -> CursorValue {
        match self {
            Self::Int(value) => CursorValue::Int64 { value },
            Self::Text(value) => CursorValue::Utf8 { value },
        }
    }
}

#[derive(Debug, Clone)]
struct CursorComponent {
    field: String,
    idx: usize,
    strategy: Strategy,
}

/// Whether to track cursor values as integer or text.
#[derive(Debug, Clone, Copy)]
enum Strategy {
    Int,
    Text,
}

/// Tracks the maximum cursor value or `(cursor, tie_breaker)` pair observed during a read.
pub struct CursorTracker {
    cursor: CursorComponent,
    tie_breaker: Option<CursorComponent>,
    max_pair: Option<(ObservedValue, Option<ObservedValue>)>,
}

impl CursorTracker {
    /// Create a tracker for the given cursor info and column list.
    ///
    /// Returns `Err` if any configured cursor field is not found in `columns`.
    pub fn new(info: &CursorInfo, columns: &[Column]) -> Result<Self, String> {
        let cursor = build_component(&info.cursor_field, Some(info.cursor_type), columns)?;
        let tie_breaker = info
            .tie_breaker_field
            .as_deref()
            .map(|field| build_component(field, None, columns))
            .transpose()?;

        Ok(Self {
            cursor,
            tie_breaker,
            max_pair: None,
        })
    }

    /// Record cursor observations from a row, updating the tracked maximum if needed.
    pub fn observe_row(&mut self, row: &Row) {
        let Some(cursor_value) = extract_observed_value(row, &self.cursor) else {
            return;
        };
        let tie_breaker_value = self
            .tie_breaker
            .as_ref()
            .and_then(|component| extract_observed_value(row, component));

        if self.tie_breaker.is_some() && tie_breaker_value.is_none() {
            return;
        }

        let candidate = (cursor_value, tie_breaker_value);
        match &self.max_pair {
            Some(current)
                if compare_pairs(current, &candidate) != Ordering::Less =>
            {
                {}
            }
            _ => self.max_pair = Some(candidate),
        }
    }

    /// Consume the tracker and build a checkpoint if any values were observed.
    pub fn into_checkpoint(
        self,
        stream_name: &str,
        records_processed: u64,
        bytes_processed: u64,
    ) -> Option<Checkpoint> {
        let (cursor_value, tie_breaker_value) = self.max_pair?;
        let cursor_value = if self.tie_breaker.is_some() {
            let cursor_json = serde_json::to_value(cursor_value.into_cursor_value()).ok()?;
            let tie_breaker_json =
                serde_json::to_value(tie_breaker_value?.into_cursor_value()).ok()?;
            CursorValue::Json {
                value: serde_json::json!({
                    "cursor": cursor_json,
                    "tie_breaker": tie_breaker_json,
                }),
            }
        } else {
            cursor_value.into_cursor_value()
        };

        Some(Checkpoint {
            id: 0,
            kind: CheckpointKind::Source,
            stream: stream_name.to_string(),
            cursor_field: Some(self.cursor.field),
            cursor_value: Some(cursor_value),
            records_processed,
            bytes_processed,
        })
    }
}

fn build_component(
    field: &str,
    declared_type: Option<CursorType>,
    columns: &[Column],
) -> Result<CursorComponent, String> {
    let idx = columns
        .iter()
        .position(|c| c.name == field)
        .ok_or_else(|| {
            format!(
                "cursor field '{}' not found in columns: [{}]",
                field,
                columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    let strategy = if matches!(declared_type, Some(CursorType::Int64))
        || matches!(
            columns[idx].arrow_type,
            ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
        )
    {
        Strategy::Int
    } else {
        Strategy::Text
    };

    Ok(CursorComponent {
        field: field.to_string(),
        idx,
        strategy,
    })
}

fn extract_observed_value(row: &Row, component: &CursorComponent) -> Option<ObservedValue> {
    match component.strategy {
        Strategy::Int => row
            .try_get::<_, i64>(component.idx)
            .ok()
            .map(ObservedValue::Int)
            .or_else(|| {
                row.try_get::<_, i32>(component.idx)
                    .ok()
                    .map(i64::from)
                    .map(ObservedValue::Int)
            })
            .or_else(|| {
                row.try_get::<_, i16>(component.idx)
                    .ok()
                    .map(i64::from)
                    .map(ObservedValue::Int)
            }),
        Strategy::Text => row
            .try_get::<_, String>(component.idx)
            .ok()
            .map(ObservedValue::Text)
            .or_else(|| {
                row.try_get::<_, i64>(component.idx)
                    .ok()
                    .map(|n| ObservedValue::Text(n.to_string()))
            })
            .or_else(|| {
                row.try_get::<_, i32>(component.idx)
                    .ok()
                    .map(|n| ObservedValue::Text(n.to_string()))
            })
            .or_else(|| {
                row.try_get::<_, NaiveDateTime>(component.idx)
                    .ok()
                    .map(|dt| ObservedValue::Text(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            })
            .or_else(|| {
                row.try_get::<_, chrono::DateTime<chrono::Utc>>(component.idx)
                    .ok()
                    .map(|dt| ObservedValue::Text(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            })
            .or_else(|| {
                row.try_get::<_, chrono::NaiveDate>(component.idx)
                    .ok()
                    .map(|d| ObservedValue::Text(d.to_string()))
            })
            .or_else(|| {
                row.try_get::<_, serde_json::Value>(component.idx)
                    .ok()
                    .map(|v| ObservedValue::Text(v.to_string()))
            }),
    }
}

fn compare_pairs(
    current: &(ObservedValue, Option<ObservedValue>),
    candidate: &(ObservedValue, Option<ObservedValue>),
) -> Ordering {
    match compare_observed_values(&current.0, &candidate.0) {
        Ordering::Equal => match (&current.1, &candidate.1) {
            (Some(left), Some(right)) => compare_observed_values(left, right),
            _ => Ordering::Equal,
        },
        ordering => ordering,
    }
}

fn compare_observed_values(left: &ObservedValue, right: &ObservedValue) -> Ordering {
    match (left, right) {
        (ObservedValue::Int(left), ObservedValue::Int(right)) => left.cmp(right),
        (ObservedValue::Text(left), ObservedValue::Text(right)) => left.cmp(right),
        (ObservedValue::Int(left), ObservedValue::Text(right)) => left.to_string().cmp(right),
        (ObservedValue::Text(left), ObservedValue::Int(right)) => left.cmp(&right.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cursor_info(
        field: &str,
        tie_breaker_field: Option<&str>,
        cursor_type: CursorType,
        last: Option<CursorValue>,
    ) -> CursorInfo {
        CursorInfo {
            cursor_field: field.to_string(),
            tie_breaker_field: tie_breaker_field.map(str::to_string),
            cursor_type,
            last_value: last,
        }
    }

    #[test]
    fn missing_cursor_column_returns_error() {
        let info = make_cursor_info("missing", None, CursorType::Int64, None);
        let cols = vec![Column::new("id", "integer", false)];
        assert!(CursorTracker::new(&info, &cols).is_err());
    }

    #[test]
    fn missing_tie_breaker_column_returns_error() {
        let info = make_cursor_info("id", Some("pk"), CursorType::Int64, None);
        let cols = vec![Column::new("id", "bigint", false)];
        assert!(CursorTracker::new(&info, &cols).is_err());
    }

    #[test]
    fn track_max_pair_emits_json_cursor() {
        let info = make_cursor_info("updated_at", Some("id"), CursorType::Utf8, None);
        let cols = vec![
            Column::new("updated_at", "timestamp", false),
            Column::new("id", "bigint", false),
        ];
        let mut tracker = CursorTracker::new(&info, &cols).unwrap();

        tracker.max_pair = Some((
            ObservedValue::Text("2024-01-01 00:00:00.000000".to_string()),
            Some(ObservedValue::Int(99)),
        ));

        let cp = tracker.into_checkpoint("stream", 100, 2048).unwrap();
        match cp.cursor_value.unwrap() {
            CursorValue::Json { value } => {
                assert_eq!(value["cursor"]["type"], "utf8");
                assert_eq!(value["tie_breaker"]["type"], "int64");
            }
            other => panic!("expected composite json cursor, got {other:?}"),
        }
    }

    #[test]
    fn track_max_single_value_emits_scalar_cursor() {
        let info = make_cursor_info("id", None, CursorType::Int64, None);
        let cols = vec![Column::new("id", "bigint", false)];
        let mut tracker = CursorTracker::new(&info, &cols).unwrap();
        tracker.max_pair = Some((ObservedValue::Int(10), None));

        let cp = tracker.into_checkpoint("stream", 100, 2048).unwrap();
        assert_eq!(cp.cursor_value, Some(CursorValue::Int64 { value: 10 }));
    }
}
