//! Arrow `RecordBatch` encoder for `pgoutput` CDC change rows.
//!
//! Converts decoded `pgoutput` tuple data into typed Arrow batches.
//! Column values arrive as text strings (`PostgreSQL` text format) and are
//! parsed into the target Arrow type determined by each column's OID.

use std::sync::Arc;

use rapidbyte_sdk::arrow::array::{
    Array, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::{arrow_data_type, ArrowDataType};

use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};
use crate::types::oid_to_arrow_type;

// ---------------------------------------------------------------------------
// Relation metadata
// ---------------------------------------------------------------------------

/// Cached relation metadata used to encode `pgoutput` rows into Arrow batches.
///
/// Built once per `Relation` message and reused for every subsequent
/// `Insert`/`Update`/`Delete` referencing the same relation OID.
#[derive(Debug, Clone)]
pub(crate) struct RelationInfo {
    #[allow(dead_code)]
    pub(crate) oid: u32,
    #[allow(dead_code)]
    pub(crate) namespace: String,
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) arrow_schema: Arc<Schema>,
}

impl RelationInfo {
    /// Create a new `RelationInfo`, building the Arrow schema from column OIDs.
    pub(crate) fn new(oid: u32, namespace: String, name: String, columns: Vec<ColumnDef>) -> Self {
        let mut fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let dt = arrow_data_type(oid_to_arrow_type(col.type_oid));
                Field::new(&col.name, dt, true)
            })
            .collect();

        // Append the CDC operation metadata column.
        fields.push(Field::new("_rb_op", DataType::Utf8, false));

        let arrow_schema = Arc::new(Schema::new(fields));

        Self {
            oid,
            namespace,
            name,
            columns,
            arrow_schema,
        }
    }
}

// ---------------------------------------------------------------------------
// CDC row
// ---------------------------------------------------------------------------

/// A single CDC change row ready for Arrow encoding.
pub(crate) struct CdcRow {
    pub(crate) op: CdcOp,
    pub(crate) tuple: TupleData,
}

// ---------------------------------------------------------------------------
// Column text extraction
// ---------------------------------------------------------------------------

/// Extract the text value from a column at `idx`, returning `None` for
/// `Null`, `UnchangedToast`, or out-of-bounds indices.
fn col_text(tuple: &TupleData, idx: usize) -> Option<&str> {
    match tuple.columns.get(idx)? {
        ColumnValue::Text(s) => Some(s.as_str()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Batch encoder
// ---------------------------------------------------------------------------

/// Encode a slice of CDC rows into an Arrow `RecordBatch`.
///
/// Each column is parsed from its pgoutput text representation into the
/// appropriate Arrow type based on the column's OID. Null, unchanged-toast,
/// and missing columns all produce Arrow nulls.
#[allow(clippy::too_many_lines)]
pub(crate) fn encode_cdc_batch(
    rows: &[CdcRow],
    relation: &RelationInfo,
) -> Result<RecordBatch, String> {
    let capacity = rows.len();
    let num_data_cols = relation.columns.len();

    // Build one typed array per data column.
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_data_cols + 1);

    for (col_idx, col_def) in relation.columns.iter().enumerate() {
        let arrow_type = oid_to_arrow_type(col_def.type_oid);
        let array: Arc<dyn Array> = match arrow_type {
            ArrowDataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx) {
                        Some(s) => match s {
                            "t" | "true" => b.append_value(true),
                            "f" | "false" => b.append_value(false),
                            _ => b.append_null(),
                        },
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int16 => {
                let mut b = Int16Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse::<i16>().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int32 => {
                let mut b = Int32Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse::<i32>().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int64 => {
                let mut b = Int64Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse::<i64>().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Float32 => {
                let mut b = Float32Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse::<f32>().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Float64 => {
                let mut b = Float64Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse::<f64>().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Date32 => {
                let mut b = Date32Builder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(parse_date32) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::TimestampMicros => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(capacity);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(parse_timestamp_micros) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            // Everything else (Utf8, Binary, unknown) → StringBuilder fallback.
            _ => {
                let mut b = StringBuilder::with_capacity(capacity, capacity * 32);
                for row in rows {
                    match col_text(&row.tuple, col_idx) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(array);
    }

    // Append the _rb_op column.
    let mut op_builder = StringBuilder::with_capacity(capacity, capacity * 8);
    for row in rows {
        op_builder.append_value(row.op.as_str());
    }
    arrays.push(Arc::new(op_builder.finish()));

    RecordBatch::try_new(relation.arrow_schema.clone(), arrays)
        .map_err(|e| format!("failed to create CDC RecordBatch: {e}"))
}

// ---------------------------------------------------------------------------
// Text → typed value parsers
// ---------------------------------------------------------------------------

/// Parse a `YYYY-MM-DD` date string into days since the Unix epoch.
fn parse_date32(s: &str) -> Option<i32> {
    // Expected format: "YYYY-MM-DD"
    let mut parts = s.splitn(3, '-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;

    // Compute days since 1970-01-01 using a simplified Julian-day formula.
    // This avoids a `chrono` dependency for the WASI connector.
    days_since_epoch(year, month, day)
}

/// Compute the number of days from 1970-01-01 to the given date.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Algorithm: convert to a Julian Day Number, then subtract the JDN of 1970-01-01.
    // Adjusted month: Jan/Feb are months 13/14 of the previous year.
    let (y, m) = if month <= 2 {
        (i64::from(year) - 1, i64::from(month) + 12)
    } else {
        (i64::from(year), i64::from(month))
    };

    let jdn =
        365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + i64::from(day) + 1_721_119;
    // JDN for 1970-01-01 is 2_440_588.
    let epoch_jdn: i64 = 2_440_588;

    // Safety: realistic date differences always fit in i32.
    #[allow(clippy::cast_possible_truncation)]
    Some((jdn - epoch_jdn) as i32)
}

/// Parse a timestamp string into microseconds since the Unix epoch.
///
/// Accepts:
/// - `YYYY-MM-DD HH:MM:SS`
/// - `YYYY-MM-DD HH:MM:SS.f` (variable fractional seconds)
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let (date_part, time_part) = s.split_once(' ')?;

    // Date
    let mut dp = date_part.splitn(3, '-');
    let year: i32 = dp.next()?.parse().ok()?;
    let month: u32 = dp.next()?.parse().ok()?;
    let day: u32 = dp.next()?.parse().ok()?;

    let days = days_since_epoch(year, month, day)?;

    // Time
    let (time_hms, frac_str) = if let Some((hms, frac)) = time_part.split_once('.') {
        (hms, Some(frac))
    } else {
        (time_part, None)
    };

    let mut tp = time_hms.splitn(3, ':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;

    // Fractional seconds → microseconds (zero-padded to 6 digits).
    let frac_micros: i64 = match frac_str {
        Some(f) if !f.is_empty() => {
            // Pad or truncate to 6 digits.
            let padded = if f.len() >= 6 { &f[..6] } else { f };
            let mut val: i64 = padded.parse().ok()?;
            // Pad with trailing zeros if fewer than 6 digits.
            for _ in 0..(6 - padded.len()) {
                val *= 10;
            }
            val
        }
        _ => 0,
    };

    let day_micros = i64::from(days) * 86_400_000_000;
    let time_micros = hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + frac_micros;
    Some(day_micros + time_micros)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::arrow::datatypes::TimeUnit;

    /// Helper: build a `RelationInfo` for a two-column table (id int4, name text).
    fn test_relation() -> RelationInfo {
        RelationInfo::new(
            16385,
            "public".to_string(),
            "users".to_string(),
            vec![
                ColumnDef {
                    flags: 1,
                    name: "id".to_string(),
                    type_oid: 23, // int4
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "name".to_string(),
                    type_oid: 25, // text
                    type_modifier: -1,
                },
            ],
        )
    }

    #[test]
    fn encode_insert_row() {
        let rel = test_relation();
        let rows = vec![CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![
                    ColumnValue::Text("42".to_string()),
                    ColumnValue::Text("Alice".to_string()),
                ],
            },
        }];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // 2 data columns + 1 _rb_op column
        assert_eq!(batch.num_columns(), 3);

        // Verify schema field names
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(2).name(), "_rb_op");
    }

    #[test]
    fn encode_null_and_toast() {
        let rel = test_relation();
        let rows = vec![CdcRow {
            op: CdcOp::Update,
            tuple: TupleData {
                columns: vec![ColumnValue::Null, ColumnValue::UnchangedToast],
            },
        }];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Both data columns should be null.
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));

        // _rb_op should not be null.
        assert!(!batch.column(2).is_null(0));
    }

    #[test]
    fn encode_multiple_rows() {
        let rel = test_relation();
        let rows = vec![
            CdcRow {
                op: CdcOp::Insert,
                tuple: TupleData {
                    columns: vec![
                        ColumnValue::Text("1".to_string()),
                        ColumnValue::Text("Alice".to_string()),
                    ],
                },
            },
            CdcRow {
                op: CdcOp::Delete,
                tuple: TupleData {
                    columns: vec![
                        ColumnValue::Text("2".to_string()),
                        ColumnValue::Text("Bob".to_string()),
                    ],
                },
            },
        ];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn relation_info_schema_has_rb_op() {
        let rel = test_relation();
        let schema = &rel.arrow_schema;

        // 2 data columns + _rb_op
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(2).name(), "_rb_op");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn relation_info_maps_int4_to_int32() {
        let rel = test_relation();
        assert_eq!(*rel.arrow_schema.field(0).data_type(), DataType::Int32);
    }

    #[test]
    fn relation_info_maps_text_to_utf8() {
        let rel = test_relation();
        assert_eq!(*rel.arrow_schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn parse_date32_valid() {
        // 2024-01-15 → days since epoch
        let days = parse_date32("2024-01-15").unwrap();
        // 2024-01-15 is 19_737 days after 1970-01-01
        assert_eq!(days, 19_737);
    }

    #[test]
    fn parse_date32_epoch() {
        let days = parse_date32("1970-01-01").unwrap();
        assert_eq!(days, 0);
    }

    #[test]
    fn parse_timestamp_micros_no_frac() {
        let micros = parse_timestamp_micros("1970-01-01 00:00:00").unwrap();
        assert_eq!(micros, 0);
    }

    #[test]
    fn parse_timestamp_micros_with_frac() {
        let micros = parse_timestamp_micros("1970-01-01 00:00:01.500000").unwrap();
        assert_eq!(micros, 1_500_000);
    }

    #[test]
    fn parse_timestamp_micros_short_frac() {
        // "0.5" should be interpreted as 500000 microseconds
        let micros = parse_timestamp_micros("1970-01-01 00:00:00.5").unwrap();
        assert_eq!(micros, 500_000);
    }

    #[test]
    fn encode_boolean_column() {
        let rel = RelationInfo::new(
            100,
            "public".to_string(),
            "flags".to_string(),
            vec![ColumnDef {
                flags: 0,
                name: "active".to_string(),
                type_oid: 16, // bool
                type_modifier: -1,
            }],
        );

        let rows = vec![
            CdcRow {
                op: CdcOp::Insert,
                tuple: TupleData {
                    columns: vec![ColumnValue::Text("t".to_string())],
                },
            },
            CdcRow {
                op: CdcOp::Insert,
                tuple: TupleData {
                    columns: vec![ColumnValue::Text("f".to_string())],
                },
            },
        ];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(!batch.column(0).is_null(0));
        assert!(!batch.column(0).is_null(1));
    }

    #[test]
    fn encode_timestamp_column() {
        let rel = RelationInfo::new(
            101,
            "public".to_string(),
            "events".to_string(),
            vec![ColumnDef {
                flags: 0,
                name: "created_at".to_string(),
                type_oid: 1114, // timestamp
                type_modifier: -1,
            }],
        );

        let rows = vec![CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![ColumnValue::Text("2024-06-15 10:30:00.123456".to_string())],
            },
        }];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            *batch.schema().field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn encode_missing_columns_produce_nulls() {
        let rel = test_relation();
        // Tuple has only 1 column but relation expects 2 → second column is null.
        let rows = vec![CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![ColumnValue::Text("1".to_string())],
            },
        }];

        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column(0).is_null(0)); // id present
        assert!(batch.column(1).is_null(0)); // name missing → null
    }
}
