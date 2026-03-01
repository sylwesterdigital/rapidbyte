//! Arrow `RecordBatch` decoding helpers for INSERT and COPY write paths.
//!
//! Provides typed column extraction (one downcast per column per batch),
//! SQL parameter value extraction for INSERT, binary COPY serialization,
//! and shared helpers for column filtering and SQL building.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, TimestampMicrosecondArray,
};
use rapidbyte_sdk::arrow::datatypes::{DataType, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::*;
use tokio_postgres::types::ToSql;

/// Unix epoch date — base for Arrow Date32 day offsets.
static UNIX_EPOCH_DATE: LazyLock<NaiveDate> =
    LazyLock::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is always valid"));

// ── Qualified name helper ────────────────────────────────────────────

/// Build a schema-qualified table name: `"schema"."table"`.
#[must_use]
pub(crate) fn qualified_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

// ── Column filtering ─────────────────────────────────────────────────

/// Indices of Arrow columns that are not in the ignored set.
#[must_use]
pub(crate) fn active_column_indices(
    arrow_schema: &Arc<Schema>,
    ignored_columns: &HashSet<String>,
) -> Vec<usize> {
    (0..arrow_schema.fields().len())
        .filter(|&i| !ignored_columns.contains(arrow_schema.field(i).name()))
        .collect()
}

/// Build ON CONFLICT clause for upsert mode. Returns None for non-upsert modes.
#[must_use]
pub(crate) fn build_upsert_clause(
    write_mode: Option<&WriteMode>,
    arrow_schema: &Arc<Schema>,
    active_cols: &[usize],
) -> Option<String> {
    if let Some(WriteMode::Upsert { primary_key }) = write_mode {
        let pk_cols = primary_key
            .iter()
            .map(|k| quote_identifier(k))
            .collect::<Vec<_>>()
            .join(", ");

        let update_cols: Vec<String> = active_cols
            .iter()
            .map(|&i| arrow_schema.field(i).name())
            .filter(|name| !primary_key.contains(name))
            .map(|name| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_identifier(name),
                    quote_identifier(name)
                )
            })
            .collect();

        if update_cols.is_empty() {
            Some(format!(" ON CONFLICT ({pk_cols}) DO NOTHING"))
        } else {
            Some(format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                pk_cols,
                update_cols.join(", ")
            ))
        }
    } else {
        None
    }
}

/// Build type-null flags: true for columns whose type is incompatible (values forced to NULL).
#[must_use]
pub(crate) fn type_null_flags(
    active_cols: &[usize],
    arrow_schema: &Schema,
    type_null_columns: &HashSet<String>,
) -> Vec<bool> {
    active_cols
        .iter()
        .map(|&i| type_null_columns.contains(arrow_schema.field(i).name()))
        .collect()
}

// ── WriteTarget: pre-computed column metadata ───────────────────────

/// Pre-computed column metadata shared by INSERT and COPY write paths.
pub(crate) struct WriteTarget<'a> {
    pub(crate) table: &'a str,
    pub(crate) active_cols: &'a [usize],
    pub(crate) schema: &'a Arc<Schema>,
    pub(crate) type_null_flags: &'a [bool],
}

// ── TypedCol: pre-downcast Arrow columns ─────────────────────────────

/// Pre-downcast Arrow column reference. Eliminates per-cell `downcast_ref()` calls
/// by resolving the concrete array type once per column per batch.
#[derive(Debug)]
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a rapidbyte_sdk::arrow::array::StringArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Date32(&'a Date32Array),
    Binary(&'a BinaryArray),
    Null,
}

/// Pre-downcast active columns from a `RecordBatch` into `TypedCol` references.
///
/// # Errors
///
/// Returns `Err` if an Arrow column has an unsupported data type for the
/// `PostgreSQL` write path.
pub(crate) fn downcast_columns<'a>(
    batch: &'a RecordBatch,
    active_cols: &[usize],
) -> Result<Vec<TypedCol<'a>>, String> {
    active_cols
        .iter()
        .map(|&i| {
            let col = batch.column(i);
            match col.data_type() {
                DataType::Int16 => Ok(TypedCol::Int16(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int16Array)"))?)),
                DataType::Int32 => Ok(TypedCol::Int32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int32Array)"))?)),
                DataType::Int64 => Ok(TypedCol::Int64(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int64Array)"))?)),
                DataType::Float32 => Ok(TypedCol::Float32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Float32Array)"))?)),
                DataType::Float64 => Ok(TypedCol::Float64(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Float64Array)"))?)),
                DataType::Boolean => Ok(TypedCol::Boolean(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected BooleanArray)"))?)),
                DataType::Utf8 => Ok(TypedCol::Utf8(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected StringArray)"))?)),
                DataType::Timestamp(rapidbyte_sdk::arrow::datatypes::TimeUnit::Microsecond, _) => {
                    Ok(TypedCol::TimestampMicros(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected TimestampMicrosecondArray)"))?))
                }
                DataType::Date32 => Ok(TypedCol::Date32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Date32Array)"))?)),
                DataType::Binary => Ok(TypedCol::Binary(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected BinaryArray)"))?)),
                DataType::Null => Ok(TypedCol::Null),
                other => Err(format!(
                    "unsupported Arrow type for column {i}: {other:?}. Cast in a transform before writing to dest-postgres"
                )),
            }
        })
        .collect()
}

// ── SqlParamValue: typed INSERT bind parameters ──────────────────────

pub(crate) enum SqlParamValue<'a> {
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Boolean(Option<bool>),
    Text(Option<&'a str>),
    Timestamp(Option<NaiveDateTime>),
    Date(Option<NaiveDate>),
    Bytes(Option<&'a [u8]>),
}

impl SqlParamValue<'_> {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int16(v) => v,
            Self::Int32(v) => v,
            Self::Int64(v) => v,
            Self::Float32(v) => v,
            Self::Float64(v) => v,
            Self::Boolean(v) => v,
            Self::Text(v) => v,
            Self::Timestamp(v) => v,
            Self::Date(v) => v,
            Self::Bytes(v) => v,
        }
    }
}

/// Extract nullable value from a typed Arrow array into a `SqlParamValue`.
macro_rules! null_param {
    ($arr:expr, $row:expr, $variant:ident) => {
        if $arr.is_null($row) {
            SqlParamValue::$variant(None)
        } else {
            SqlParamValue::$variant(Some($arr.value($row)))
        }
    };
}

pub(crate) fn sql_param_value<'a>(col: &'a TypedCol<'a>, row_idx: usize) -> SqlParamValue<'a> {
    match col {
        TypedCol::Null => SqlParamValue::Text(None),
        TypedCol::Int16(arr) => null_param!(arr, row_idx, Int16),
        TypedCol::Int32(arr) => null_param!(arr, row_idx, Int32),
        TypedCol::Int64(arr) => null_param!(arr, row_idx, Int64),
        TypedCol::Float32(arr) => null_param!(arr, row_idx, Float32),
        TypedCol::Float64(arr) => null_param!(arr, row_idx, Float64),
        TypedCol::Boolean(arr) => null_param!(arr, row_idx, Boolean),
        TypedCol::Utf8(arr) => null_param!(arr, row_idx, Text),
        TypedCol::Binary(arr) => null_param!(arr, row_idx, Bytes),
        TypedCol::TimestampMicros(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Timestamp(None)
            } else {
                let micros = arr.value(row_idx);
                let secs = micros.div_euclid(1_000_000);
                // Safety: rem_euclid(1_000_000) * 1_000 yields max 999_999_000, well within u32::MAX.
                #[allow(clippy::cast_possible_truncation)]
                let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
                let dt = DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc());
                SqlParamValue::Timestamp(dt)
            }
        }
        TypedCol::Date32(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Date(None)
            } else {
                let days = arr.value(row_idx);
                let date =
                    UNIX_EPOCH_DATE.checked_add_signed(chrono::Duration::days(i64::from(days)));
                SqlParamValue::Date(date)
            }
        }
    }
}

// ── Binary COPY format ──────────────────────────────────────────────

/// `PostgreSQL` epoch offset: microseconds between 1970-01-01 and 2000-01-01.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// `PostgreSQL` epoch offset: days between 1970-01-01 and 2000-01-01.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// Binary NULL indicator: field length = -1.
const BINARY_NULL: [u8; 4] = (-1_i32).to_be_bytes();

/// Write a fixed-size numeric field in `PostgreSQL` binary format.
macro_rules! binary_fixed {
    ($arr:expr, $buf:expr, $row:expr, $len:literal) => {
        if $arr.is_null($row) {
            $buf.extend_from_slice(&BINARY_NULL);
        } else {
            $buf.extend_from_slice(&{ $len as i32 }.to_be_bytes());
            $buf.extend_from_slice(&$arr.value($row).to_be_bytes());
        }
    };
}

/// Write a single field in `PostgreSQL` binary COPY format.
///
/// Binary format per field: 4-byte length (i32, big-endian, -1 for NULL) + data bytes.
/// All integers use network byte order (big-endian). Timestamps are microseconds
/// since 2000-01-01, dates are days since 2000-01-01.
pub(crate) fn write_binary_field(buf: &mut Vec<u8>, col: &TypedCol<'_>, row_idx: usize) {
    match col {
        TypedCol::Null => buf.extend_from_slice(&BINARY_NULL),
        TypedCol::Int16(arr) => binary_fixed!(arr, buf, row_idx, 2_i32),
        TypedCol::Int32(arr) => binary_fixed!(arr, buf, row_idx, 4_i32),
        TypedCol::Int64(arr) => binary_fixed!(arr, buf, row_idx, 8_i32),
        TypedCol::Float32(arr) => binary_fixed!(arr, buf, row_idx, 4_i32),
        TypedCol::Float64(arr) => binary_fixed!(arr, buf, row_idx, 8_i32),
        TypedCol::Boolean(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(&BINARY_NULL);
            } else {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(u8::from(arr.value(row_idx)));
            }
        }
        TypedCol::Utf8(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(&BINARY_NULL);
            } else {
                let val = arr.value(row_idx).as_bytes();
                // Safety: PG COPY binary field lengths are i32; individual field values
                // in a RecordBatch will never exceed i32::MAX (~2 GB).
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let len = val.len() as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(val);
            }
        }
        TypedCol::TimestampMicros(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(&BINARY_NULL);
            } else {
                buf.extend_from_slice(&8_i32.to_be_bytes());
                let pg_micros = arr.value(row_idx) - PG_EPOCH_OFFSET_MICROS;
                buf.extend_from_slice(&pg_micros.to_be_bytes());
            }
        }
        TypedCol::Date32(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(&BINARY_NULL);
            } else {
                buf.extend_from_slice(&4_i32.to_be_bytes());
                let pg_days = arr.value(row_idx) - PG_EPOCH_OFFSET_DAYS;
                buf.extend_from_slice(&pg_days.to_be_bytes());
            }
        }
        TypedCol::Binary(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(&BINARY_NULL);
            } else {
                let val = arr.value(row_idx);
                // Safety: PG COPY binary field lengths are i32; individual field values
                // in a RecordBatch will never exceed i32::MAX (~2 GB).
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let len = val.len() as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, StringArray,
        TimestampMicrosecondArray,
    };
    use rapidbyte_sdk::arrow::datatypes::{Field, TimeUnit};

    // ── qualified_name ───────────────────────────────────────────────

    #[test]
    fn qualified_name_formats_correctly() {
        assert_eq!(qualified_name("raw", "users"), "raw.users");
        assert_eq!(qualified_name("public", "orders"), "public.orders");
    }

    // ── active_column_indices ────────────────────────────────────────

    #[test]
    fn active_column_indices_respects_ignored_set() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ignored = HashSet::from(["name".to_string()]);
        assert_eq!(active_column_indices(&schema, &ignored), vec![0]);
    }

    // ── build_upsert_clause ──────────────────────────────────────────

    #[test]
    fn build_upsert_clause_generates_on_conflict() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let clause = build_upsert_clause(Some(&mode), &schema, &[0, 1]).expect("upsert clause");
        assert_eq!(
            clause,
            " ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        );
    }

    #[test]
    fn build_upsert_clause_none_for_append() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(build_upsert_clause(Some(&WriteMode::Append), &schema, &[0]).is_none());
        assert!(build_upsert_clause(None, &schema, &[0]).is_none());
    }

    // ── type_null_flags ──────────────────────────────────────────────

    #[test]
    fn type_null_flags_marks_incompatible_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]);
        let nulls = HashSet::from(["name".to_string()]);
        let flags = type_null_flags(&[0, 1, 2], &schema, &nulls);
        assert_eq!(flags, vec![false, true, false]);
    }

    // ── TypedCol + SqlParamValue ─────────────────────────────────────

    #[test]
    fn sql_param_value_handles_numeric_and_nulls() {
        let arr = Int32Array::from(vec![Some(7), None]);
        let col = TypedCol::Int32(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Int32(Some(v)) => assert_eq!(v, 7),
            _ => panic!("expected Int32(Some(7))"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Int32(None) => {}
            _ => panic!("expected Int32(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_utf8() {
        let arr = StringArray::from(vec![Some("alice"), None]);
        let col = TypedCol::Utf8(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Text(Some(v)) => assert_eq!(v, "alice"),
            _ => panic!("expected Text(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Text(None) => {}
            _ => panic!("expected Text(None)"),
        }
    }

    #[test]
    fn downcast_columns_handles_timestamp_date_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("d", DataType::Date32, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let ts_arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64)]);
        let d_arr = Date32Array::from(vec![Some(19737)]);
        let b_arr = BinaryArray::from(vec![Some(b"test" as &[u8])]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts_arr), Arc::new(d_arr), Arc::new(b_arr)],
        )
        .unwrap();
        let cols = downcast_columns(&batch, &[0, 1, 2]).unwrap();
        assert!(matches!(cols[0], TypedCol::TimestampMicros(_)));
        assert!(matches!(cols[1], TypedCol::Date32(_)));
        assert!(matches!(cols[2], TypedCol::Binary(_)));
    }

    #[test]
    fn downcast_columns_rejects_unsupported_types() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::LargeUtf8, true)]));
        let arr = rapidbyte_sdk::arrow::array::LargeStringArray::from(vec![Some("alice")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let err = downcast_columns(&batch, &[0]).expect_err("LargeUtf8 should be rejected");
        assert!(err.contains("unsupported Arrow type"));
    }

    #[test]
    fn sql_param_value_handles_timestamp() {
        let arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64), None]);
        let col = TypedCol::TimestampMicros(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Timestamp(Some(dt)) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2024-01-15 09:50:00"
                );
            }
            _ => panic!("expected Timestamp(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Timestamp(None) => {}
            _ => panic!("expected Timestamp(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_date32() {
        let arr = Date32Array::from(vec![Some(19737), None]);
        let col = TypedCol::Date32(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Date(Some(d)) => assert_eq!(d.to_string(), "2024-01-15"),
            _ => panic!("expected Date(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Date(None) => {}
            _ => panic!("expected Date(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_binary() {
        let arr = BinaryArray::from(vec![Some(b"hello" as &[u8]), None]);
        let col = TypedCol::Binary(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Bytes(Some(b)) => assert_eq!(b, b"hello"),
            _ => panic!("expected Bytes(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Bytes(None) => {}
            _ => panic!("expected Bytes(None)"),
        }
    }

    // ── Binary COPY format ──────────────────────────────────────────

    #[test]
    fn binary_field_int16() {
        let arr = Int16Array::from(vec![Some(256_i16), None]);
        let col = TypedCol::Int16(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        assert_eq!(buf, [0, 0, 0, 2, 1, 0]);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 1);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn binary_field_int32() {
        let arr = Int32Array::from(vec![Some(42)]);
        let col = TypedCol::Int32(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        assert_eq!(buf, [0, 0, 0, 4, 0, 0, 0, 42]);
    }

    #[test]
    fn binary_field_int64() {
        let arr = Int64Array::from(vec![Some(1_000_000)]);
        let col = TypedCol::Int64(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        let mut expected = vec![0, 0, 0, 8];
        expected.extend_from_slice(&1_000_000_i64.to_be_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn binary_field_float64() {
        let arr = Float64Array::from(vec![Some(3.14)]);
        let col = TypedCol::Float64(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        let mut expected = vec![0, 0, 0, 8];
        expected.extend_from_slice(&3.14_f64.to_be_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn binary_field_boolean() {
        let arr = BooleanArray::from(vec![Some(true), Some(false)]);
        let col = TypedCol::Boolean(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        assert_eq!(buf, [0, 0, 0, 1, 1]);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 1);
        assert_eq!(buf, [0, 0, 0, 1, 0]);
    }

    #[test]
    fn binary_field_utf8() {
        let arr = StringArray::from(vec![Some("hello")]);
        let col = TypedCol::Utf8(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        assert_eq!(buf, [0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn binary_field_timestamp_micros() {
        let arrow_micros = 1705312200_i64 * 1_000_000;
        let pg_micros = arrow_micros - 946_684_800_000_000;
        let arr = TimestampMicrosecondArray::from(vec![Some(arrow_micros)]);
        let col = TypedCol::TimestampMicros(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        let mut expected = vec![0, 0, 0, 8];
        expected.extend_from_slice(&pg_micros.to_be_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn binary_field_date32() {
        let arrow_days = 19737;
        let pg_days = arrow_days - 10957;
        let arr = Date32Array::from(vec![Some(arrow_days)]);
        let col = TypedCol::Date32(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        let mut expected = vec![0, 0, 0, 4];
        expected.extend_from_slice(&pg_days.to_be_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn binary_field_bytea() {
        let arr = BinaryArray::from(vec![Some(&[0xDE_u8, 0xAD, 0xBE, 0xEF] as &[u8])]);
        let col = TypedCol::Binary(&arr);
        let mut buf = Vec::new();
        write_binary_field(&mut buf, &col, 0);
        assert_eq!(buf, [0, 0, 0, 4, 0xDE, 0xAD, 0xBE, 0xEF]);
    }
}
