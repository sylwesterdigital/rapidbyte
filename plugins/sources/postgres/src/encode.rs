//! Arrow `RecordBatch` encoding from `PostgreSQL` rows.
//!
//! Converts `tokio_postgres::Row` slices into Arrow `RecordBatch` using the
//! `Column` type registry. Each `Column` carries its `ArrowDataType` and PG
//! type name, eliminating the need for a parallel `pg_types` array.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rapidbyte_sdk::arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::{build_arrow_schema, ArrowDataType};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::LazyLock;
use tokio_postgres::Row;

use crate::types::Column;

/// Unix epoch date -- used as the base for Arrow Date32 day offsets.
static UNIX_EPOCH_DATE: LazyLock<NaiveDate> =
    LazyLock::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is always valid"));

/// Build an Arrow `Schema` from column definitions.
///
/// Delegates to the SDK's `build_arrow_schema` so the type conversion stays
/// in one place.
pub fn arrow_schema(columns: &[Column]) -> Arc<Schema> {
    let col_schemas: Vec<_> = columns.iter().map(Column::to_schema).collect();
    build_arrow_schema(&col_schemas)
}

pub struct InvalidRow {
    pub message: String,
    pub record_json: String,
}

pub struct DecodedRow {
    values: Vec<Option<CellValue>>,
    pub estimated_bytes: usize,
}

pub struct RecordBatchBuilder {
    builders: Vec<ColumnBuilder>,
    row_count: usize,
}

impl RecordBatchBuilder {
    pub fn new(columns: &[Column], capacity: usize) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ColumnBuilder::new(column, capacity))
                .collect(),
            row_count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn append_decoded_row(&mut self, row: DecodedRow) -> Result<(), String> {
        for (builder, value) in self.builders.iter_mut().zip(row.values) {
            builder.append(value)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn finish(self, schema: &Arc<Schema>) -> Result<Option<RecordBatch>, String> {
        if self.row_count == 0 {
            return Ok(None);
        }

        let arrays: Vec<Arc<dyn Array>> = self.builders.into_iter().map(ColumnBuilder::finish).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| format!("failed to create RecordBatch: {e}"))?;
        Ok(Some(batch))
    }
}

enum CellValue {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    TimestampMicros(i64),
    Date32(i32),
    Binary(Vec<u8>),
    Utf8(String),
}

pub fn decode_row(row: &Row, columns: &[Column], row_index: usize) -> Result<DecodedRow, InvalidRow> {
    let mut values = Vec::with_capacity(columns.len());
    let mut estimated_bytes = 0usize;

    for (column_index, column) in columns.iter().enumerate() {
        match read_cell(row, column_index, column) {
            Ok(value) => {
                estimated_bytes += estimate_cell_bytes(value.as_ref());
                estimated_bytes += 1;
                values.push(value);
            }
            Err(error) => {
                return Err(InvalidRow {
                    message: format!("row {row_index} decode failed: {error}"),
                    record_json: row_to_json_best_effort(row, columns),
                });
            }
        }
    }

    Ok(DecodedRow {
        values,
        estimated_bytes,
    })
}

enum ColumnBuilder {
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
    Date32(Date32Builder),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(column: &Column, capacity: usize) -> Self {
        match column.arrow_type {
            ArrowDataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            ArrowDataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            ArrowDataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ArrowDataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            ArrowDataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            ArrowDataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            ArrowDataType::TimestampMicros => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ArrowDataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            ArrowDataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64)),
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append(&mut self, value: Option<CellValue>) -> Result<(), String> {
        match (self, value) {
            (Self::Int16(builder), Some(CellValue::Int16(value))) => builder.append_value(value),
            (Self::Int16(builder), None) => builder.append_null(),
            (Self::Int32(builder), Some(CellValue::Int32(value))) => builder.append_value(value),
            (Self::Int32(builder), None) => builder.append_null(),
            (Self::Int64(builder), Some(CellValue::Int64(value))) => builder.append_value(value),
            (Self::Int64(builder), None) => builder.append_null(),
            (Self::Float32(builder), Some(CellValue::Float32(value))) => builder.append_value(value),
            (Self::Float32(builder), None) => builder.append_null(),
            (Self::Float64(builder), Some(CellValue::Float64(value))) => builder.append_value(value),
            (Self::Float64(builder), None) => builder.append_null(),
            (Self::Boolean(builder), Some(CellValue::Boolean(value))) => builder.append_value(value),
            (Self::Boolean(builder), None) => builder.append_null(),
            (Self::TimestampMicros(builder), Some(CellValue::TimestampMicros(value))) => {
                builder.append_value(value)
            }
            (Self::TimestampMicros(builder), None) => builder.append_null(),
            (Self::Date32(builder), Some(CellValue::Date32(value))) => builder.append_value(value),
            (Self::Date32(builder), None) => builder.append_null(),
            (Self::Binary(builder), Some(CellValue::Binary(value))) => builder.append_value(value),
            (Self::Binary(builder), None) => builder.append_null(),
            (Self::Utf8(builder), Some(CellValue::Utf8(value))) => builder.append_value(value),
            (Self::Utf8(builder), None) => builder.append_null(),
            (builder, Some(value)) => {
                return Err(format!(
                    "internal type mismatch while appending {:?} into {:?}",
                    cell_value_kind(&value),
                    builder.kind_name()
                ))
            }
        }
        Ok(())
    }

    fn finish(self) -> Arc<dyn Array> {
        match self {
            Self::Int16(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::TimestampMicros(mut builder) => Arc::new(builder.finish()),
            Self::Date32(mut builder) => Arc::new(builder.finish()),
            Self::Binary(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Int16(_) => "int16",
            Self::Int32(_) => "int32",
            Self::Int64(_) => "int64",
            Self::Float32(_) => "float32",
            Self::Float64(_) => "float64",
            Self::Boolean(_) => "boolean",
            Self::TimestampMicros(_) => "timestamp_micros",
            Self::Date32(_) => "date32",
            Self::Binary(_) => "binary",
            Self::Utf8(_) => "utf8",
        }
    }
}

fn cell_value_kind(value: &CellValue) -> &'static str {
    match value {
        CellValue::Int16(_) => "int16",
        CellValue::Int32(_) => "int32",
        CellValue::Int64(_) => "int64",
        CellValue::Float32(_) => "float32",
        CellValue::Float64(_) => "float64",
        CellValue::Boolean(_) => "boolean",
        CellValue::TimestampMicros(_) => "timestamp_micros",
        CellValue::Date32(_) => "date32",
        CellValue::Binary(_) => "binary",
        CellValue::Utf8(_) => "utf8",
    }
}

pub fn row_to_json(row: &Row, columns: &[Column]) -> String {
    row_to_json_best_effort(row, columns)
}

fn read_cell(row: &Row, idx: usize, col: &Column) -> Result<Option<CellValue>, String> {
    match col.arrow_type {
        ArrowDataType::Int16 => read_typed::<i16>(row, idx, col).map(|v| v.map(CellValue::Int16)),
        ArrowDataType::Int32 => read_typed::<i32>(row, idx, col).map(|v| v.map(CellValue::Int32)),
        ArrowDataType::Int64 => read_typed::<i64>(row, idx, col).map(|v| v.map(CellValue::Int64)),
        ArrowDataType::Float32 => {
            read_typed::<f32>(row, idx, col).map(|v| v.map(CellValue::Float32))
        }
        ArrowDataType::Float64 => {
            read_typed::<f64>(row, idx, col).map(|v| v.map(CellValue::Float64))
        }
        ArrowDataType::Boolean => {
            read_typed::<bool>(row, idx, col).map(|v| v.map(CellValue::Boolean))
        }
        ArrowDataType::TimestampMicros => read_timestamp(row, idx, col)
            .map(|v| v.map(CellValue::TimestampMicros)),
        ArrowDataType::Date32 => read_date32(row, idx, col).map(|v| v.map(CellValue::Date32)),
        ArrowDataType::Binary => {
            read_typed::<Vec<u8>>(row, idx, col).map(|v| v.map(CellValue::Binary))
        }
        _ if col.is_json() => read_typed::<serde_json::Value>(row, idx, col)
            .map(|v| v.map(|json| CellValue::Utf8(json.to_string()))),
        _ => read_typed::<String>(row, idx, col).map(|v| v.map(CellValue::Utf8)),
    }
}

fn read_typed<T>(row: &Row, idx: usize, col: &Column) -> Result<Option<T>, String>
where
    for<'a> T: tokio_postgres::types::FromSql<'a>,
{
    row.try_get::<_, Option<T>>(idx).map_err(|error| {
        format!(
            "column '{}' (pg_type='{}', arrow_type={:?}) decode error: {error}",
            col.name, col.pg_type, col.arrow_type
        )
    })
}

fn read_timestamp(row: &Row, idx: usize, col: &Column) -> Result<Option<i64>, String> {
    match col.pg_type.as_str() {
        "timestamp" | "timestamp without time zone" => {
            read_typed::<NaiveDateTime>(row, idx, col)
                .map(|value| value.map(|dt| dt.and_utc().timestamp_micros()))
        }
        "timestamp with time zone" | "timestamptz" => read_typed::<DateTime<Utc>>(row, idx, col)
            .map(|value| value.map(|dt| dt.timestamp_micros())),
        _ => read_typed::<NaiveDateTime>(row, idx, col)
            .map(|value| value.map(|dt| dt.and_utc().timestamp_micros()))
            .or_else(|_| {
                read_typed::<DateTime<Utc>>(row, idx, col)
                    .map(|value| value.map(|dt| dt.timestamp_micros()))
            }),
    }
}

fn read_date32(row: &Row, idx: usize, col: &Column) -> Result<Option<i32>, String> {
    read_typed::<NaiveDate>(row, idx, col).map(|value| {
        value.map(|date| {
            #[allow(clippy::cast_possible_truncation)]
            let days = (date - *UNIX_EPOCH_DATE).num_days() as i32;
            days
        })
    })
}

fn row_to_json_best_effort(row: &Row, columns: &[Column]) -> String {
    let mut object = JsonMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let value = match read_cell(row, idx, column) {
            Ok(Some(value)) => cell_value_to_json(value),
            Ok(None) => JsonValue::Null,
            Err(error) => JsonValue::String(format!("<decode error: {error}>")),
        };
        object.insert(column.name.clone(), value);
    }
    JsonValue::Object(object).to_string()
}

fn cell_value_to_json(value: CellValue) -> JsonValue {
    match value {
        CellValue::Int16(value) => JsonValue::from(i64::from(value)),
        CellValue::Int32(value) => JsonValue::from(i64::from(value)),
        CellValue::Int64(value) => JsonValue::from(value),
        CellValue::Float32(value) => JsonValue::from(f64::from(value)),
        CellValue::Float64(value) => JsonValue::from(value),
        CellValue::Boolean(value) => JsonValue::from(value),
        CellValue::TimestampMicros(value) => JsonValue::from(value),
        CellValue::Date32(value) => JsonValue::from(i64::from(value)),
        CellValue::Binary(value) => JsonValue::Array(
            value
                .into_iter()
                .map(|byte| JsonValue::from(u64::from(byte)))
                .collect(),
        ),
        CellValue::Utf8(value) => JsonValue::from(value),
    }
}

fn estimate_cell_bytes(value: Option<&CellValue>) -> usize {
    match value {
        None => 0,
        Some(CellValue::Int16(_)) => 2,
        Some(CellValue::Int32(_)) | Some(CellValue::Float32(_)) | Some(CellValue::Date32(_)) => 4,
        Some(CellValue::Int64(_))
        | Some(CellValue::Float64(_))
        | Some(CellValue::TimestampMicros(_)) => 8,
        Some(CellValue::Boolean(_)) => 1,
        Some(CellValue::Binary(value)) => value.len(),
        Some(CellValue::Utf8(value)) => value.len(),
    }
}
