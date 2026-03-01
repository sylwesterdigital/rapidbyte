//! Rule evaluation engine for the validation transform.
//!
//! Evaluates compiled validation rules against each Arrow `RecordBatch`, partitions
//! rows into valid/invalid sets, emits metrics, and routes invalid rows according
//! to the stream's error policy (fail, skip, or dead-letter queue).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use ::arrow::compute::take;
use ::arrow::datatypes::DataType;
use ::arrow::util::display::array_value_to_string;
use arrow_json::LineDelimitedWriter;
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::DataErrorPolicy;

use crate::config::{CompiledConfig, CompiledRule, NumericBound};

#[derive(Debug, Clone)]
struct RuleFailure {
    rule: &'static str,
    field: String,
    message: String,
}

/// Run the validation transform for a single stream.
///
/// # Errors
///
/// Returns `Err` if a failing row is encountered and the stream error policy is `Fail`.
pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    config: &CompiledConfig,
) -> Result<TransformSummary, ConnectorError> {
    let mut records_in: u64 = 0;
    let mut records_out: u64 = 0;
    let mut bytes_in: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut batches_processed: u64 = 0;

    while let Some((_schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            batches_processed += 1;
            records_in += batch.num_rows() as u64;
            bytes_in += batch.get_array_memory_size() as u64;

            let evaluation = evaluate_batch(batch, config);
            emit_validation_metrics(ctx, &evaluation)?;

            if !evaluation.valid_indices.is_empty() {
                let filtered = select_rows(batch, &evaluation.valid_indices)?;
                records_out += filtered.num_rows() as u64;
                bytes_out += filtered.get_array_memory_size() as u64;
                ctx.emit_batch(&filtered)?;
            }

            if !evaluation.invalid_rows.is_empty() {
                match stream.policies.on_data_error {
                    DataErrorPolicy::Fail => {
                        let first = &evaluation.invalid_rows[0];
                        return Err(ConnectorError::data(
                            "VALIDATION_FAILED",
                            format!(
                                "validation failed for {} row(s); first failure at row {}: {}",
                                evaluation.invalid_rows.len(),
                                first.row_index,
                                first.message
                            ),
                        ));
                    }
                    DataErrorPolicy::Skip => {}
                    DataErrorPolicy::Dlq => {
                        for invalid in &evaluation.invalid_rows {
                            let record_json = row_to_json(batch, invalid.row_index)?;
                            ctx.emit_dlq_record(
                                &record_json,
                                &invalid.message,
                                rapidbyte_sdk::error::ErrorCategory::Data,
                            )?;
                        }
                    }
                }
            }
        }
    }

    ctx.log(
        LogLevel::Info,
        &format!(
            "Validation transform complete: {} rows in, {} rows out, {} batches",
            records_in, records_out, batches_processed
        ),
    );

    Ok(TransformSummary {
        records_in,
        records_out,
        bytes_in,
        bytes_out,
        batches_processed,
    })
}

struct InvalidRow {
    row_index: usize,
    message: String,
}

struct BatchEvaluation {
    valid_indices: Vec<u32>,
    invalid_rows: Vec<InvalidRow>,
    failure_counts: BTreeMap<(String, String), u64>,
}

enum NumericCellValue {
    Float(f64),
    Decimal(BigDecimal),
}

fn evaluate_batch(batch: &RecordBatch, config: &CompiledConfig) -> BatchEvaluation {
    let field_to_index = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), idx))
        .collect::<HashMap<_, _>>();

    let unique_counts = build_unique_counts(batch, config, &field_to_index);

    let mut valid_indices = Vec::with_capacity(batch.num_rows());
    let mut invalid_rows = Vec::new();
    let mut failure_counts = BTreeMap::new();

    for row in 0..batch.num_rows() {
        let mut failures = Vec::with_capacity(config.rules.len());

        for rule in &config.rules {
            if let Some(failure) = evaluate_rule(rule, batch, row, &field_to_index, &unique_counts)
            {
                failures.push(failure);
            }
        }

        if failures.is_empty() {
            valid_indices.push(row as u32);
        } else {
            for failure in &failures {
                *failure_counts
                    .entry((failure.rule.to_string(), failure.field.clone()))
                    .or_default() += 1;
            }
            invalid_rows.push(InvalidRow {
                row_index: row,
                message: failures
                    .into_iter()
                    .map(|f| f.message)
                    .collect::<Vec<_>>()
                    .join("; "),
            });
        }
    }

    BatchEvaluation {
        valid_indices,
        invalid_rows,
        failure_counts,
    }
}

fn build_unique_counts(
    batch: &RecordBatch,
    config: &CompiledConfig,
    field_to_index: &HashMap<String, usize>,
) -> HashMap<String, HashMap<String, usize>> {
    let mut unique_counts: HashMap<String, HashMap<String, usize>> = HashMap::new();
    for rule in &config.rules {
        if let CompiledRule::Unique { field } = rule {
            let mut counts = HashMap::new();
            if let Some(index) = field_to_index.get(field) {
                let column = batch.column(*index);
                for row in 0..batch.num_rows() {
                    let key = unique_key(column.as_ref(), row);
                    *counts.entry(key).or_insert(0) += 1;
                }
            }
            unique_counts.insert(field.clone(), counts);
        }
    }
    unique_counts
}

fn evaluate_rule(
    rule: &CompiledRule,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
    unique_counts: &HashMap<String, HashMap<String, usize>>,
) -> Option<RuleFailure> {
    match rule {
        CompiledRule::NotNull { field } => evaluate_not_null_rule(field, batch, row, field_to_index),
        CompiledRule::Regex {
            field,
            pattern,
            regex,
        } => evaluate_regex_rule(field, pattern, regex, batch, row, field_to_index),
        CompiledRule::Range { field, min, max } => {
            evaluate_range_rule(field, min, max, batch, row, field_to_index)
        }
        CompiledRule::Unique { field } => {
            evaluate_unique_rule(field, batch, row, field_to_index, unique_counts)
        }
    }
}

fn evaluate_not_null_rule(
    field: &str,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "not_null",
            field: field.to_string(),
            message: format!("assert_not_null({field}) failed: column missing"),
        });
    };
    if batch.column(*index).is_null(row) {
        return Some(RuleFailure {
            rule: "not_null",
            field: field.to_string(),
            message: format!("assert_not_null({field}) failed: value is null"),
        });
    }
    None
}

fn evaluate_regex_rule(
    field: &str,
    pattern: &str,
    regex: &regex::Regex,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: column missing"),
        });
    };
    let column = batch.column(*index);
    if column.is_null(row) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: value is null"),
        });
    }
    if !matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: field is not string"),
        });
    }
    let value = string_value(column.as_ref(), row).unwrap_or_default();
    if !regex.is_match(value) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!(
                "assert_regex({field}, {pattern}) failed: value '{value}' does not match"
            ),
        });
    }
    None
}

fn evaluate_range_rule(
    field: &str,
    min: &Option<NumericBound>,
    max: &Option<NumericBound>,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: column missing"),
        });
    };
    let column = batch.column(*index);
    if column.is_null(row) {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: value is null"),
        });
    }

    let Some(numeric_value) = numeric_value(column.as_ref(), row) else {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: field is not numeric"),
        });
    };
    match numeric_value {
        NumericCellValue::Float(value) => {
            if !value.is_finite() {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!("assert_range({field}) failed: value is non-finite number"),
                });
            }
            if value_is_out_of_float_bounds(value, min, max) {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!(
                        "assert_range({field}) failed: value {} outside bounds {}",
                        display_value(column.as_ref(), row),
                        format_bounds(min, max)
                    ),
                });
            }
        }
        NumericCellValue::Decimal(value) => {
            if value_is_out_of_decimal_bounds(&value, min, max) {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!(
                        "assert_range({field}) failed: value {} outside bounds {}",
                        display_value(column.as_ref(), row),
                        format_bounds(min, max)
                    ),
                });
            }
        }
    }
    None
}

fn evaluate_unique_rule(
    field: &str,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
    unique_counts: &HashMap<String, HashMap<String, usize>>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "unique",
            field: field.to_string(),
            message: format!("assert_unique({field}) failed: column missing"),
        });
    };
    let column = batch.column(*index);
    let key = unique_key(column.as_ref(), row);
    if unique_counts
        .get(field)
        .and_then(|counts| counts.get(&key))
        .copied()
        .unwrap_or_default()
        > 1
    {
        return Some(RuleFailure {
            rule: "unique",
            field: field.to_string(),
            message: format!(
                "assert_unique({field}) failed: duplicate value {}",
                display_value(column.as_ref(), row)
            ),
        });
    }
    None
}

fn emit_validation_metrics(ctx: &Context, evaluation: &BatchEvaluation) -> Result<(), ConnectorError> {
    for metric in build_validation_metrics(evaluation) {
        ctx.metric(&metric)?;
    }
    Ok(())
}

fn build_validation_metrics(evaluation: &BatchEvaluation) -> Vec<Metric> {
    let mut metrics = Vec::new();
    for ((rule, field), count) in &evaluation.failure_counts {
        metrics.push(Metric {
            name: "validation_failures_total".to_string(),
            value: MetricValue::Counter(*count),
            labels: vec![
                ("rule".to_string(), rule.clone()),
                ("field".to_string(), field.clone()),
            ],
        });
    }
    metrics.push(Metric {
        name: "validation_rows_valid_total".to_string(),
        value: MetricValue::Counter(evaluation.valid_indices.len() as u64),
        labels: Vec::new(),
    });
    metrics.push(Metric {
        name: "validation_rows_invalid_total".to_string(),
        value: MetricValue::Counter(evaluation.invalid_rows.len() as u64),
        labels: Vec::new(),
    });
    metrics
}

fn select_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ConnectorError> {
    let idx = UInt32Array::from(indices.to_vec());
    let idx_ref = &idx as &dyn Array;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let filtered = take(column.as_ref(), idx_ref, None).map_err(|e| {
            ConnectorError::internal("VALIDATE_FILTER", format!("failed to filter rows: {e}"))
        })?;
        arrays.push(filtered);
    }
    RecordBatch::try_new(Arc::clone(&batch.schema()), arrays).map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_BATCH",
            format!("failed to build filtered record batch: {e}"),
        )
    })
}

fn row_to_json(batch: &RecordBatch, row: usize) -> Result<String, ConnectorError> {
    let single = select_rows(batch, &[row as u32])?;
    let mut writer = LineDelimitedWriter::new(Vec::new());
    writer.write_batches(&[&single]).map_err(|e| {
        ConnectorError::internal("VALIDATE_DLQ_JSON", format!("failed to encode DLQ row: {e}"))
    })?;
    writer.finish().map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_DLQ_JSON",
            format!("failed to finalize DLQ row json: {e}"),
        )
    })?;
    let mut text = String::from_utf8(writer.into_inner()).map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_DLQ_JSON",
            format!("dlq json was not utf8: {e}"),
        )
    })?;
    while text.ends_with('\n') {
        text.pop();
    }
    Ok(text)
}

fn string_value<'a>(array: &'a dyn Array, row: usize) -> Option<&'a str> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        Some(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        Some(values.value(row))
    } else {
        None
    }
}

fn numeric_value(array: &dyn Array, row: usize) -> Option<NumericCellValue> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row) as f64)),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row) as f64)),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row))),
        DataType::Decimal128(_, scale) => {
            let values = array.as_any().downcast_ref::<Decimal128Array>()?;
            Some(NumericCellValue::Decimal(decimal128_to_decimal(
                values.value(row),
                *scale,
            )))
        }
        DataType::Decimal256(_, _scale) => {
            let rendered = array_value_to_string(array, row).ok()?;
            rendered.parse::<BigDecimal>().ok().map(NumericCellValue::Decimal)
        }
        _ => None,
    }
}

fn decimal128_to_decimal(value: i128, scale: i8) -> BigDecimal {
    BigDecimal::new(BigInt::from(value), i64::from(scale))
}

fn value_is_out_of_float_bounds(value: f64, min: &Option<NumericBound>, max: &Option<NumericBound>) -> bool {
    min.as_ref().is_some_and(|bound| value < bound.as_f64)
        || max.as_ref().is_some_and(|bound| value > bound.as_f64)
}

fn value_is_out_of_decimal_bounds(
    value: &BigDecimal,
    min: &Option<NumericBound>,
    max: &Option<NumericBound>,
) -> bool {
    min.as_ref().is_some_and(|bound| value < &bound.decimal)
        || max.as_ref().is_some_and(|bound| value > &bound.decimal)
}

fn format_bounds(min: &Option<NumericBound>, max: &Option<NumericBound>) -> String {
    let min_bound = min
        .as_ref()
        .map_or_else(|| "-inf".to_string(), |bound| bound.literal.clone());
    let max_bound = max
        .as_ref()
        .map_or_else(|| "+inf".to_string(), |bound| bound.literal.clone());
    format!("[{min_bound}, {max_bound}]")
}

fn display_value(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        "null".to_string()
    } else {
        array_value_to_string(array, row).unwrap_or_else(|_| "<unprintable>".to_string())
    }
}

fn unique_key(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        "__NULL__".to_string()
    } else {
        display_value(array, row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::{Decimal128Array, Decimal256Array, Float64Array, StringArray};
    use ::arrow::datatypes::{Field, Schema};
    use arrow_buffer::i256;

    fn bound(literal: &str) -> NumericBound {
        NumericBound {
            literal: literal.to_string(),
            decimal: literal.parse::<BigDecimal>().expect("bound should parse"),
            as_f64: literal.parse::<f64>().expect("bound should parse as f64"),
        }
    }

    #[test]
    fn evaluate_batch_collects_multiple_failures() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, true),
            Field::new("age", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(30.0), Some(300.0)])) as ArrayRef,
            ],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![
                CompiledRule::NotNull {
                    field: "email".to_string(),
                },
                CompiledRule::Regex {
                    field: "email".to_string(),
                    pattern: "^.+@.+$".to_string(),
                    regex: regex::Regex::new("^.+@.+$").expect("regex should compile"),
                },
                CompiledRule::Range {
                    field: "age".to_string(),
                    min: Some(bound("0")),
                    max: Some(bound("150")),
                },
            ],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices.len(), 0);
        assert_eq!(evaluation.invalid_rows.len(), 2);
        assert!(evaluation.invalid_rows[1].message.contains("assert_not_null"));
        assert!(evaluation.invalid_rows[1].message.contains("assert_regex"));
        assert!(evaluation.invalid_rows[1].message.contains("assert_range"));
    }

    #[test]
    fn evaluate_unique_flags_duplicate_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![None, None, Some("a"), Some("a")])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Unique {
                field: "id".to_string(),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert!(evaluation.valid_indices.is_empty());
        assert_eq!(evaluation.invalid_rows.len(), 4);
    }

    #[test]
    fn evaluate_decimal128_range_supports_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let values = Decimal128Array::from(vec![1000_i128, 2500_i128, 7000_i128])
            .with_data_type(DataType::Decimal128(10, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert_eq!(evaluation.invalid_rows.len(), 1);
    }

    #[test]
    fn evaluate_decimal256_range_supports_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(20, 2),
            false,
        )]));
        let values = Decimal256Array::from(vec![
            i256::from_i128(1000),
            i256::from_i128(2500),
            i256::from_i128(7000),
        ])
        .with_data_type(DataType::Decimal256(20, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert_eq!(evaluation.invalid_rows.len(), 1);
    }

    #[test]
    fn evaluate_range_rejects_non_finite_float_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("score", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                50.0,
            ])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "score".to_string(),
                min: Some(bound("0")),
                max: Some(bound("100")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![3]);
        assert_eq!(evaluation.invalid_rows.len(), 3);
        assert!(evaluation
            .invalid_rows
            .iter()
            .any(|row| row.message.contains("non-finite number")));
    }

    #[test]
    fn evaluate_decimal128_range_allows_exact_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let values = Decimal128Array::from(vec![1000_i128, 3000_i128])
            .with_data_type(DataType::Decimal128(10, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert!(evaluation.invalid_rows.is_empty());
    }

    #[test]
    fn evaluate_unique_and_metrics_count_all_duplicate_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("dup"),
                Some("dup"),
                Some("dup"),
                Some("ok"),
            ])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Unique {
                field: "id".to_string(),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![3]);
        assert_eq!(evaluation.invalid_rows.len(), 3);
        assert_eq!(
            evaluation
                .failure_counts
                .get(&("unique".to_string(), "id".to_string())),
            Some(&3)
        );

        let metrics = build_validation_metrics(&evaluation);
        let unique_failure = metrics
            .iter()
            .find(|m| {
                m.name == "validation_failures_total"
                    && m
                        .labels
                        .contains(&("rule".to_string(), "unique".to_string()))
                    && m.labels.contains(&("field".to_string(), "id".to_string()))
            })
            .expect("unique failure metric should exist");
        assert_eq!(unique_failure.value, MetricValue::Counter(3));
    }

    #[test]
    fn build_validation_metrics_includes_rule_and_field_labels() {
        let evaluation = BatchEvaluation {
            valid_indices: vec![0, 2],
            invalid_rows: vec![InvalidRow {
                row_index: 1,
                message: "bad".to_string(),
            }],
            failure_counts: BTreeMap::from([(("regex".to_string(), "email".to_string()), 3)]),
        };

        let metrics = build_validation_metrics(&evaluation);
        let failure = metrics
            .iter()
            .find(|m| m.name == "validation_failures_total")
            .expect("failure metric should exist");
        assert_eq!(
            failure.labels,
            vec![
                ("rule".to_string(), "regex".to_string()),
                ("field".to_string(), "email".to_string())
            ]
        );
    }

    #[test]
    fn evaluate_batch_reports_missing_columns_for_all_rules() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![
                CompiledRule::NotNull {
                    field: "missing_not_null".to_string(),
                },
                CompiledRule::Regex {
                    field: "missing_regex".to_string(),
                    pattern: "^.+$".to_string(),
                    regex: regex::Regex::new("^.+$").expect("regex should compile"),
                },
                CompiledRule::Range {
                    field: "missing_range".to_string(),
                    min: Some(bound("1")),
                    max: Some(bound("2")),
                },
                CompiledRule::Unique {
                    field: "missing_unique".to_string(),
                },
            ],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert!(evaluation.valid_indices.is_empty());
        assert_eq!(evaluation.invalid_rows.len(), 1);
        let message = &evaluation.invalid_rows[0].message;
        assert!(message.contains("assert_not_null(missing_not_null) failed: column missing"));
        assert!(message.contains("assert_regex(missing_regex, ^.+$) failed: column missing"));
        assert!(message.contains("assert_range(missing_range) failed: column missing"));
        assert!(message.contains("assert_unique(missing_unique) failed: column missing"));
    }

    #[test]
    fn evaluate_decimal_range_uses_exact_decimal_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 18),
            false,
        )]));
        let values = Decimal128Array::from(vec![
            100_000_000_000_000_000_i128,
            100_000_000_000_000_001_i128,
            99_999_999_999_999_999_i128,
        ])
        .with_data_type(DataType::Decimal128(38, 18));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("0.100000000000000000")),
                max: Some(bound("0.100000000000000000")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0]);
        assert_eq!(evaluation.invalid_rows.len(), 2);
    }
}
