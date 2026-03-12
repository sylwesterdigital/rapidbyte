//! Runtime batch handling for the validation transform.

use std::sync::Arc;

use ::arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use ::arrow::compute::take;
use arrow_json::LineDelimitedWriter;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::DataErrorPolicy;

use crate::config::CompiledConfig;
use crate::validate::{build_validation_metrics, evaluate_batch};

/// Run the validation transform for a single stream.
///
/// # Errors
///
/// Returns `Err` if a failing row is encountered and the stream error policy is `Fail`.
pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    config: &CompiledConfig,
) -> Result<TransformSummary, PluginError> {
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
                        return Err(PluginError::data(
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
                        let invalid_indices: Vec<u32> = evaluation
                            .invalid_rows
                            .iter()
                            .map(|r| r.row_index as u32)
                            .collect();
                        let dlq_batch = select_rows(batch, &invalid_indices)?;
                        for (i, invalid) in evaluation.invalid_rows.iter().enumerate() {
                            let record_json = row_to_json_from_batch(&dlq_batch, i)?;
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

fn emit_validation_metrics(
    ctx: &Context,
    evaluation: &crate::validate::BatchEvaluation,
) -> Result<(), PluginError> {
    for metric in build_validation_metrics(evaluation) {
        ctx.metric(&metric)?;
    }
    Ok(())
}

fn select_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, PluginError> {
    let idx = UInt32Array::from(indices.to_vec());
    let idx_ref = &idx as &dyn Array;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let filtered = take(column.as_ref(), idx_ref, None).map_err(|e| {
            PluginError::internal("VALIDATE_FILTER", format!("failed to filter rows: {e}"))
        })?;
        arrays.push(filtered);
    }
    RecordBatch::try_new(Arc::clone(&batch.schema()), arrays).map_err(|e| {
        PluginError::internal(
            "VALIDATE_BATCH",
            format!("failed to build filtered record batch: {e}"),
        )
    })
}

fn row_to_json_from_batch(batch: &RecordBatch, row: usize) -> Result<String, PluginError> {
    let single = select_rows(batch, &[row as u32])?;
    let mut writer = LineDelimitedWriter::new(Vec::new());
    writer.write_batches(&[&single]).map_err(|e| {
        PluginError::internal("VALIDATE_DLQ_JSON", format!("failed to encode DLQ row: {e}"))
    })?;
    writer.finish().map_err(|e| {
        PluginError::internal(
            "VALIDATE_DLQ_JSON",
            format!("failed to finalize DLQ row json: {e}"),
        )
    })?;
    let mut text = String::from_utf8(writer.into_inner()).map_err(|e| {
        PluginError::internal(
            "VALIDATE_DLQ_JSON",
            format!("dlq json was not utf8: {e}"),
        )
    })?;
    while text.ends_with('\n') {
        text.pop();
    }
    Ok(text)
}
