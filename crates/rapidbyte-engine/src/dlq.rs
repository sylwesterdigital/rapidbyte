//! DLQ persistence helper used by the orchestrator.

use rapidbyte_types::envelope::DlqRecord;

use rapidbyte_state::error;
use rapidbyte_state::StateBackend;
use rapidbyte_types::state::PipelineId;

/// Persist collected DLQ records to the state backend.
pub(crate) fn persist_dlq_records(
    state_backend: &dyn StateBackend,
    pipeline: &PipelineId,
    run_id: i64,
    records: &[DlqRecord],
) -> error::Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }

    let dlq_count = records.len();

    match state_backend.insert_dlq_records(pipeline, run_id, records) {
        Ok(inserted) => {
            tracing::info!(
                pipeline = pipeline.as_str(),
                dlq_records = inserted,
                "Persisted DLQ records to state backend"
            );
            Ok(inserted)
        }
        Err(e) => {
            tracing::error!(
                pipeline = pipeline.as_str(),
                dlq_count,
                error = %e,
                "Failed to persist DLQ records"
            );
            Err(e)
        }
    }
}
