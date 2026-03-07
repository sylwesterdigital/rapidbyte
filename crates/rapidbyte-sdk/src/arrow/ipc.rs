//! Arrow IPC encoding and decoding.
//!
//! All data in Rapidbyte flows as Arrow RecordBatches. This module handles
//! serialization to/from IPC stream format for host-guest communication.

use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::PluginError;

/// Write a `RecordBatch` as Arrow IPC stream format into any `Write` sink.
///
/// # Errors
///
/// Returns `Err` if IPC writer initialization, encoding, or finishing fails.
pub fn encode_ipc_into<W: std::io::Write>(
    batch: &RecordBatch,
    writer: &mut W,
) -> Result<(), PluginError> {
    let mut ipc_writer = StreamWriter::try_new(writer, batch.schema().as_ref()).map_err(|e| {
        PluginError::internal("ARROW_IPC_ENCODE", format!("IPC writer init: {e}"))
    })?;
    ipc_writer
        .write(batch)
        .map_err(|e| PluginError::internal("ARROW_IPC_ENCODE", format!("IPC write: {e}")))?;
    ipc_writer
        .finish()
        .map_err(|e| PluginError::internal("ARROW_IPC_ENCODE", format!("IPC finish: {e}")))?;
    Ok(())
}

/// Encode a RecordBatch into Arrow IPC stream bytes.
///
/// # Errors
///
/// Returns `Err` if Arrow IPC stream writer initialization or encoding fails.
pub fn encode_ipc(batch: &RecordBatch) -> Result<Vec<u8>, PluginError> {
    let capacity = batch.get_array_memory_size() + 1024;
    let mut buf = Vec::with_capacity(capacity);
    encode_ipc_into(batch, &mut buf)?;
    Ok(buf)
}

/// Decode Arrow IPC stream bytes into schema and record batches.
///
/// # Errors
///
/// Returns `Err` if Arrow IPC stream reader initialization or batch deserialization fails.
pub fn decode_ipc(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), PluginError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        PluginError::internal("ARROW_IPC_DECODE", format!("IPC reader init: {e}"))
    })?;
    let schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PluginError::internal("ARROW_IPC_DECODE", format!("IPC read: {e}")))?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn encode_decode_roundtrip() {
        let batch = sample_batch();
        let ipc_bytes = encode_ipc(&batch).unwrap();
        let (schema, batches) = decode_ipc(&ipc_bytes).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn decode_empty_bytes_fails() {
        assert!(decode_ipc(&[]).is_err());
    }

    #[test]
    fn encode_preserves_nullability() {
        let batch = sample_batch();
        let ipc_bytes = encode_ipc(&batch).unwrap();
        let (schema, batches) = decode_ipc(&ipc_bytes).unwrap();

        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(batches[0].column(1).is_null(1));
    }
}
