//! Preview spool — holds dry-run results for Flight replay.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use rapidbyte_engine::result::SourceTiming;
use rapidbyte_engine::DryRunResult;
use rapidbyte_engine::DryRunStreamResult;
use uuid::Uuid;

const DEFAULT_SPILL_THRESHOLD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PreviewKey {
    pub run_id: String,
    pub task_id: String,
    pub lease_epoch: u64,
}

pub struct PreviewSpool {
    entries: HashMap<PreviewKey, SpoolEntry>,
    default_ttl: Duration,
    spill_threshold_bytes: usize,
}

struct SpoolEntry {
    streams: Vec<StoredStream>,
    source: SourceTiming,
    transform_count: usize,
    transform_duration_secs: f64,
    duration_secs: f64,
    created_at: Instant,
    created_at_wall: SystemTime,
    ttl: Duration,
}

struct StoredStream {
    stream_name: String,
    total_rows: u64,
    total_bytes: u64,
    storage: StoredStreamData,
}

enum StoredStreamData {
    Memory(Vec<RecordBatch>),
    File {
        path: PathBuf,
        schema: Option<SchemaRef>,
    },
}

#[derive(Clone)]
pub struct PreviewListing {
    pub key: PreviewKey,
    pub stream_name: String,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub schema: Option<SchemaRef>,
    pub expires_at_unix: u64,
}

impl PreviewSpool {
    #[must_use]
    pub fn new(default_ttl: Duration) -> Self {
        Self::with_spill_threshold(default_ttl, DEFAULT_SPILL_THRESHOLD_BYTES)
    }

    #[must_use]
    pub fn with_spill_threshold(default_ttl: Duration, spill_threshold_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            default_ttl,
            spill_threshold_bytes,
        }
    }

    pub fn store(&mut self, key: PreviewKey, result: DryRunResult) {
        if let Some(old) = self.entries.remove(&key) {
            remove_entry_files(old);
        }

        let streams = result
            .streams
            .into_iter()
            .map(|stream| self.store_stream(stream))
            .collect();
        self.entries.insert(
            key,
            SpoolEntry {
                streams,
                source: result.source,
                transform_count: result.transform_count,
                transform_duration_secs: result.transform_duration_secs,
                duration_secs: result.duration_secs,
                created_at: Instant::now(),
                created_at_wall: SystemTime::now(),
                ttl: self.default_ttl,
            },
        );
    }

    #[must_use]
    pub fn get(&self, key: &PreviewKey) -> Option<DryRunResult> {
        let entry = self.entries.get(key)?;
        if entry.created_at.elapsed() >= entry.ttl {
            return None;
        }

        let streams = entry
            .streams
            .iter()
            .map(|stored| {
                load_batches(&stored.storage)
                    .ok()
                    .map(|batches| DryRunStreamResult {
                        stream_name: stored.stream_name.clone(),
                        batches,
                        total_rows: stored.total_rows,
                        total_bytes: stored.total_bytes,
                    })
            })
            .collect::<Option<Vec<_>>>()?;

        Some(DryRunResult {
            streams,
            source: entry.source.clone(),
            transform_count: entry.transform_count,
            transform_duration_secs: entry.transform_duration_secs,
            duration_secs: entry.duration_secs,
        })
    }

    #[must_use]
    pub fn list_active(&mut self) -> Vec<PreviewListing> {
        self.evict_expired();

        self.entries
            .iter()
            .flat_map(|(key, entry)| {
                entry.streams.iter().map(|stream| PreviewListing {
                    key: key.clone(),
                    stream_name: stream.stream_name.clone(),
                    total_rows: stream.total_rows,
                    total_bytes: stream.total_bytes,
                    schema: stream.schema(),
                    expires_at_unix: entry
                        .created_at_wall
                        .checked_add(entry.ttl)
                        .and_then(|expires_at| {
                            expires_at.duration_since(SystemTime::UNIX_EPOCH).ok()
                        })
                        .map_or(0, |duration| duration.as_secs()),
                })
            })
            .collect()
    }

    pub fn cleanup_expired(&mut self) -> usize {
        self.evict_expired()
    }

    fn evict_expired(&mut self) -> usize {
        let expired_keys: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.created_at.elapsed() >= entry.ttl)
            .map(|(key, _)| key.clone())
            .collect();
        let count = expired_keys.len();
        for key in expired_keys {
            if let Some(entry) = self.entries.remove(&key) {
                remove_entry_files(entry);
            }
        }
        count
    }

    fn store_stream(&self, stream: DryRunStreamResult) -> StoredStream {
        let DryRunStreamResult {
            stream_name,
            batches,
            total_rows,
            total_bytes,
        } = stream;
        let bytes = {
            let batches_bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
            let reported = usize::try_from(total_bytes).unwrap_or(usize::MAX);
            batches_bytes.max(reported)
        };
        let should_spill = bytes > self.spill_threshold_bytes;
        let storage = if should_spill && !batches.is_empty() {
            match write_batches_to_file(&batches) {
                Ok(storage) => storage,
                Err(_) => StoredStreamData::Memory(batches),
            }
        } else {
            StoredStreamData::Memory(batches)
        };

        StoredStream {
            stream_name,
            total_rows,
            total_bytes,
            storage,
        }
    }
}

impl StoredStream {
    fn schema(&self) -> Option<SchemaRef> {
        match &self.storage {
            StoredStreamData::Memory(batches) => batches.first().map(RecordBatch::schema),
            StoredStreamData::File { schema, .. } => schema.clone(),
        }
    }
}

fn write_batches_to_file(batches: &[RecordBatch]) -> std::io::Result<StoredStreamData> {
    let path = std::env::temp_dir().join(format!("rapidbyte-preview-{}.arrow", Uuid::new_v4()));
    let mut file = File::options().create_new(true).write(true).open(&path)?;
    let schema = batches.first().map(RecordBatch::schema);
    if let Some(schema_ref) = &schema {
        let mut writer =
            StreamWriter::try_new(&mut file, schema_ref.as_ref()).map_err(std::io::Error::other)?;
        for batch in batches {
            writer.write(batch).map_err(std::io::Error::other)?;
        }
        writer.finish().map_err(std::io::Error::other)?;
    }

    Ok(StoredStreamData::File { path, schema })
}

fn load_batches(storage: &StoredStreamData) -> std::io::Result<Vec<RecordBatch>> {
    match storage {
        StoredStreamData::Memory(batches) => Ok(batches.clone()),
        StoredStreamData::File { path, .. } => {
            let file = File::open(path)?;
            let reader = StreamReader::try_new(file, None).map_err(std::io::Error::other)?;
            reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(std::io::Error::other)
        }
    }
}

fn remove_entry_files(entry: SpoolEntry) {
    for stream in entry.streams {
        if let StoredStreamData::File { path, .. } = stream.storage {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rapidbyte_engine::result::SourceTiming;
    use rapidbyte_engine::DryRunStreamResult;
    use std::sync::Arc;

    fn make_dry_run_result() -> DryRunResult {
        DryRunResult {
            streams: vec![],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        }
    }

    #[test]
    fn store_and_get() {
        let mut spool = PreviewSpool::new(Duration::from_secs(60));
        let key = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        spool.store(key.clone(), make_dry_run_result());
        assert!(spool.get(&key).is_some());
    }

    #[test]
    fn expired_entries_return_none() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        let key = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        spool.store(key.clone(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));
        assert!(spool.get(&key).is_none());
    }

    #[test]
    fn expired_get_returns_none_without_evicting() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        let key = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        spool.store(key.clone(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));

        assert!(spool.get(&key).is_none());
        // get() no longer evicts; cleanup is done by cleanup_expired()
        assert_eq!(spool.cleanup_expired(), 1);
    }

    #[test]
    fn expired_cleanup_removes_file_backed_preview_file() {
        let mut spool = PreviewSpool::with_spill_threshold(Duration::from_secs(0), 1);
        let key = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))])
                .unwrap();
        let result = DryRunResult {
            streams: vec![DryRunStreamResult {
                stream_name: "users".into(),
                batches: vec![batch],
                total_rows: 4,
                total_bytes: 16,
            }],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        };
        spool.store(key.clone(), result);
        std::thread::sleep(Duration::from_millis(10));

        let path = match &spool.entries.get(&key).unwrap().streams[0].storage {
            StoredStreamData::File { path, .. } => path.clone(),
            StoredStreamData::Memory(_) => panic!("preview should spill to disk"),
        };

        assert!(spool.get(&key).is_none());
        assert_eq!(spool.cleanup_expired(), 1);
        assert!(!path.exists());
    }

    #[test]
    fn cleanup_removes_expired() {
        let mut spool = PreviewSpool::new(Duration::from_secs(0));
        let expired = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        spool.store(expired.clone(), make_dry_run_result());
        std::thread::sleep(Duration::from_millis(10));

        // Store a fresh one
        spool.default_ttl = Duration::from_secs(60);
        let fresh = PreviewKey {
            run_id: "r2".into(),
            task_id: "t2".into(),
            lease_epoch: 2,
        };
        spool.store(fresh.clone(), make_dry_run_result());

        let removed = spool.cleanup_expired();
        assert_eq!(removed, 1);
        assert!(spool.get(&expired).is_none());
        assert!(spool.get(&fresh).is_some());
    }

    #[test]
    fn unknown_task_returns_none() {
        let spool = PreviewSpool::new(Duration::from_secs(60));
        assert!(spool
            .get(&PreviewKey {
                run_id: "missing".into(),
                task_id: "nonexistent".into(),
                lease_epoch: 1,
            })
            .is_none());
    }

    #[test]
    fn large_preview_spills_to_disk() {
        let mut spool = PreviewSpool::with_spill_threshold(Duration::from_secs(60), 1);
        let key = PreviewKey {
            run_id: "r1".into(),
            task_id: "t1".into(),
            lease_epoch: 1,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))])
                .unwrap();
        let result = DryRunResult {
            streams: vec![DryRunStreamResult {
                stream_name: "users".into(),
                batches: vec![batch],
                total_rows: 4,
                total_bytes: 16,
            }],
            source: SourceTiming::default(),
            transform_count: 0,
            transform_duration_secs: 0.0,
            duration_secs: 1.0,
        };

        spool.store(key.clone(), result);

        let stored = spool.entries.get(&key).expect("entry must exist");
        assert!(matches!(
            stored.streams[0].storage,
            StoredStreamData::File { .. }
        ));

        let loaded = spool.get(&key).expect("file-backed preview should load");
        assert_eq!(loaded.streams[0].batches[0].num_rows(), 4);
    }
}
