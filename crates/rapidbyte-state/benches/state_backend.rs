//! Criterion benchmarks for the `SQLite` state backend.
//!
//! These measure cursor persistence and run lifecycle operations that occur
//! on every pipeline execution.

use chrono::Utc;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use rapidbyte_state::prelude::*;

fn bench_run_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/run_lifecycle");

    group.bench_function("start_and_complete", |b| {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let pipeline = PipelineId::new("bench_pipeline");
        let stream = StreamName::new("bench_stream");

        b.iter(|| {
            let run_id = backend.start_run(&pipeline, &stream).unwrap();
            backend
                .complete_run(
                    run_id,
                    RunStatus::Completed,
                    &RunStats {
                        records_read: 1000,
                        records_written: 1000,
                        bytes_read: 50000,
                        bytes_written: 50000,
                        error_message: None,
                    },
                )
                .unwrap();
        });
    });

    group.finish();
}

fn bench_persist_cursor(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/persist_cursor");

    for stream_count in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("streams", stream_count),
            &stream_count,
            |b, &stream_count| {
                let backend = SqliteStateBackend::in_memory().unwrap();
                let pipeline = PipelineId::new("bench_pipeline");
                let streams: Vec<StreamName> = (0..stream_count)
                    .map(|i| StreamName::new(format!("stream_{i}")))
                    .collect();
                let mut counter = 0u64;

                b.iter(|| {
                    for stream in &streams {
                        let cursor = CursorState {
                            cursor_field: Some("id".to_string()),
                            cursor_value: Some(counter.to_string()),
                            updated_at: Utc::now().to_rfc3339(),
                        };
                        backend.set_cursor(&pipeline, stream, &cursor).unwrap();
                        counter += 1;
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_get_cursor(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/get_cursor");

    // Pre-populate some cursors, then benchmark reads
    for stream_count in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("streams", stream_count),
            &stream_count,
            |b, &stream_count| {
                let backend = SqliteStateBackend::in_memory().unwrap();
                let pipeline = PipelineId::new("bench_pipeline");
                let streams: Vec<StreamName> = (0..stream_count)
                    .map(|i| StreamName::new(format!("stream_{i}")))
                    .collect();

                // Pre-populate cursors
                for (i, stream) in streams.iter().enumerate() {
                    let cursor = CursorState {
                        cursor_field: Some("id".to_string()),
                        cursor_value: Some(i.to_string()),
                        updated_at: Utc::now().to_rfc3339(),
                    };
                    backend.set_cursor(&pipeline, stream, &cursor).unwrap();
                }

                b.iter(|| {
                    for stream in &streams {
                        let _cursor = backend.get_cursor(&pipeline, stream).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_run_lifecycle,
    bench_persist_cursor,
    bench_get_cursor
);
criterion_main!(benches);
