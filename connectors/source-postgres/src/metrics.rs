//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::prelude::*;

/// Maximum number of rows per Arrow `RecordBatch`.
pub(crate) const BATCH_SIZE: usize = 10_000;

/// Cumulative emit counters shared by both full-refresh and CDC read paths.
pub(crate) struct EmitState {
    pub(crate) total_records: u64,
    pub(crate) total_bytes: u64,
    pub(crate) batches_emitted: u64,
    pub(crate) arrow_encode_nanos: u64,
}

/// Emit cumulative source read counters for a stream.
pub(crate) fn emit_read_metrics(ctx: &Context, total_records: u64, total_bytes: u64) {
    let _ = ctx.metric(&Metric {
        name: "records_read".to_string(),
        value: MetricValue::Counter(total_records),
        labels: vec![],
    });
    let _ = ctx.metric(&Metric {
        name: "bytes_read".to_string(),
        value: MetricValue::Counter(total_bytes),
        labels: vec![],
    });
}

/// Emit source read timing metrics so the host can aggregate per-phase timings.
pub(crate) fn emit_read_perf_metrics(ctx: &Context, perf: &ReadPerf) {
    let gauges = [
        ("source_connect_secs", perf.connect_secs),
        ("source_query_secs", perf.query_secs),
        ("source_fetch_secs", perf.fetch_secs),
        ("source_arrow_encode_secs", perf.arrow_encode_secs),
    ];

    for (name, value) in gauges {
        let _ = ctx.metric(&Metric {
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            labels: vec![],
        });
    }
}
