use rapidbyte_sdk::prelude::*;

pub(crate) fn emit_write_perf_metrics(ctx: &Context, perf: &WritePerf) {
    let gauges = [
        ("dest_connect_secs", perf.connect_secs),
        ("dest_flush_secs", perf.flush_secs),
        ("dest_commit_secs", perf.commit_secs),
    ];

    for (name, value) in gauges {
        let _ = ctx.metric(&Metric {
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            labels: vec![],
        });
    }
}
