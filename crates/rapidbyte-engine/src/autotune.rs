//! Runtime autotune decision helpers.

use rapidbyte_types::stream::PartitionStrategy;

use crate::config::types::{PipelineConfig, MAX_COPY_FLUSH_BYTES, MIN_COPY_FLUSH_BYTES};

/// Resolved runtime overrides for a stream execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamAutotuneDecision {
    pub parallelism: u32,
    pub partition_strategy: Option<PartitionStrategy>,
    pub copy_flush_bytes_override: Option<u64>,
    pub autotune_enabled: bool,
}

/// Build parallelism candidates around a baseline value within the provided cap.
#[must_use]
pub fn parallelism_candidates(base: u32, cap: u32) -> Vec<u32> {
    let base = base.max(1);
    let cap = cap.max(1);
    let half = (base / 2).max(1);
    let bump = {
        let scaled = u64::from(base).saturating_mul(3).saturating_add(1) / 2;
        let scaled = u32::try_from(scaled).unwrap_or(u32::MAX);
        scaled.clamp(1, cap)
    };

    let mut out = vec![half, base.min(cap), bump];
    out.sort_unstable();
    out.dedup();
    out
}

/// Resolve stream-level autotune decisions, honoring explicit manual pins.
#[must_use]
pub fn resolve_stream_autotune(
    config: &PipelineConfig,
    baseline_parallelism: u32,
    supports_partitioned_read: bool,
) -> StreamAutotuneDecision {
    let autotune_cfg = &config.resources.autotune;

    let pinned_parallelism = autotune_cfg.pin_parallelism.map(|value| value.max(1));
    let parallelism = pinned_parallelism.unwrap_or_else(|| baseline_parallelism.max(1));

    let partition_strategy = if supports_partitioned_read {
        autotune_cfg.pin_source_partition_mode
    } else {
        None
    };

    let copy_flush_bytes_override = autotune_cfg.pin_copy_flush_bytes.map(|bytes| {
        let bytes = u64::try_from(bytes).unwrap_or(MAX_COPY_FLUSH_BYTES as u64);
        bytes.clamp(MIN_COPY_FLUSH_BYTES as u64, MAX_COPY_FLUSH_BYTES as u64)
    });

    StreamAutotuneDecision {
        parallelism,
        partition_strategy,
        copy_flush_bytes_override,
        autotune_enabled: autotune_cfg.enabled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parser::parse_pipeline_str;

    fn base_pipeline_yaml() -> &'static str {
        r#"
version: "1.0"
pipeline: autotune_test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#
    }

    #[test]
    fn manual_pin_parallelism_overrides_baseline() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_parallelism: 8\n",
            base_pipeline_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();

        let decision = resolve_stream_autotune(&config, 3, true);
        assert_eq!(decision.parallelism, 8);
    }

    #[test]
    fn partition_mode_pin_maps_to_strategy() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_source_partition_mode: range\n",
            base_pipeline_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();

        let decision = resolve_stream_autotune(&config, 3, true);
        assert_eq!(decision.partition_strategy, Some(PartitionStrategy::Range));
    }

    #[test]
    fn copy_flush_pin_is_clamped_to_guardrail() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_copy_flush_bytes: 999999999\n",
            base_pipeline_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();

        let decision = resolve_stream_autotune(&config, 3, true);
        assert_eq!(
            decision.copy_flush_bytes_override,
            Some(MAX_COPY_FLUSH_BYTES as u64)
        );
    }

    #[test]
    fn candidate_generation_stays_within_cap_and_dedupes() {
        assert_eq!(parallelism_candidates(1, 1), vec![1]);
        assert_eq!(parallelism_candidates(12, 16), vec![6, 12, 16]);
    }
}
