# PR Benchmark Suite

This directory holds the small, stable benchmark scenarios intended for pull
request regression detection.

The PR suite should remain low-noise, deterministic, and fast enough for local
perf-regression checks while still exercising the real engine and plugin path
end to end.

The matching GitHub workflow is manual-only for now. Routine validation should
use `just bench-pr` locally, and maintainers can trigger the workflow
explicitly if they need a hosted run.
