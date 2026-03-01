# Transform Validate Refactor Design

## Context

`connectors/transform-validate` needs a comprehensive refactor to align with the Rapidbyte coding style blueprint, with emphasis on correctness semantics, explicit error modeling, numeric/type safety, and hot-path allocation discipline.

## Goals

1. Preserve connector external behavior where intentional while improving internal structure.
2. Keep data semantics explicit and deterministic for `fail`/`skip`/`dlq` handling.
3. Tighten numeric semantics, especially decimal and non-finite handling, to avoid lossy validation decisions.
4. Reduce avoidable allocation/cloning in config compilation and batch evaluation loops.
5. Expand focused tests for correctness-sensitive cases.

## Architecture

1. Keep `main.rs` API unchanged.
2. Refactor `config.rs` compilation into helper paths that validate and expand rule selectors without unnecessary clone-heavy intermediates.
3. Refactor `transform.rs` to separate orchestration from row evaluation through focused helpers and explicit internal result structures.
4. Keep metric names/labels stable (`rule`, `field`) and preserve bounded-stream behavior.

## Data Semantics and Errors

1. Evaluate all applicable rules per row and retain all validation failure reasons.
2. Centralize policy handling:
   - `fail`: return `ConnectorError::data` with row + summary context.
   - `skip`: drop invalid rows only.
   - `dlq`: emit every invalid row with full failure message.
3. Preserve error category boundaries:
   - Config compile issues => config errors.
   - Validation failures => data errors.
   - Arrow/encoding failures => internal errors.

## Numeric Semantics

1. Continue explicit non-finite float rejection for range checks.
2. Replace lossy decimal comparisons with scale-aware decimal comparisons for `Decimal128` and `Decimal256`.
3. Keep integer and float comparisons explicit and typed.

## Performance

1. Avoid per-rule clone-heavy selector expansion in config compile.
2. Preserve existing preallocation in row evaluation and apply additional low-risk reductions in temporary allocations.
3. Do not introduce unbounded buffering or hidden async/blocking behavior.

## Testing and Verification

1. Add/update unit tests for:
   - Decimal edge comparisons (including exact bounds).
   - Non-finite float handling.
   - Multi-failure row aggregation.
   - Config compile validation paths.
2. Run verification commands relevant to the changed scope:
   - `just fmt`
   - `just lint`
   - `cargo test` in `connectors/transform-validate`
