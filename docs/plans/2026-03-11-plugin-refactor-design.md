# Plugin Refactor Design

## Summary

Refactor the in-tree plugins toward a clearer, role-oriented structure without changing the core SDK role model again. The goal is not to make every plugin file tree identical; it is to establish a predictable standard that keeps `main.rs` thin, pushes heavy logic into focused modules, and aligns scaffolding and SDK helpers with that standard.

This work should preserve the current plugin isolation model. Plugins remain self-contained WASM components that only interact with the host through `rapidbyte-sdk`. Cross-plugin duplication remains acceptable unless and until a boundary is clearly reusable at the SDK level.

## Goals

- Define and document the standard plugin architecture for source, destination, and transform plugins.
- Refactor the highest-value structural hot spots:
  - `plugins/transforms/sql/src/main.rs`
  - `plugins/destinations/postgres/src/writer.rs`
  - `plugins/transforms/validate/src/transform.rs`
- Align scaffolding with the documented structure.
- Add small SDK helpers where plugin code is currently forced to reach into lower-level FFI details.
- Standardize a few consistency rules around visibility, validation, and metric helper placement.

## Non-Goals

- No cross-plugin shared crate.
- No repo-wide rename churn for modules that are already clear and stable.
- No speculative “plugin kit” extraction in this tranche.
- No protocol or ABI changes.

## Architecture Principles

### Thin Entrypoints

`main.rs` should contain plugin struct definitions, trait implementations, and lightweight wiring only. It should not own large analysis engines, heavy query walkers, or bulk runtime orchestration logic.

### Role-Oriented Modules

Each plugin role should converge on a predictable default structure:

#### Source plugins

- `main.rs`: trait impl and wiring
- `config.rs`: raw config plus normalization and validation
- `client.rs` or `transport.rs`: connection/session/auth
- `discover.rs`: schema/catalog discovery
- `read.rs`: standard reads
- `cdc.rs`: CDC-specific logic when applicable
- `partition.rs`: partition planning/helpers when applicable
- `types.rs`: source-type to Arrow mapping
- `metrics.rs`: metric helpers when non-trivial

#### Destination plugins

- `main.rs`: trait impl and wiring
- `config.rs`
- `client.rs` or `transport.rs`
- `prepare.rs` / `ddl.rs`: schema/table/staging/preflight
- `write.rs`: central orchestration entrypoint
- `bulk.rs`: copy/load/batch upload logic when present
- `rowwise.rs` / `insert.rs`: smaller write path
- `replace.rs` / `upsert.rs`: mode-specific semantics when present
- `types.rs`: type mapping
- `metrics.rs`: metric helpers when non-trivial

#### Transform plugins

- `main.rs`: trait impl and wiring
- `config.rs`: raw config plus normalization/compile helpers
- `validate.rs`: separate validation/planning helpers when validation is substantial
- `transform.rs`: runtime data path
- `rules.rs` / `planner.rs` / `engine.rs`: complexity-specific modules
- `metrics.rs`: metric helpers when non-trivial

These are defaults, not hard rules. The standard should optimize for predictability without forcing empty or redundant files into simple plugins.

## Standards

### Visibility

Prefer private modules and `pub(crate)` items over `pub mod`. `config` may be public when macro ergonomics require it, but public module exposure should be the exception rather than the default.

### Validation

All plugins should perform real config validation in `init()`. Static `validate()` remains required and should provide meaningful success, warning, or failure results, but `init()` is the hard runtime safety gate.

### Errors

Internal helpers may continue to use `Result<T, String>` when they are local plumbing layers, but plugin boundaries should consistently convert failures into typed `PluginError` categories:

- config/auth for setup and validation failures
- transient network/db for retryable external failures
- data/schema for record and schema issues
- internal only for genuine internal faults

### Metrics

Extract a `metrics.rs` module once metric emission is reused or starts to clutter runtime orchestration. Do not force `metrics.rs` into tiny plugins that only emit one obvious metric.

### Large Module Threshold

Split files by responsibility, not by arbitrary line count. A file is a refactor target when it mixes multiple distinct concerns that would be easier to test and review separately.

### Shared Kits

Only extract SDK-level shared kits after at least two real plugins need the same abstraction and the boundary is obvious from working code.

## Planned Refactors

### 1. `transform-sql`

`plugins/transforms/sql/src/main.rs` currently mixes plugin wiring with a large SQL reference-analysis implementation. Extract the query table-reference walker into `references.rs` and leave `main.rs` focused on plugin initialization and validation orchestration.

### 2. `dest-postgres`

`plugins/destinations/postgres/src/writer.rs` currently combines orchestration, contract setup, session lifecycle, checkpoint handling, and metric helpers. Split it into focused modules such as:

- `write.rs` or a thinner `writer.rs` entrypoint
- `contract.rs`
- `session.rs`
- optional `metrics.rs`

This should be a behavior-preserving move.

### 3. `transform-validate`

`plugins/transforms/validate/src/transform.rs` is now large enough to deserve the same treatment as `transform-sql`. After the first structural moves establish the standard, split validation/runtime helper concerns into smaller transform-specific modules.

### 4. Scaffold Alignment

`rapidbyte scaffold` should generate source, destination, and transform plugins that reflect the documented standard more closely. This includes:

- thinner generated `main.rs`
- stronger separation of config, validation, and runtime code
- role-appropriate module names

### 5. SDK Helper for Spawned Logging

Where plugins currently call raw `rapidbyte_sdk::host_ffi::log(...)` from spawned tasks because `Context` is unavailable, add a small SDK helper such as `log_error()` so plugin code stays inside the SDK boundary.

## Sequencing

### Tranche 1

- Write and land the plugin architecture/conventions doc.
- Extract `transform-sql` reference analysis.
- Split `dest-postgres` writer structure.
- Add the SDK logging helper.
- Update scaffold templates to align with the documented standard.

### Tranche 2

- Refactor `transform-validate` to match the transform standard.
- Revisit destination type-module consolidation if it still improves clarity after the writer split.
- Apply low-churn visibility cleanup where it improves consistency.

## Verification

Success for this refactor means:

- existing plugin behavior remains unchanged
- targeted plugin/unit/e2e tests still pass
- scaffold output matches the documented architecture
- at least one plugin per role serves as a credible in-tree reference for the standard

## Expected Outcome

The plugin layer becomes easier to navigate and safer to extend:

- plugin authors learn one clear mental model
- runtime behavior stays plugin-owned rather than macro-owned
- heavy modules become easier to review and test
- future shared abstractions are extracted from proven patterns instead of speculation
