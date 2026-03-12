# Plugin Architecture

This document defines the standard structure for Rapidbyte plugins. The goal is predictable plugin layout and behavior, not identical file trees for every plugin.

## Core Rules

### Required

- `main.rs` is thin.
- Every plugin performs real config validation in `init()`.
- Every plugin implements meaningful `validate()`.
- Runtime code streams data and avoids unnecessary full materialization.
- Plugin boundary errors are converted into typed `PluginError` categories.
- Tests cover config handling, validation behavior, role-specific runtime behavior, and at least one happy-path plus one failure-path flow.

### Recommended

- `config.rs` owns raw config plus normalization, preparation, or compilation helpers.
- Heavy runtime logic lives outside `main.rs` in role-specific modules.
- Metric helpers move into `metrics.rs` once they stop being obviously local.
- Modules stay private unless there is a real internal API boundary that needs `pub(crate)` or public exposure.

## Standard Layouts

These are defaults, not mandatory file lists.

### Source Plugins

- `main.rs`: trait impl and wiring
- `config.rs`: config, normalization, validation
- `client.rs` or `transport.rs`: connection/session/auth
- `discover.rs`: schema/catalog discovery
- `read.rs`: standard reads
- `cdc.rs`: CDC-specific logic when needed
- `partition.rs`: partition helpers when needed
- `types.rs`: source-type to Arrow mapping
- `metrics.rs`: metric helpers when non-trivial

### Destination Plugins

- `main.rs`: trait impl and wiring
- `config.rs`
- `client.rs` or `transport.rs`
- `prepare.rs` or `ddl.rs`: preflight/schema/staging work
- `write.rs`: orchestration entrypoint
- `bulk.rs`: copy/load/batch upload logic when needed
- `rowwise.rs` or `insert.rs`: non-bulk write path
- `replace.rs` or `upsert.rs`: mode-specific semantics when needed
- `types.rs`: type mapping
- `metrics.rs`: metric helpers when non-trivial

### Transform Plugins

- `main.rs`: trait impl and wiring
- `config.rs`: raw config plus normalization/compile helpers
- `validate.rs`: validation/planning helpers when validation is substantial
- `transform.rs`: runtime data path
- `rules.rs`, `planner.rs`, or `engine.rs`: complexity-specific helpers
- `metrics.rs`: metric helpers when non-trivial

## Validation and Config

- `init()` is the runtime safety gate and should reject invalid config.
- `validate()` is required even when lightweight.
- Expensive parsing or compilation should happen once and be stored as prepared state when practical.
- If validation must repeat work because `validate()` is static and cannot access instance state, that is acceptable.

## Error Discipline

Use `PluginError` categories consistently:

- `config` / `auth` for setup and validation failures
- `transient_network` / `transient_db` for retryable external failures
- `data` / `schema` for record and schema issues
- `internal` only for genuine internal faults

Internal helper functions may still use `Result<T, String>` when they are local plumbing layers, but plugin boundaries should convert to typed errors.

## Visibility

- Prefer private modules.
- Prefer `pub(crate)` items over `pub mod` when access is internal to the plugin crate.
- Make modules public only when macro ergonomics or a real consumer boundary requires it.

## Metrics

Keep metric emission near the runtime path when it is small. Extract `metrics.rs` once helpers are reused or they clutter orchestration code.

## Feature Traits and Capabilities

- Use separate traits only when the contract shape actually changes.
- Source feature traits such as CDC or partitioned read are valid because they change inputs and semantics.
- Destination bulk loading is a strategy, not a separate top-level role. Capability metadata should stay declarative; runtime strategy stays inside the plugin.

## Shared Kits

Do not extract shared SDK-level “kits” early. Only do it after at least two real plugins need the same abstraction and the boundary is obvious from working code.

## Scaffold Expectations

`rapidbyte scaffold` should produce plugins that follow this standard closely enough to teach the right shape:

- thin `main.rs`
- explicit validation hooks
- role-appropriate runtime modules
- clearly non-production-ready placeholders where implementation is missing

## Current Reference Plugins

- Source reference: `plugins/sources/postgres`
- Destination reference: `plugins/destinations/postgres`
- Transform references: `plugins/transforms/sql`, `plugins/transforms/validate`

These references should evolve toward the standard over time, but the standard itself should stay stable enough for new plugin authors to follow immediately.
