# Replace Hardcoded Plugin Name Checks with `Feature::PartitionedRead`

## Problem

The orchestrator and autotune code hardcode `source_connector_id == "source-postgres"` in three places to decide whether a source supports parallel partitioned reads. This is brittle — adding a second partitioning-capable source would require editing engine internals. It also broke silently during the connector→plugin rename when the YAML `use:` value changed from `source-postgres` to `postgres`.

## Design

### Approach: New `Feature` variant (static manifest declaration)

Add `PartitionedRead` to the existing `Feature` enum. The host checks the manifest's feature list instead of comparing plugin names.

**Why static (manifest) instead of dynamic (`open()` time):**
- The host resolves auto-parallelism and builds stream contexts *before* calling `open()`.
- Partitioned read support is intrinsic to the plugin, not config-dependent.
- Matches the existing pattern (`BulkLoadCopy`, `Cdc`, etc.).

### Changes

**1. Types (`rapidbyte-types/src/wire.rs`)**

Add `PartitionedRead` to `Feature` enum. Serde serializes as `"partitioned_read"`.

**2. Plugin declaration**

Postgres source adds `Feature::PartitionedRead` to its `PluginInfo` features and manifest `SourceCapabilities.features`.

**3. Engine consumption**

Replace the `source_connector_id: &str` parameter in auto-parallelism, partitioning, and autotune functions with a `bool` derived from the manifest's feature list:

- `orchestrator.rs` — `resolve_auto_parallelism_for_cores`: use bool instead of name check
- `orchestrator.rs` — `build_stream_contexts`: use bool instead of name check
- `autotune.rs` — `resolve_stream_autotune`: use bool instead of name check

The manifest is already resolved in `run_pipeline_once` and available as `connectors.source_manifest`. Thread `supports_partitioned_read: bool` to the callsites.

**4. No protocol/WIT changes**

`Feature` is a Rust-side enum used in manifest JSON and `PluginInfo`. No WIT or protocol version changes needed.
