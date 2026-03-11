# Plugin Surface Cleanup Design

## Goal

Clean up the public plugin model so destination bulk-load is no longer expressed
as a separate public trait, destination runtime dispatch no longer depends on
manifest feature presence, manifest capabilities become purely declarative, and
transform plugins become first-class in scaffolding.

## Current Problems

- `BulkLoadDestination` in
  [crates/rapidbyte-sdk/src/features.rs](/home/netf/rapidbyte/crates/rapidbyte-sdk/src/features.rs)
  duplicates the `Destination::write()` contract instead of modeling a distinct
  protocol shape.
- The destination plugin macro in
  [crates/rapidbyte-sdk/macros/src/plugin.rs](/home/netf/rapidbyte/crates/rapidbyte-sdk/macros/src/plugin.rs)
  uses `Feature::BulkLoad` as a static dispatch switch for `run`, even though
  the WIT destination world only exports one execution entrypoint.
- `dest-postgres` in
  [plugins/destinations/postgres/src/main.rs](/home/netf/rapidbyte/plugins/destinations/postgres/src/main.rs)
  shows the abstraction leak directly: `write_bulk()` just delegates to
  `write()`, and `Feature::BulkLoad` is declared both statically in `build.rs`
  and dynamically in `init()`.
- `rapidbyte scaffold` in
  [crates/rapidbyte-cli/src/commands/scaffold.rs](/home/netf/rapidbyte/crates/rapidbyte-cli/src/commands/scaffold.rs)
  only supports source and destination plugins, despite transform plugins being
  first-class elsewhere.

## Decision

### 1. Collapse destination bulk-load into `Destination`

`Destination` becomes the only public destination role trait most plugin authors
need. It will gain:

```rust
async fn write_bulk(
    &mut self,
    ctx: &Context,
    stream: StreamContext,
) -> Result<WriteSummary, PluginError> {
    self.write(ctx, stream).await
}
```

`BulkLoadDestination` will be removed from the SDK public surface.

Rationale:

- The method contract is identical to `write()`.
- The WIT ABI does not have a separate bulk destination entrypoint.
- Existing in-tree usage already treats it as an optional strategy variant, not
  a distinct protocol role.

### 2. Keep source feature traits separate

`PartitionedSource` and `CdcSource` remain as separate feature traits because
they represent real contract specialization:

- CDC adds a resume token and stream-mode semantics.
- Partitioned reads add partition coordinates and altered read semantics.

Rule going forward:

- Separate trait only when the method contract materially changes.
- Default method on the main role trait when the behavior is an optional
  strategy variant.

### 3. Make `Feature::BulkLoad` declarative only

The manifest should say what a destination plugin is capable of, not how the
host should route calls.

After this refactor:

- destination macro glue always routes through the `Destination` role trait
- plugin internals choose row-wise vs copy/bulk behavior
- `Feature::BulkLoad` remains metadata that says bulk-capable strategies exist

This matches the current WIT model and prevents capability declarations from
doubling as hidden strategy policy.

### 4. Remove split bulk capability semantics from `dest-postgres`

`dest-postgres` should become the canonical example:

- no separate `BulkLoadDestination` impl
- static manifest capability declaration in `build.rs`
- no dynamic re-declaration of `Feature::BulkLoad` in `init()`
- plugin-owned write strategy selection in normal destination code

If a plugin supports bulk in some configurations and not others, that should be
handled internally by strategy routing, not by host dispatch or trait shape.

### 5. Add first-class transform scaffolding

`rapidbyte scaffold` should support:

- `source-*`
- `dest-*`
- `transform-*`

Transform scaffolding should generate a minimal but structurally correct plugin
layout aligned with current patterns:

- `main.rs`
- `config.rs`
- `transform.rs`
- role-appropriate `build.rs`
- README and cargo config like the existing source/destination scaffolds

The generated code should keep `main.rs` thin and route real behavior to helper
modules, matching the structure already used by the better in-tree plugins.

## What Stays Out of Scope

- introducing shared plugin “kits” such as DB/HTTP helper layers
- broader plugin-internal module normalization across every in-tree plugin
- WIT protocol expansion for new destination entrypoints
- compatibility shims or deprecated parallel APIs

This tranche should produce a smaller, cleaner public plugin surface, not a
temporary dual model.

## Testing Strategy

### SDK tests

- `Destination::write_bulk()` default delegates to `write()`
- source feature trait compile-time assertions remain intact

### Macro/runtime tests

- destination macro glue no longer dispatches bulk by manifest feature presence
- source partitioned/CDC dispatch remains unchanged

### Real plugin tests

- `dest-postgres` compiles and passes tests with only `Destination`
- manifest metadata remains correct for bulk-capable destinations

### Scaffold tests

- `rapidbyte scaffold transform-example` succeeds
- generated transform scaffold contains expected files and unimplemented stubs
- source and destination scaffold tests continue to pass

## Resulting Public Model

Most plugin authors should only need to touch:

- `Source`
- `Destination`
- `Transform`

Plus, only when required by real protocol specialization:

- `PartitionedSource`
- `CdcSource`

This keeps the plugin SDK small, predictable, and aligned with the actual
runtime ABI.
