# Transform SQL Stream Names Design

## Summary

Replace the `transform-sql` plugin’s synthetic `input` table contract with a
stream-name-based contract. Each transform invocation should expose the incoming
batch to DataFusion under the current pipeline stream name, and SQL queries must
reference that stream name directly, for example `FROM users`.

## Goals

- Remove the hardcoded `input` table contract from `transform-sql`.
- Make SQL transforms reference the actual pipeline stream name users already
  configure in source streams.
- Keep the contract connector-agnostic by using the pipeline stream name rather
  than source-specific fully-qualified catalog names.
- Update tests, docs, examples, and benchmarks to use the new stream-name-based
  SQL syntax consistently.

## Non-Goals

- Maintaining backward compatibility for `FROM input`.
- Introducing aliases or dual-registration of both `input` and the stream name.
- Adding multi-table SQL semantics or cross-stream joins in this slice.
- Reworking transform execution beyond table naming and validation semantics.

## Current Problems

- `plugins/transforms/sql/src/main.rs` validates that queries reference
  `FROM input`, which hardcodes a synthetic table name into the plugin API.
- `plugins/transforms/sql/src/transform.rs` registers each batch as a DataFusion
  table named `input`.
- Docs and examples in `docs/PROTOCOL.md`, `docs/PLUGIN_DEV.md`, `docs/DRAFT.md`,
  benchmarks, and e2e tests all reinforce that synthetic table name.
- Users naturally expect SQL to reference the stream they configured, for
  example `users`, not an internal placeholder.

## Alternatives Considered

### 1. Recommended: expose the current pipeline stream name

Register each batch under `ctx.stream_name()` and require SQL to reference that
exact stream name.

Pros:
- Matches pipeline configuration.
- Connector-agnostic.
- Ergonomic SQL for common cases such as `FROM users`.
- Clean long-term contract with no synthetic placeholder.

Cons:
- Requires touching validation, runtime registration, tests, and docs.

### 2. Use fully-qualified source names like `public.users`

Expose batches under connector-specific source identifiers.

Pros:
- More explicit for some database-backed sources.

Cons:
- Couples transform semantics to source-specific naming.
- Awkward for non-Postgres sources.
- More cumbersome SQL because qualified identifiers need additional care.

### 3. Keep `input` and add stream-name aliases

Support both names for a transition period.

Pros:
- Easier migration.

Cons:
- Preserves legacy ambiguity.
- Not aligned with the requested clean break.
- More code and docs complexity for a contract we do not want long term.

## Recommended Design

### Table Naming Contract

The only valid SQL table name for a transform batch should be the current
pipeline stream name.

Example:

```yaml
source:
  streams:
    - name: users

transforms:
  - use: transform-sql
    config:
      query: "SELECT id, lower(email) AS email FROM users"
```

`FROM input` becomes invalid with no compatibility alias.

### Validation Model

Validation should split into two concerns:

- syntax and single-statement validation can remain independent of stream name
- semantic “does this query reference the current stream name?” validation must
  happen where the stream name is available

That means:

- `init()` should continue to normalize and parse the SQL query
- `validate()` should use `Context` to check that the query references the
  current stream name
- runtime planning should also fail clearly if the query does not reference the
  current stream name

This avoids pretending init-time validation can check a stream-specific
contract without stream-specific context.

### Runtime Execution

`plugins/transforms/sql/src/transform.rs` should:

- register incoming batches as a DataFusion MemTable named after the current
  stream name
- deregister the previous table using that same stream name
- plan and execute the cached statement against that stream-named table

The runtime should not register `input` at all.

### Error Messages

All error messages should use stream-name language, for example:

- `SQL query must reference current stream table 'users'`

Avoid mentioning `input` anywhere in runtime or validation messages after this
change.

### Documentation and Examples

Update:

- `docs/PROTOCOL.md`
- `docs/PLUGIN_DEV.md`
- `docs/DRAFT.md`
- any benchmark or example YAML that uses `SELECT * FROM input`
- e2e tests and snapshots

All first-party examples should show stream names such as `users`.

## Testing Strategy

Add focused coverage for:

- validation accepting `FROM users` and rejecting queries that do not reference
  the current stream name
- runtime registration under the stream name instead of `input`
- old `FROM input` queries failing with a clear message
- e2e transform pipelines succeeding with stream-name SQL and failing with the
  legacy placeholder

## Change Scope

### Modified files

- `plugins/transforms/sql/src/main.rs`
- `plugins/transforms/sql/src/transform.rs`
- `plugins/transforms/sql/src/config.rs`
- `tests/e2e/tests/transform.rs`
- `benchmarks/src/pipeline.rs`
- `docs/PROTOCOL.md`
- `docs/PLUGIN_DEV.md`
- `docs/DRAFT.md`

### New files

- none required beyond tests and docs updates in this slice

## Outcome

After this change, `transform-sql` will use the actual stream name as its only
table contract. SQL transforms become easier to read, better aligned with
pipeline configuration, and free of the legacy `input` placeholder.
