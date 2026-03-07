# Connector Protocol v4 API Design

## Goals

- Establish a standard, beautiful, and stable API contract across WIT, runtime, SDK, and first-party connectors.
- Remove role-specific naming drift and unify lifecycle shape.
- Make platform-level contracts strongly typed in WIT while preserving connector config flexibility.
- Provide a durable foundation for SDK ergonomics and consistent connector behavior.

## Scope

This design introduces breaking changes and intentionally does not preserve legacy v2/v3 compatibility paths.

In scope:

- New `rapidbyte:connector@4.0.0` WIT package surface.
- Runtime binding and conversion updates.
- SDK host FFI and connector macro updates.
- First-party connector migration.
- Protocol documentation update.

Out of scope:

- Backward-compatible shims.
- Connector config schema standardization in WIT.

## Design Decisions

### 1. Hybrid typing model

- Keep connector config as `config-json: string` in `open`.
- Make platform contracts typed: stream context, run request/summary, telemetry, state scope, lifecycle records.

Rationale:

- Connector config evolves quickly and is connector-specific.
- Platform contracts are cross-connector invariants and benefit from strict typing.

### 2. Canonical lifecycle and run shape

All roles follow the same lifecycle verb set:

- `open`
- `validate`
- `run`
- `close`

Role differences are represented in typed payloads (`run-phase`, role-specific summary field usage), not function naming.

### 3. Canonical naming grammar

- Transport: `noun-action` (for example `batch-emit`, `batch-next`).
- Network: `domain-action` (`tcp-connect`, `tcp-read`, ...).
- Telemetry: `telemetry-*` prefix (`telemetry-checkpoint`, `telemetry-metric`, `telemetry-dlq`).

## Proposed v4 WIT Surface

Package:

- `package rapidbyte:connector@4.0.0`

Interfaces:

- `core-types`
- `stream-types`
- `telemetry`
- `host-io`
- `source`
- `destination`
- `transform`

Worlds:

- `rapidbyte-host` (imports `host-io`)
- `rapidbyte-source` (imports `host-io`, exports `source`)
- `rapidbyte-destination` (imports `host-io`, exports `destination`)
- `rapidbyte-transform` (imports `host-io`, exports `transform`)

## Canonical Types

- `type session = u64`
- `enum connector-role { source, destination, transform }`
- `enum run-phase { discover, read, write, transform }`
- `record validation-report { status, message, warnings }`
- `record open-info { connector-id, role, protocol-version, capabilities, default-limits }`
- `record run-request { stream, phase, dry-run, max-records }`
- `record run-summary { role, read, write, transform }`
- `record checkpoint-event { kind, stream, id, cursor, records, bytes }`
- `record metric-event { name, value, labels }`

Error model keeps current metadata semantics and standardizes naming (`details` field as JSON-encoded string convention).

## Migration Plan

1. Replace WIT contract with v4 canonical definitions.
2. Update runtime generated bindings and conversion glue.
3. Update SDK host FFI and `#[connector(...)]` macro code generation.
4. Align SDK connector traits to canonical run shape.
5. Migrate first-party connectors (`source-postgres`, `dest-postgres`, `transform-sql`).
6. Update protocol docs and verify formatting/lint/tests/build/e2e.

## Success Criteria

- Single lifecycle vocabulary across all connector roles.
- Typed platform contracts in WIT (except connector config).
- SDK and first-party connectors compile only against v4.
- No legacy protocol shim code remains.
