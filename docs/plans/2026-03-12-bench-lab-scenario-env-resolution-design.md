# bench-lab Scenario Environment Resolution Design

## Summary

`just bench-lab <scenario>` should use the scenario manifest as the default
source of truth for benchmark environment selection. A scenario that declares
`environment.ref: local-bench-postgres` should run against that environment by
default; a distributed scenario that declares
`environment.ref: local-bench-distributed-postgres` should do the same.

An explicit `env=...` recipe argument must still override the manifest when the
caller intentionally wants a different environment profile.

## Problem

The current `Justfile` hardcodes `env="local-bench-postgres"` for every
`bench-lab` invocation. That works for local Postgres lab scenarios, but it
breaks distributed scenarios such as
`pg_dest_copy_release_distributed`, which already declare a different
environment profile in their manifest. Users are forced to pass a manual
override, and even that is awkward because the `just` recipe syntax makes
`env=...` easy to misuse.

This creates two problems:

1. The benchmark manifest is not authoritative, even though it already contains
   the correct environment reference.
2. Distributed and non-distributed scenarios are not equally ergonomic to run.

## Goals

- Make `just bench-lab <scenario>` work for both local and distributed
  scenarios without extra flags in the common case.
- Keep scenario manifests authoritative for default environment selection.
- Preserve `env=...` as an explicit manual override.
- Keep the benchmark runner CLI contract explicit and unchanged.

## Non-Goals

- Redesign benchmark scenario schema.
- Move environment inference into the Rust benchmark runner.
- Add a second distributed-only `just` command.

## Design

### Resolution Order

`bench-lab` should resolve the effective environment profile in this order:

1. Explicit `env=...` override from the `just` invocation.
2. Scenario manifest `environment.ref`.
3. Legacy fallback to `local-bench-postgres` only if the scenario omits
   `environment.ref`.

### Behavior

- Local scenarios such as `pg_dest_copy_release` continue to run exactly as
  before because they already declare `local-bench-postgres`.
- Distributed scenarios such as `pg_dest_copy_release_distributed` pick up
  `local-bench-distributed-postgres` automatically.
- `env=...` remains available for intentional overrides and experimentation.

### Failure Handling

- Missing scenario file: fail clearly and name the scenario.
- Missing referenced environment profile: fail clearly and name the profile.
- Bad manual override: let the benchmark runner fail normally with that
  override value.

## Implementation Boundary

Keep the change in the `Justfile` wrapper and supporting docs only. The Rust
benchmark runner already handles explicit `--env-profile` correctly, so this is
an ergonomics fix at the workflow layer rather than a runner behavior change.

## Validation

- `just bench-lab pg_dest_copy_release` should still resolve to
  `local-bench-postgres`.
- `just bench-lab pg_dest_copy_release_distributed` should now resolve to
  `local-bench-distributed-postgres` automatically.
- `just bench-lab pg_dest_copy_release_distributed env=local-bench-postgres`
  should still honor the explicit override.
