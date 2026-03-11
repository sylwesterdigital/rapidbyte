# Repo-Wide Cleanup Design

**Date:** 2026-03-11

## Goal

Complete the next repo-wide cleanup tranche by fixing correctness and trust gaps that remain after the benchmark/validation refactor:
- remove duplicated human-formatting helpers
- clean up awkward progress/dry-run placeholder shapes
- harden scaffolded plugin output and block committed scaffold stubs in CI

## Scope

In scope:
- shared byte/count formatting used by CLI and dev shell
- `ProgressEvent::Error` lifecycle decision
- dry-run stream result construction cleanup
- scaffold warning text and CI-backed `UNIMPLEMENTED` guard

Out of scope:
- dev-shell parser/completer UX changes
- benchmark adapter helper dedupe beyond what this tranche directly touches
- broader orchestrator/event redesign

## Current Problems

### 1. Formatting drift

[`crates/rapidbyte-cli/src/output/format.rs`](/home/netf/rapidbyte/crates/rapidbyte-cli/src/output/format.rs) and [`crates/rapidbyte-dev/src/display.rs`](/home/netf/rapidbyte/crates/rapidbyte-dev/src/display.rs) both implement `format_bytes` and `format_count`. They use powers-of-1024 while labeling values as `KB`/`MB`/`GB`, which is inconsistent with the benchmark summary output that already uses `MiB`.

### 2. Dead progress placeholder

[`crates/rapidbyte-cli/src/output/progress.rs`](/home/netf/rapidbyte/crates/rapidbyte-cli/src/output/progress.rs) matches on `ProgressEvent::Error`, but the orchestrator path is centered around phase, batch, stream, and retry events. A variant that is not part of real emission paths adds noise to the event API and to the CLI.

### 3. Partially-invalid dry-run result construction

[`collect_dry_run_frames`](/home/netf/rapidbyte/crates/rapidbyte-engine/src/orchestrator.rs) currently returns `DryRunStreamResult { stream_name: String::new(), ... }`, and the caller patches the stream name later. That shape is valid only after extra mutation, which is avoidable.

### 4. Scaffold trust gap

[`crates/rapidbyte-cli/src/commands/scaffold.rs`](/home/netf/rapidbyte/crates/rapidbyte-cli/src/commands/scaffold.rs) intentionally generates stubs with `UNIMPLEMENTED` markers, but the output does not loudly enough communicate “not production-ready”, and CI does not currently fail if such scaffold stubs are committed under [`plugins/`](/home/netf/rapidbyte/plugins).

## Design

### Shared formatting module

Create a small shared formatting module in `rapidbyte-types` that exposes:
- `format_bytes_binary(bytes: u64) -> String`
- `format_count(n: u64) -> String`

The byte formatter will use binary units and label them explicitly as `KiB`, `MiB`, and `GiB`. CLI and dev-shell call sites will be updated to use the shared implementation. Existing CLI-only helpers for rates and durations remain in the CLI crate because they are not duplicated elsewhere.

Why `rapidbyte-types`:
- already a low-level shared crate
- avoids pulling CLI/dev dependencies in the wrong direction
- keeps the formatting logic reusable without coupling it to engine code

### Progress event cleanup

Remove `ProgressEvent::Error` unless a real producer exists in the main execution path. The current evidence says it is dead API rather than partially-wired functionality, so the cleanest change is:
- delete the variant from the engine progress model
- remove the unused CLI spinner branch
- update any tests accordingly

This narrows the event contract to the events the engine actually emits.

### Dry-run result construction cleanup

Change `collect_dry_run_frames` so it receives the stream name and returns a fully-valid `DryRunStreamResult` in one shot. This avoids placeholder construction and keeps the dry-run execution path easier to reason about.

### Scaffold hardening

Harden scaffold output in two layers:

1. Generated files:
- add a generated `README.md`
- add top-of-file comments to stub source files
- make the command output explicitly say scaffolded plugins are not production-ready

2. CI enforcement:
- add a small repo script that scans committed files under `plugins/` for scaffold `UNIMPLEMENTED` markers
- run it from the main CI workflow
- add a focused test for the script if practical, otherwise keep the script simple and deterministic

The guard should be narrow enough to avoid false positives outside committed plugin code.

## Testing Strategy

- unit tests for shared formatting in the new common module
- updated CLI/dev-shell tests for the new `KiB`/`MiB`/`GiB` labels where needed
- engine tests covering valid dry-run stream result construction
- compile/test verification that removing `ProgressEvent::Error` does not break the execution path
- scaffold tests asserting generated warnings/README content
- CI script exercised locally during verification

## Risks And Mitigations

### Output string drift

Changing from `MB` to `MiB` can break exact-string tests. That is acceptable and should be updated deliberately because the current labels are inaccurate.

### CI false positives

A broad `UNIMPLEMENTED` scan could catch unrelated examples or documentation. Restrict the guard to committed plugin source trees under `plugins/` and to scaffold marker strings we own.

### Dependency direction

Moving shared formatting into the wrong crate could create coupling problems. Keeping only the duplicated low-level helpers in `rapidbyte-types` avoids that.

## Acceptance Criteria

- CLI and dev-shell share one byte/count formatting implementation
- binary-unit labels are accurate and consistent
- `ProgressEvent::Error` is either emitted for real or removed; no dead branch remains
- dry-run stream results are constructed with valid stream names immediately
- scaffold output clearly marks generated plugins as non-production-ready
- CI fails if committed plugin scaffold stubs still contain `UNIMPLEMENTED`
