# Benchmark, Validation, And CLI Refactor Design

## Summary

This tranche makes benchmark and validation surfaces truthful enough for
production use. The benchmark tool will stop exposing placeholder execution
paths, benchmark artifacts will carry real provenance and correctness data, and
pipeline validation will stop reporting unimplemented checks as green. The work
also separates engine data production from CLI rendering so the validation path
is reusable and testable.

## Goals

- Fully implement benchmark `compare`, `pipeline`, `source`, `destination`, and
  `transform` execution paths.
- Materialize transform connectors into rendered benchmark pipelines.
- Produce benchmark artifacts with real provenance and correctness outcomes.
- Preserve validation warnings across WIT, runtime, engine, and CLI layers.
- Move `rapidbyte check` rendering and exit policy fully into the CLI.
- Remove benchmark dead-code shields and prune dead or placeholder shapes
  surfaced by the compiler.

## Non-Goals

- Adding new benchmarked plugins beyond the adapters already present in the
  repository.
- Designing a remote benchmark artifact service.
- Reworking unrelated CLI or dev-shell behavior outside the paths touched by
  this refactor.

## Benchmark Architecture

`rapidbyte-benchmarks` remains the orchestration layer for scenario discovery,
environment resolution, warmups, measured iterations, artifact emission,
summary, and compare. Execution is split into explicit real benchmark modes
behind one shared benchmark execution pipeline:

- `pipeline`: render a full pipeline YAML including transforms, run a prebuilt
  `rapidbyte` binary, capture benchmark JSON, and validate assertions against
  runtime output.
- `source`: execute the source plugin in isolation through engine/runtime
  primitives with deterministic harness-owned consumption.
- `destination`: feed deterministic Arrow batches into the destination plugin in
  isolation through engine/runtime primitives.
- `transform`: execute one or more transform plugins in isolation with
  deterministic input batches and capture transform summaries directly.

All modes produce one canonical artifact type and share the same provenance,
correctness, grouping, and reporting logic.

## Scenario And Harness Model

Scenario parsing continues to be declarative, but all declared surfaces become
honest:

- pipeline scenarios may declare source, destination, and transform connectors
  and all of them must materialize into the rendered pipeline
- source scenarios may declare only one source connector
- destination scenarios may declare only one destination connector
- transform scenarios may declare one or more transform connectors and no source
  or destination connectors

The harness owns deterministic benchmark input generation. For the first full
implementation tranche, the existing Postgres adapters remain the concrete
benchmarked connectors and the SQL/validate transforms remain the concrete
transform adapters. Unsupported workload and adapter combinations fail
explicitly.

## Artifact Trust Model

`BenchmarkArtifact` is extended so the artifact states exactly what ran and how
it was validated:

- `benchmark_kind`
- `git_sha`
- `hardware_class`
- `scenario_fingerprint`
- shared execution flags
- structured correctness details

`git_sha` is taken from the current repository state. `hardware_class` is
provided explicitly by CLI input or environment and is never silently presented
as authoritative if it is unknown. Correctness is computed from runtime facts,
not hardcoded. Synthetic artifacts are not part of the normal benchmark run
path after this refactor.

Summary and comparison identity grouping must include at least:

- `scenario_id`
- `benchmark_kind`
- `suite_id`
- `git_sha`
- `hardware_class`
- `build_mode`
- execution flags
- `scenario_fingerprint`

This prevents grouping across different commits or materially different
scenario definitions.

## Compare And Summary

The benchmark binary gains a real Rust `compare` command. It uses the same
artifact loading and identity model as summary reporting, so grouping rules do
not drift between commands. The existing Python comparison tool can remain as a
secondary analysis utility, but it is no longer the only real implementation of
comparison behavior.

## Validation Semantics

`ValidationResult` gains `warnings: Vec<String>`. Warnings already exist in the
WIT contract, so the Rust shared type and both conversion directions are
updated to preserve them end to end.

Default SDK validation changes from:

- `Success("Validation not implemented")`

to:

- `Warning("Validation not implemented")`

This keeps unimplemented validation visible without turning it into a hard
failure by default. The CLI check path treats `Failed` as failure, `Warning` as
non-fatal but visible, and `Success` as pass.

## Engine And CLI Responsibilities

The engine no longer prints validation/config/state status lines. Instead it
returns structured check data covering:

- config validation outcomes
- plugin connectivity validation outcomes
- transform validation outcomes
- state backend outcome

The CLI owns:

- human-readable rendering
- warning/failure display
- exit-code policy

`std::process::exit(1)` is removed from the command implementation and replaced
with returned errors or typed status handling.

## Cleanup Scope

This tranche also includes targeted cleanup required to keep the result honest:

- remove blanket `#![cfg_attr(not(test), allow(dead_code))]` from benchmark
  modules
- delete or isolate any benchmark dead code exposed by the compiler
- consolidate duplicated formatting/helper code touched by this refactor
- remove dead or placeholder structs/events encountered in the modified paths
- harden scaffold output messaging so generated plugins are clearly marked as
  incomplete, and add a check for committed unimplemented scaffold stubs if it
  can be done without speculative infrastructure work

Broader dev-shell polish remains secondary unless it is directly touched by the
shared helper cleanup.

## Verification Strategy

- TDD for each behavior change: failing test first, then implementation
- unit tests for benchmark mode validation and dispatch
- unit tests for transform materialization into benchmark pipeline YAML
- unit tests for artifact provenance, correctness, summary grouping, and compare
- targeted integration tests for source, destination, and transform benchmark
  harnesses
- validation round-trip tests proving warnings survive SDK macro generation,
  runtime conversion, engine results, and CLI output
- command-level verification that benchmark runs use a prebuilt binary path
  instead of `cargo run`

## Outcome

After this refactor, benchmarks that are publicly exposed will actually run,
benchmark artifacts will be trustworthy enough for regression analysis, and the
validation/check path will stop conflating unimplemented checks with success.
