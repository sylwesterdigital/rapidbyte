# Benchmark Validation Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fully implement the exposed benchmark modes and comparison flow, make benchmark artifacts and validation results truthful, and separate engine check data from CLI rendering.

**Architecture:** Keep `rapidbyte-benchmarks` as the orchestration layer, but move all exposed benchmark modes onto real harnesses with one canonical artifact pipeline. Extend shared validation types so warnings survive the full stack, then refactor `rapidbyte check` so the engine returns structured data and the CLI owns rendering and exit policy.

**Tech Stack:** Rust, Clap, serde/serde_json/serde_yaml, Tokio, Wasmtime runtime bindings, existing Rapidbyte engine/runtime/plugin crates

---

### Task 1: Add failing tests for truthful validation semantics

**Files:**
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`
- Modify: `crates/rapidbyte-types/src/error.rs`
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`
- Modify: `crates/rapidbyte-cli/src/commands/check.rs`
- Test: `crates/rapidbyte-sdk/src/plugin.rs`
- Test: `crates/rapidbyte-runtime/src/bindings.rs`
- Test: `crates/rapidbyte-cli/src/commands/check.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- default validation returns `ValidationStatus::Warning`
- `ValidationResult` preserves `warnings`
- runtime binding conversion preserves WIT warnings
- CLI rendering prints warnings and does not treat them as success

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-sdk plugin::tests::`
Expected: FAIL because default validation still returns `Success`

Run: `cargo test -p rapidbyte-runtime bindings::tests::`
Expected: FAIL because warnings are dropped during conversion

**Step 3: Write minimal implementation**

Implement:
- `warnings: Vec<String>` on `ValidationResult`
- warning-preserving conversion in runtime bindings and SDK macros
- warning default in SDK plugin helpers
- CLI rendering/assertion helpers that distinguish success, warning, and failure

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-sdk plugin::tests::`
Expected: PASS

Run: `cargo test -p rapidbyte-runtime bindings::tests::`
Expected: PASS

Run: `cargo test -p rapidbyte-cli check`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/plugin.rs crates/rapidbyte-types/src/error.rs crates/rapidbyte-runtime/src/bindings.rs crates/rapidbyte-sdk/macros/src/plugin.rs crates/rapidbyte-cli/src/commands/check.rs
git commit -m "fix(validation): preserve warnings and downgrade default validation"
```

### Task 2: Refactor engine check results away from CLI printing

**Files:**
- Modify: `crates/rapidbyte-engine/src/result.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs`
- Modify: `crates/rapidbyte-cli/src/commands/check.rs`
- Test: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-cli/src/commands/check.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- `check_pipeline` returns config and plugin validation results without printing
- CLI check returns `Ok(())` for success and warnings, `Err(...)` for failures
- `std::process::exit(1)` is no longer part of the command path

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-cli check`
Expected: FAIL because the command currently exits the process on failure

**Step 3: Write minimal implementation**

Introduce structured check result fields for config validation and state status,
remove `println!`/`eprintln!` from engine check orchestration, and centralize
all check rendering and exit policy in the CLI command.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine orchestrator::`
Expected: PASS

Run: `cargo test -p rapidbyte-cli check`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/result.rs crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/lib.rs crates/rapidbyte-cli/src/commands/check.rs
git commit -m "refactor(check): return structured validation data from engine"
```

### Task 3: Add failing tests for benchmark pipeline transform materialization

**Files:**
- Modify: `benchmarks/src/pipeline.rs`
- Modify: `benchmarks/src/adapters/mod.rs`
- Modify: `benchmarks/src/adapters/transform.rs`
- Test: `benchmarks/src/pipeline.rs`
- Test: `benchmarks/src/adapters/mod.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- pipeline scenarios with transform connectors render a `transforms:` section
- transform connector config survives adapter materialization
- invalid transform declarations fail clearly

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks pipeline::tests::`
Expected: FAIL because rendered pipeline YAML omits transforms

**Step 3: Write minimal implementation**

Add transform materialization support to the adapter pipeline preparation layer
and render transforms into benchmark pipeline YAML.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks pipeline::tests::`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/pipeline.rs benchmarks/src/adapters/mod.rs benchmarks/src/adapters/transform.rs
git commit -m "feat(bench): render transforms in benchmark pipelines"
```

### Task 4: Add failing tests for real benchmark artifact provenance and grouping

**Files:**
- Modify: `benchmarks/src/artifact.rs`
- Modify: `benchmarks/src/summary.rs`
- Modify: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/artifact.rs`
- Test: `benchmarks/src/summary.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- artifacts record `benchmark_kind`, `git_sha`, `hardware_class`, and scenario fingerprint
- correctness data comes from runtime/assertion outcomes, not hardcoded values
- summary identity includes `git_sha`
- benchmark invocation uses a prebuilt binary path rather than `cargo run`

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks summary:: runner::tests::`
Expected: FAIL because identity ignores `git_sha` and the runner still uses `cargo run`

**Step 3: Write minimal implementation**

Extend artifact types, plumb provenance resolution through the runner, compute
correctness from execution results, and switch benchmark command construction to
a prebuilt `rapidbyte` binary path.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks artifact:: summary:: runner::tests::`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/artifact.rs benchmarks/src/summary.rs benchmarks/src/runner.rs
git commit -m "feat(bench): make artifacts and grouping truthful"
```

### Task 5: Add failing tests for real benchmark compare command

**Files:**
- Modify: `benchmarks/src/cli.rs`
- Modify: `benchmarks/src/main.rs`
- Create: `benchmarks/src/compare.rs`
- Test: `benchmarks/src/cli.rs`
- Test: `benchmarks/src/compare.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- CLI compare args parse thresholds and sample requirements
- Rust compare groups candidate and baseline artifacts by the same identity as summary
- regressions and correctness failures are rendered deterministically

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks cli::tests:: compare::tests::`
Expected: FAIL because compare logic is only a skeleton in `main.rs`

**Step 3: Write minimal implementation**

Implement a Rust compare module, wire it into the benchmark binary, and keep the
rendered report consistent with artifact summary identity rules.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks cli::tests:: compare::tests::`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/cli.rs benchmarks/src/main.rs benchmarks/src/compare.rs
git commit -m "feat(bench): implement benchmark compare command"
```

### Task 6: Add failing tests for isolated source benchmark execution

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/workload.rs`
- Modify: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- `BenchmarkKind::Source` executes through a real harness path
- source benchmark artifacts report source-specific metrics and correctness
- unsupported source workload/adapter combinations fail explicitly

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::source`
Expected: FAIL because source execution currently returns "not implemented yet"

**Step 3: Write minimal implementation**

Implement the isolated source benchmark harness using engine/runtime runner
primitives and deterministic harness-owned batch consumption.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::source`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/workload.rs benchmarks/src/scenario.rs
git commit -m "feat(bench): implement source benchmark execution"
```

### Task 7: Add failing tests for isolated destination benchmark execution

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/workload.rs`
- Modify: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- `BenchmarkKind::Destination` executes through a real harness path
- destination benchmark artifacts report destination-specific metrics and correctness
- deterministic input batches are fed into the destination harness

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::destination`
Expected: FAIL because destination execution currently returns "not implemented yet"

**Step 3: Write minimal implementation**

Implement the isolated destination harness using deterministic Arrow batch input
generation and engine/runtime destination runner primitives.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::destination`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/workload.rs benchmarks/src/scenario.rs
git commit -m "feat(bench): implement destination benchmark execution"
```

### Task 8: Add failing tests for isolated transform benchmark execution

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/workload.rs`
- Modify: `benchmarks/src/scenario.rs`
- Modify: `benchmarks/src/adapters/transform.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- `BenchmarkKind::Transform` executes through a real harness path
- multiple transform connectors execute in order
- transform benchmark artifacts report transform metrics and correctness

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::transform`
Expected: FAIL because transform execution currently returns "not implemented yet"

**Step 3: Write minimal implementation**

Implement the isolated transform harness using deterministic input batches and
engine/runtime transform runner primitives.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks runner::tests::transform`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/workload.rs benchmarks/src/scenario.rs benchmarks/src/adapters/transform.rs
git commit -m "feat(bench): implement transform benchmark execution"
```

### Task 9: Remove benchmark dead-code shields and cleanup touched placeholders

**Files:**
- Modify: `benchmarks/src/artifact.rs`
- Modify: `benchmarks/src/environment.rs`
- Modify: `benchmarks/src/output.rs`
- Modify: `benchmarks/src/metrics.rs`
- Modify: `benchmarks/src/summary.rs`
- Modify: `benchmarks/src/scenario.rs`
- Modify: `benchmarks/src/workload.rs`
- Modify: `crates/rapidbyte-engine/src/execution.rs`
- Modify: `crates/rapidbyte-engine/src/progress.rs`
- Modify: `crates/rapidbyte-dev/src/display.rs`
- Modify: `crates/rapidbyte-cli/src/output/format.rs`
- Test: touched unit tests

**Step 1: Write the failing tests**

Add tests where needed for:
- dry-run stream collection without partially invalid structs
- shared formatting behavior using consistent binary units
- progress event behavior for any variant retained after cleanup

**Step 2: Run lint/test to verify failures**

Run: `cargo clippy -p rapidbyte-benchmarks --all-targets -- -D warnings`
Expected: FAIL because removing blanket dead-code suppression should expose unused items

**Step 3: Write minimal implementation**

Remove dead-code shields, delete dead code, move test-only helpers under
`#[cfg(test)]`, consolidate formatting/helpers touched by this refactor, and
clean up placeholder shapes exposed by the compiler.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-benchmarks`
Expected: PASS

Run: `cargo clippy -p rapidbyte-benchmarks --all-targets -- -D warnings`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src crates/rapidbyte-engine/src/execution.rs crates/rapidbyte-engine/src/progress.rs crates/rapidbyte-dev/src/display.rs crates/rapidbyte-cli/src/output/format.rs
git commit -m "refactor(bench): remove dead code shields and cleanup helpers"
```

### Task 10: Harden scaffold messaging for incomplete generated plugins

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`
- Modify: generated scaffold templates under the scaffold command path
- Test: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- generated README/comments clearly mark scaffold output as non-production-ready
- generated stubs include explicit unimplemented markers suitable for CI detection

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-cli scaffold`
Expected: FAIL because scaffold output is not strict enough about incompleteness

**Step 3: Write minimal implementation**

Strengthen generated README/comments and surface stable unimplemented markers.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-cli scaffold`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "chore(scaffold): mark generated plugins as incomplete"
```

### Task 11: Final verification

**Files:**
- Verify: `benchmarks/src/*.rs`
- Verify: `crates/rapidbyte-sdk/src/plugin.rs`
- Verify: `crates/rapidbyte-runtime/src/bindings.rs`
- Verify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Verify: `crates/rapidbyte-cli/src/commands/check.rs`

**Step 1: Run focused Rust test suites**

Run: `cargo test -p rapidbyte-benchmarks`
Expected: PASS

Run: `cargo test -p rapidbyte-sdk`
Expected: PASS

Run: `cargo test -p rapidbyte-runtime`
Expected: PASS

Run: `cargo test -p rapidbyte-engine`
Expected: PASS

Run: `cargo test -p rapidbyte-cli`
Expected: PASS

**Step 2: Run lint on touched benchmark and CLI crates**

Run: `cargo clippy -p rapidbyte-benchmarks -p rapidbyte-cli -p rapidbyte-engine -p rapidbyte-runtime -p rapidbyte-sdk --all-targets -- -D warnings`
Expected: PASS

**Step 3: Run command-level verification**

Run: `cargo run --manifest-path benchmarks/Cargo.toml -- summary benchmarks/baselines/main/pr.jsonl`
Expected: readable benchmark summary output

Run: `cargo run --manifest-path benchmarks/Cargo.toml -- compare benchmarks/baselines/main/pr.jsonl benchmarks/baselines/main/pr.jsonl --min-samples 1`
Expected: deterministic all-ok comparison output

**Step 4: Commit**

```bash
git add benchmarks crates/rapidbyte-sdk crates/rapidbyte-runtime crates/rapidbyte-engine crates/rapidbyte-cli
git commit -m "feat: complete benchmark and validation refactor"
```
