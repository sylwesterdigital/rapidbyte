# Benchmark Adapters And E2E PR Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a real benchmark adapter architecture and use it to make the PR
benchmark suite execute a deterministic end-to-end Rapidbyte pipeline instead
of emitting synthetic placeholder artifacts.

**Architecture:** Introduce adapter contracts under `benchmarks/src/adapters/`
so benchmark-core no longer hardcodes Postgres pipeline rendering, fixture
seeding, or connector-specific config. Implement Postgres-backed source and
destination adapters first, route `pipeline` scenarios through them, surface
explicit errors for unsupported benchmark kinds and workload families, and
switch the PR smoke scenario to a real environment-backed execution path.

**Tech Stack:** Rust, Cargo tests, GitHub Actions benchmark runner assumptions,
Rapidbyte CLI/runtime/plugins, YAML scenario manifests, JSON benchmark
artifacts, local Postgres benchmark environment profiles.

---

### Task 1: Define adapter contracts and adapter-backed scenario validation

**Files:**
- Modify: `benchmarks/src/adapters/mod.rs`
- Modify: `benchmarks/src/adapters/source.rs`
- Modify: `benchmarks/src/adapters/destination.rs`
- Modify: `benchmarks/src/adapters/transform.rs`
- Modify: `benchmarks/src/scenario.rs`
- Test: benchmark adapter/scenario unit tests in the same files

**Step 1: Write the failing tests**

Add tests that prove:
- a scenario connector ref resolves to a supported adapter implementation
- unsupported connector/plugin combinations fail with a scenario-specific error
- `BenchmarkKind::Source`, `Destination`, and `Transform` are recognized as
  explicit execution modes even when concrete harnesses are not implemented yet
- invalid scenario shape for a kind fails validation before execution starts

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks adapters scenario
```

Expected:
- FAIL because adapter contracts and adapter-backed validation do not exist yet

**Step 3: Write the minimal implementation**

Implement:
- shared adapter traits and/or resolver types in `adapters/mod.rs`
- concrete Postgres source and destination adapter definitions
- transform adapter contract stubs with explicit not-yet-implemented behavior
- scenario validation that uses adapter resolution instead of only checking
  feature requirements

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks adapters scenario
```

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/adapters benchmarks/src/scenario.rs
git commit -m "feat(bench): add benchmark adapter contracts"
```

### Task 2: Move pipeline construction and fixture seeding behind adapters

**Files:**
- Modify: `benchmarks/src/pipeline.rs`
- Modify: `benchmarks/src/workload.rs`
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/environment.rs`
- Test: runner/workload/pipeline tests in the same files

**Step 1: Write the failing tests**

Add tests that prove:
- pipeline rendering uses adapter-produced connector config instead of direct
  Postgres-specific logic embedded in benchmark-core
- `WorkloadFamily::NarrowAppend` resolves to a real Postgres fixture plan via
  adapters
- unsupported workload-family plus adapter combinations fail explicitly
- pipeline scenario execution routes through an adapter-backed execution plan

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks pipeline workload runner
```

Expected:
- FAIL because benchmark-core still owns Postgres-specific rendering/seeding

**Step 3: Write the minimal implementation**

Implement:
- adapter-owned pipeline config materialization
- adapter-owned fixture/seed preparation for Postgres `NarrowAppend`
- runner changes that dispatch `BenchmarkKind::Pipeline` through adapter-backed
  execution
- explicit "not implemented" errors for unsupported benchmark kinds and real
  workload families

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks pipeline workload runner
```

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/pipeline.rs benchmarks/src/workload.rs benchmarks/src/runner.rs benchmarks/src/environment.rs
git commit -m "refactor(bench): route pipeline benchmarks through adapters"
```

### Task 3: Make PR smoke a real e2e benchmark scenario

**Files:**
- Modify: `benchmarks/scenarios/pr/smoke.yaml`
- Modify: `benchmarks/environments/local-dev-postgres.yaml`
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/scenario.rs`
- Modify: `benchmarks/src/workload.rs`
- Test: benchmark runner/scenario integration tests

**Step 1: Write the failing tests**

Add tests that prove:
- the PR smoke scenario no longer resolves to the synthetic success stub
- a PR scenario with a real environment reference requires environment profile
  resolution
- the runner invokes the real execution path for the PR smoke scenario
- emitted artifacts from the PR scenario contain real benchmark JSON-derived
  metrics rather than only placeholder synthetic metrics

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner scenario workload
```

Expected:
- FAIL because the PR smoke scenario is still synthetic and bypasses real
  execution

**Step 3: Write the minimal implementation**

Implement:
- a deterministic real PR smoke scenario bound to the committed local Postgres
  benchmark environment
- runner logic that no longer treats the PR smoke benchmark as synthetic stub
  output
- any scenario/workload validation needed so real PR runs fail loudly when
  environment setup is missing

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner scenario workload
```

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/scenarios/pr/smoke.yaml benchmarks/environments/local-dev-postgres.yaml benchmarks/src/runner.rs benchmarks/src/scenario.rs benchmarks/src/workload.rs
git commit -m "feat(bench): run PR smoke benchmarks end to end"
```

### Task 4: Verify benchmark behavior end to end

**Files:**
- Verify in-place against:
  - `benchmarks/src/adapters/*.rs`
  - `benchmarks/src/runner.rs`
  - `benchmarks/src/pipeline.rs`
  - `benchmarks/scenarios/pr/smoke.yaml`

**Step 1: Check for malformed diffs**

Run:

```bash
git diff --check
```

Expected: no output

**Step 2: Run benchmark crate tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected: PASS

**Step 3: Run the PR benchmark suite locally**

Run:

```bash
just bench-pr
```

Expected:
- the PR benchmark runner executes the real benchmark path
- artifacts are written under `target/benchmarks/pr/`
- comparison against `benchmarks/baselines/main/pr.jsonl` completes

**Step 4: Run repo verification**

Run:

```bash
just ci
```

Expected: PASS

**Step 5: Commit**

If verification required cleanup:

```bash
git add benchmarks .github/workflows README.md CONTRIBUTING.md Justfile
git commit -m "chore(bench): finalize adapter-backed e2e benchmark flow"
```
