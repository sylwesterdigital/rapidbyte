# Benchmark Summary Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a benchmark `summary` command that prints readable latency and throughput statistics for a single JSONL artifact set.

**Architecture:** Extend the Rust benchmark CLI with a `summary` subcommand, add a small summary module that loads and groups artifact rows by benchmark identity, and render a compact terminal report that includes records/sec and MB/sec throughput. Keep the existing Python comparison layer unchanged and add a `just` wrapper plus docs for the new local inspection path.

**Tech Stack:** Rust, Clap, Serde JSON, Markdown docs, Just recipes.

---

### Task 1: Add CLI coverage for the new summary command

**Files:**
- Modify: `benchmarks/src/cli.rs`

**Step 1: Write the failing test**

Add a CLI parsing test that expects:
- `rapidbyte-benchmarks summary target/benchmarks/lab/pg_dest_copy.jsonl`
- parses into a new `BenchCommand::Summary`
- captures the artifact path correctly

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks cli::tests::summary_args_accept_artifact_path
```

Expected:
- FAIL because the `summary` subcommand does not exist yet

**Step 3: Write minimal implementation**

Update `benchmarks/src/cli.rs` to:
- add `BenchCommand::Summary`
- add a `SummaryArgs` struct with one positional `artifact: PathBuf`

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks cli::tests::summary_args_accept_artifact_path
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/cli.rs
git commit -m "feat(bench): add summary cli command"
```

### Task 2: Build the summary loader, stats, and renderer

**Files:**
- Create: `benchmarks/src/summary.rs`

**Step 1: Write the failing tests**

Add unit tests for:
- loading a small JSONL artifact fixture into rows
- grouping rows by benchmark identity
- computing median/min/max for `records_per_sec`, `mb_per_sec`, and `duration_secs`
- counting correctness pass/fail
- rendering output that contains scenario id, sample count, records/sec, MB/sec, and latency
- failing clearly on empty input or missing required metrics

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::
```

Expected:
- FAIL because the module and functions do not exist yet

**Step 3: Write minimal implementation**

Implement `benchmarks/src/summary.rs` with:
- artifact loading from JSONL
- identity grouping that matches the comparison layer
- summary stat computation
- plain terminal rendering for one block per identity group

Keep the implementation narrow:
- only required metrics
- no JSON output
- no trend/history features

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "feat(bench): add artifact summary reporting"
```

### Task 3: Wire the summary command into the benchmark binary

**Files:**
- Modify: `benchmarks/src/main.rs`

**Step 1: Write the failing test or readback expectation**

Add or update a testable expectation that the binary dispatches `summary`
through the new module and prints the rendered report for a valid artifact file.

If a direct binary test is too heavy, add a focused function-level test in
`summary.rs` and use `main.rs` readback verification for the dispatch path.

**Step 2: Run verification to confirm it fails or is absent**

Run:

```bash
sed -n '1,200p' benchmarks/src/main.rs
```

Expected:
- only `run` and `compare` are handled

**Step 3: Write minimal implementation**

Update `benchmarks/src/main.rs` to:
- `mod summary;`
- handle `BenchCommand::Summary`
- load the target artifact file
- print the rendered summary

**Step 4: Run targeted verification**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- summary target/benchmarks/lab/pg_dest_copy.jsonl
```

Expected:
- prints a readable report with records/sec, MB/sec, latency, and correctness

**Step 5: Commit**

```bash
git add benchmarks/src/main.rs
git commit -m "feat(bench): wire summary command into benchmark runner"
```

### Task 4: Add local ergonomics and documentation

**Files:**
- Modify: `Justfile`
- Modify: `docs/BENCHMARKING.md`

**Step 1: Write the failing expectation**

Identify the missing local entrypoint and docs coverage for single-artifact
inspection.

**Step 2: Re-read current files to confirm the gap**

Run:

```bash
sed -n '96,130p' Justfile
sed -n '1,120p' docs/BENCHMARKING.md
```

Expected:
- no `bench-summary` helper exists
- docs only describe run and compare flows

**Step 3: Write minimal implementation**

Update:
- `Justfile` with `bench-summary artifact`
- `docs/BENCHMARKING.md` with example usage for a lab artifact summary

**Step 4: Re-read the changes**

Run:

```bash
sed -n '96,135p' Justfile
sed -n '1,140p' docs/BENCHMARKING.md
```

Expected:
- documented local summary command exists
- docs describe both direct CLI and `just` wrapper usage

**Step 5: Commit**

```bash
git add Justfile docs/BENCHMARKING.md
git commit -m "docs(bench): document artifact summary command"
```

### Task 5: Verify the feature end to end

**Files:**
- Verify in-place against:
  - `benchmarks/src/cli.rs`
  - `benchmarks/src/summary.rs`
  - `benchmarks/src/main.rs`
  - `Justfile`
  - `docs/BENCHMARKING.md`

**Step 1: Check malformed diffs**

Run:

```bash
git diff --check
```

Expected:
- no output

**Step 2: Run the benchmark crate tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected:
- PASS

**Step 3: Run the new summary command on a real artifact**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- summary target/benchmarks/lab/pg_dest_copy.jsonl
```

Expected:
- summary output includes:
  - `pg_dest_copy`
  - `records/sec`
  - `MB/sec`
  - `duration`
  - correctness counts

**Step 4: Verify the `just` wrapper**

Run:

```bash
just bench-summary target/benchmarks/lab/pg_dest_copy.jsonl
```

Expected:
- same readable summary output via the repo wrapper

**Step 5: Capture final branch state**

Run:

```bash
git status --short
git log --oneline -n 5
```

Expected:
- only intended files changed for this feature
- recent commits reflect the summary feature work

**Step 6: Commit**

If verification required cleanup:

```bash
git add benchmarks/src/cli.rs benchmarks/src/summary.rs benchmarks/src/main.rs Justfile docs/BENCHMARKING.md
git commit -m "chore(bench): finalize artifact summary command"
```
