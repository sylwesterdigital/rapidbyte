# bench-lab Scenario Environment Resolution Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `just bench-lab <scenario>` default to the scenario manifest's `environment.ref`, while preserving `env=...` as an explicit override.

**Architecture:** Keep environment resolution in the `Justfile` wrapper. The wrapper will look up the scenario manifest, prefer an explicit override when provided, otherwise use the manifest's declared environment reference, and fall back to `local-bench-postgres` only for legacy scenarios with no `environment.ref`.

**Tech Stack:** `just`, shell, YAML parsing via Python or existing repo tooling, Markdown docs

---

### Task 1: Implement scenario-driven environment resolution in `bench-lab`

**Files:**
- Modify: `Justfile`

**Step 1: Write the failing manual reproduction**

Run:

```bash
just bench-lab pg_dest_copy_release_distributed
```

Expected: FAIL today because `bench-lab` hardcodes `local-bench-postgres`.

**Step 2: Write minimal wrapper logic**

- Update `bench-lab` so it:
  - treats `env` as optional override instead of unconditional default
  - resolves `benchmarks/scenarios/lab/<scenario>.yaml`
  - reads `environment.ref`
  - uses the manifest value when `env` is not provided
  - falls back to `local-bench-postgres` only if the manifest omits the field

**Step 3: Run direct smoke checks**

Run:

```bash
just bench-lab pg_dest_copy_release -- --help
just bench-lab pg_dest_copy_release_distributed -- --help
```

Expected: both commands resolve a valid `--env-profile` without scenario/env
lookup failures.

**Step 4: Commit**

```bash
git add Justfile
git commit -m "fix: resolve bench-lab env from scenario"
```

### Task 2: Update benchmark docs to describe the new default behavior

**Files:**
- Modify: `README.md`
- Modify: `docs/BENCHMARKING.md`
- Modify: `benchmarks/scenarios/README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Update the command docs**

- Replace wording that implies `bench-lab` always uses
  `local-bench-postgres`.
- Document that `bench-lab` uses the scenario's declared environment by
  default and that `env=...` overrides it.
- Add the distributed example without the manual `env=` requirement.

**Step 2: Verify docs references**

Run:

```bash
rg -n "bench-lab|local-bench-postgres|pg_dest_copy_release_distributed" README.md docs/BENCHMARKING.md benchmarks/scenarios/README.md CONTRIBUTING.md
```

Expected: wording is consistent with scenario-driven env resolution.

**Step 3: Commit**

```bash
git add README.md docs/BENCHMARKING.md benchmarks/scenarios/README.md CONTRIBUTING.md
git commit -m "docs: clarify bench-lab environment resolution"
```

### Task 3: Verify end-to-end behavior

**Files:**
- No code changes expected

**Step 1: Run benchmark wrapper verification**

Run:

```bash
just bench-lab pg_dest_copy_release
just bench-lab pg_dest_copy_release_distributed
```

Expected: both commands select the correct environment automatically and start
the benchmark runner without env-profile lookup errors.

**Step 2: Run lint/smoke verification**

Run:

```bash
just --fmt
```

Expected: `Justfile` formatting remains valid.

**Step 3: Commit if needed**

```bash
git add Justfile README.md docs/BENCHMARKING.md benchmarks/scenarios/README.md CONTRIBUTING.md
git commit -m "test: verify bench-lab scenario env defaults"
```
