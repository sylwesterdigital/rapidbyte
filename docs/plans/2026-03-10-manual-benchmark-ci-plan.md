# Manual Benchmark CI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stop running benchmark PR CI automatically while keeping the benchmark
workflow available for manual maintainer use and preserving `just bench-pr` as
the main local performance-regression path.

**Architecture:** Restrict `.github/workflows/bench-pr.yml` to
`workflow_dispatch`, keep the existing benchmark command path intact, and align
repo messaging so badges and contributor docs no longer imply automatic PR
benchmark coverage. This is a CI policy/documentation change, not a benchmark
engine change.

**Tech Stack:** GitHub Actions YAML, Markdown docs, repository workflow and
command documentation.

---

### Task 1: Make the benchmark workflow manual-only

**Files:**
- Modify: `.github/workflows/bench-pr.yml`

**Step 1: Write the failing workflow expectation**

Add or update a local check expectation that proves:
- `bench-pr.yml` no longer contains a `pull_request` trigger
- `workflow_dispatch` remains present

**Step 2: Run a readback check to verify current behavior fails**

Run:

```bash
sed -n '1,120p' .github/workflows/bench-pr.yml
```

Expected:
- FAIL against the desired policy because `pull_request` is still present

**Step 3: Write the minimal implementation**

Edit `.github/workflows/bench-pr.yml` so:
- `on:` contains only `workflow_dispatch`
- the benchmark job body remains intact for manual runs

**Step 4: Re-read the workflow**

Run:

```bash
sed -n '1,120p' .github/workflows/bench-pr.yml
```

Expected:
- `workflow_dispatch` present
- `pull_request` absent

**Step 5: Commit**

```bash
git add .github/workflows/bench-pr.yml
git commit -m "ci(bench): make benchmark workflow manual only"
```

### Task 2: Align repo docs and badges with the manual-only policy

**Files:**
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`
- Modify: `benchmarks/scenarios/pr/README.md`

**Step 1: Write the failing doc expectation**

Identify and update text that currently implies benchmark PR checks are normal
CI or part of routine PR automation.

**Step 2: Re-read the current docs to confirm misalignment**

Run:

```bash
sed -n '1,20p' README.md
sed -n '180,240p' README.md
sed -n '70,110p' CONTRIBUTING.md
sed -n '1,80p' benchmarks/scenarios/pr/README.md
```

Expected:
- benchmark badge still visible in `README.md`
- docs still suggest benchmark PR behavior is normal CI or routine PR flow

**Step 3: Write the minimal implementation**

Update docs so they say:
- benchmark CI is manual-only for now
- `just bench-pr` is the local path for perf investigation/regression checks
- the PR benchmark workflow remains available via manual dispatch if needed
- the benchmark badge is removed from `README.md`

**Step 4: Re-read the changed docs**

Run:

```bash
sed -n '1,20p' README.md
sed -n '180,240p' README.md
sed -n '70,110p' CONTRIBUTING.md
sed -n '1,80p' benchmarks/scenarios/pr/README.md
```

Expected:
- no benchmark badge in `README.md`
- wording consistently describes local/manual-only benchmark policy

**Step 5: Commit**

```bash
git add README.md CONTRIBUTING.md benchmarks/scenarios/pr/README.md
git commit -m "docs(bench): document manual benchmark workflow"
```

### Task 3: Verify the manual-only policy end to end

**Files:**
- Verify in-place against:
  - `.github/workflows/bench-pr.yml`
  - `README.md`
  - `CONTRIBUTING.md`
  - `benchmarks/scenarios/pr/README.md`

**Step 1: Check for malformed diffs**

Run:

```bash
git diff --check
```

Expected: no output

**Step 2: Re-read the workflow and docs**

Run:

```bash
sed -n '1,120p' .github/workflows/bench-pr.yml
sed -n '1,20p' README.md
sed -n '70,110p' CONTRIBUTING.md
sed -n '1,80p' benchmarks/scenarios/pr/README.md
```

Expected:
- workflow is manual-only
- docs and repo signals match that policy

**Step 3: Optional YAML sanity check**

Run:

```bash
python3 -c 'import yaml, pathlib; yaml.safe_load(pathlib.Path(".github/workflows/bench-pr.yml").read_text()); print("yaml-ok")'
```

Expected:
- `yaml-ok`

**Step 4: Capture final branch state**

Run:

```bash
git status --short
git log --oneline -n 5
```

Expected:
- only the intended workflow/doc files changed for this task
- recent commits clearly reflect the CI policy update

**Step 5: Commit**

If verification required cleanup:

```bash
git add .github/workflows/bench-pr.yml README.md CONTRIBUTING.md benchmarks/scenarios/pr/README.md
git commit -m "chore(bench): finalize manual benchmark CI policy"
```
