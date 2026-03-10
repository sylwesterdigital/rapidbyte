# Pre-commit Quality Hooks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a lightweight repo-managed pre-commit hook that auto-formats code, restages changes, and keeps heavy validation in `just ci` / CI.

**Architecture:** Store hooks in `.githooks/`, install them through a small repo script, and expose the same fast auto-fix path through `just fix`. The hook runs `cargo fmt --all`, re-stages modified tracked files, and aborts if it had to change anything so the user can review and recommit.

**Tech Stack:** Git hooks, POSIX shell, `just`, Rust toolchain (`cargo fmt`)

---

### Task 1: Add the repo-managed hook and installer

**Files:**
- Create: `.githooks/pre-commit`
- Create: `scripts/install-git-hooks.sh`
- Modify: `Justfile`

**Step 1: Write the failing shell validation expectation**

Document the exact commands that must succeed once files exist:

```bash
bash -n .githooks/pre-commit
bash -n scripts/install-git-hooks.sh
just --show install-hooks
```

**Step 2: Create the hook**

Implement `.githooks/pre-commit` to:

- detect staged files with `git diff --cached --name-only --diff-filter=ACMR`
- return early if nothing relevant is staged
- run `cargo fmt --all`
- re-stage tracked modified files with `git add -u`
- fail with a message if formatting changed files

**Step 3: Create the installer**

Implement `scripts/install-git-hooks.sh` to run:

```bash
git config core.hooksPath .githooks
```

and print a short confirmation.

**Step 4: Add `Justfile` entry points**

Add:

- `install-hooks`
- `fix`

`fix` should run `cargo fmt --all`.

**Step 5: Verify the files**

Run:

```bash
bash -n .githooks/pre-commit
bash -n scripts/install-git-hooks.sh
just --show install-hooks
just --show fix
```

Expected: all commands succeed and the recipes render correctly.

**Step 6: Commit**

```bash
git add .githooks/pre-commit scripts/install-git-hooks.sh Justfile
git commit -m "feat: add lightweight pre-commit hooks"
```

### Task 2: Document contributor workflow

**Files:**
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the documentation diff**

Add a short setup path:

- run `just install-hooks` after clone
- use `just fix` for manual formatting
- hooks are auto-fix only
- `just ci` remains the pre-PR gate
- no Python `pre-commit` package is required

**Step 2: Update `README.md`**

Add `just install-hooks` and `just fix` to the development command list and
mention the one-time hook install in setup/development guidance.

**Step 3: Update `CONTRIBUTING.md`**

Add the hook-install step to setup or contribution workflow and clarify the
division between local auto-fix hooks and strict CI validation.

**Step 4: Verify docs**

Run:

```bash
rg -n "install-hooks|just fix|pre-commit|core.hooksPath|just ci" README.md CONTRIBUTING.md
```

Expected: the new contributor workflow is documented consistently.

**Step 5: Commit**

```bash
git add README.md CONTRIBUTING.md
git commit -m "docs: document pre-commit hook workflow"
```

### Task 3: Verify the installed-flow behavior

**Files:**
- Reuse: `.githooks/pre-commit`
- Reuse: `scripts/install-git-hooks.sh`
- Reuse: `Justfile`

**Step 1: Install hooks in the worktree**

Run:

```bash
just install-hooks
git config --get core.hooksPath
```

Expected: output is `.githooks`.

**Step 2: Verify manual fast-fix path**

Run:

```bash
just fix
```

Expected: formatter runs successfully.

**Step 3: Re-run baseline verification**

Run:

```bash
cargo test -p rapidbyte-benchmarks
git status --short
```

Expected: benchmark crate tests pass; only intentional changes remain.

**Step 4: Commit**

```bash
git add .githooks/pre-commit scripts/install-git-hooks.sh Justfile README.md CONTRIBUTING.md
git commit -m "chore: verify pre-commit quality workflow"
```

### Task 4: Final verification and handoff

**Files:**
- Review: `.githooks/pre-commit`
- Review: `scripts/install-git-hooks.sh`
- Review: `Justfile`
- Review: `README.md`
- Review: `CONTRIBUTING.md`

**Step 1: Run final verification**

Run:

```bash
bash -n .githooks/pre-commit
bash -n scripts/install-git-hooks.sh
just --show install-hooks
just --show fix
just install-hooks
git config --get core.hooksPath
just fix
cargo test -p rapidbyte-benchmarks
git status --short
```

Expected:

- shell scripts are valid
- `install-hooks` and `fix` recipes exist
- Git hooks path is `.githooks`
- formatter succeeds
- benchmark crate tests pass
- worktree is clean after commit

**Step 2: Request code review**

Use `superpowers:requesting-code-review` before merging.

**Step 3: Choose integration path**

Use `superpowers:finishing-a-development-branch` after verification and review.
