# Pre-commit Quality Hooks Design

## Goal

Add a lightweight local pre-commit workflow that automatically prepares code for
commit without turning commits into a slow CI gate. The hook should fix cheap,
deterministic issues up front, restage changed files, and ask the developer to
review and recommit.

## Decision

Use a repo-managed Git hook, not the Python `pre-commit` framework.

This repo already has a Rust-first toolchain and a `Justfile` with canonical
commands. Adding `.githooks/` plus a small installer script keeps the workflow
auditable, dependency-free, and aligned with existing commands. Nothing needs
to be installed system-wide beyond Git and the project toolchain.

## Scope

The local pre-commit hook will:

- run fast auto-fix steps only
- re-stage files changed by those auto-fix steps
- abort the current commit if it changed files, so the user can inspect the
  diff and re-run `git commit`

The hook will not:

- run `clippy`
- run tests
- run benchmarks
- require Docker
- require network access

Strict validation remains in `just ci` and GitHub Actions.

## Hook Behavior

The pre-commit hook should:

1. detect whether staged files include Rust or Rust-adjacent files worth
   formatting
2. run `cargo fmt --all`
3. re-stage tracked files changed by formatting
4. exit non-zero if formatting changed files, with a short message telling the
   user formatting was applied and the commit should be retried
5. otherwise exit zero and allow the commit

This keeps behavior predictable. The hook can help, but it should not silently
mutate commit contents and proceed.

## Entry Points

Add these repo-owned entry points:

- `.githooks/pre-commit`
- `scripts/install-git-hooks.sh`
- `just install-hooks`
- `just fix`

`just fix` should mirror the fast auto-fix path and act as the documented manual
equivalent of the hook.

## Configuration Model

The installer should configure:

```bash
git config core.hooksPath .githooks
```

That keeps hooks versioned in-repo and avoids editing `.git/hooks` directly.

## UX Principles

- fast enough to run on every commit
- deterministic
- no hidden dependencies
- no heavy checks in the commit path
- clear message when it modifies files

## Documentation Changes

Update `README.md` and `CONTRIBUTING.md` to explain:

- install hooks once after clone with `just install-hooks`
- hooks are auto-fix only
- `just ci` is still required before PR
- no Python `pre-commit` package is required for this workflow

## Testing Strategy

Verification should cover:

- installer sets `core.hooksPath` correctly
- hook script is shell-valid
- `just install-hooks` and `just fix` resolve correctly
- docs point contributors to the correct setup

The hook itself should be validated with a controlled staged-file scenario if a
cheap script-based test can be added without overbuilding the solution.
