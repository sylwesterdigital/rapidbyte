# Distributed CLI Flight Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix remaining distributed CLI/controller/agent correctness gaps around terminal watch handling, dry-run preview failures, controller auth wiring, Flight startup, and empty preview metadata.

**Architecture:** Tighten success semantics at the CLI boundary, wire the existing global bearer token into controller startup, fail the agent before registration when Flight bind fails, and make Flight metadata generation tolerant of empty preview streams.

**Tech Stack:** Rust, Tokio, tonic gRPC, Arrow Flight, clap-based CLI, existing in-process test suites.

---

### Task 1: Document The Fix Set

**Files:**
- Create: `docs/plans/2026-03-12-distributed-cli-flight-hardening-design.md`
- Create: `docs/plans/2026-03-12-distributed-cli-flight-hardening-plan.md`

**Step 1: Write the approved design**

Capture the CLI terminal semantics, controller auth wiring, agent startup failure, and empty preview metadata behavior.

**Step 2: Save the implementation plan**

Record the TDD task list in this file.

**Step 3: Commit**

```bash
git add docs/plans/2026-03-12-distributed-cli-flight-hardening-design.md docs/plans/2026-03-12-distributed-cli-flight-hardening-plan.md
git commit -m "docs: plan distributed cli flight hardening"
```

### Task 2: Lock Down CLI Success Semantics

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`

**Step 1: Write the failing tests**

Add tests for:

- `WatchRun` EOF before a terminal event returns an error
- distributed dry-run preview fetch failure returns an error

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli commands::distributed_run::tests::watch_run_eof_before_terminal_is_error -- --exact
cargo test -p rapidbyte-cli commands::distributed_run::tests::dry_run_preview_failure_is_error -- --exact
```

Expected: failures showing the CLI still treats those cases as success.

**Step 3: Write the minimal implementation**

- Return an error if the watch loop exits without a terminal event.
- Treat preview fetch as required for distributed dry-runs and limits.

**Step 4: Run the targeted tests to verify they pass**

Run the same commands and expect both tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/distributed_run.rs
git commit -m "fix(cli): harden distributed terminal handling"
```

### Task 3: Wire Controller Auth Tokens Through The CLI

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Write the failing test**

Add a unit test that proves the controller command maps the provided auth token into `ControllerConfig.auth_tokens`.

**Step 2: Run the targeted test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-cli commands::controller::tests::controller_execute_uses_auth_token -- --exact
```

Expected: failure showing the config still leaves auth disabled.

**Step 3: Write the minimal implementation**

Thread the global auth token through the controller command and set `auth_tokens` when present.

**Step 4: Run the targeted test to verify it passes**

Run the same command and expect it to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/controller.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat(cli): enable controller auth token wiring"
```

### Task 4: Fail Fast On Broken Flight Startup And Empty Preview Metadata

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Write the failing tests**

Add tests for:

- agent startup fails when the Flight port is already bound
- `get_flight_info` succeeds for an empty preview stream

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent worker::tests::run_fails_when_flight_listener_is_unavailable -- --exact
cargo test -p rapidbyte-agent flight::tests::get_flight_info_supports_empty_preview_stream -- --exact
```

Expected: the startup test shows `run()` still succeeds, and the Flight test panics or errors on `batches[0]`.

**Step 3: Write the minimal implementation**

- Bind the Flight listener up front and propagate bind failure from `run()`.
- Make `get_flight_info` omit schema encoding when a preview stream has no batches.

**Step 4: Run the targeted tests to verify they pass**

Run the same commands and expect both tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs crates/rapidbyte-agent/src/flight.rs
git commit -m "fix(agent): harden flight startup and empty previews"
```

### Task 5: Full Verification

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Run package tests**

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-agent
cargo test -p rapidbyte-cli
```

**Step 2: Inspect git status**

```bash
git status --short
```

**Step 3: Final commit**

```bash
git add <touched files>
git commit -m "fix distributed cli preview and flight edge cases"
```
