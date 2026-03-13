# Controller Restart Recovery E2E Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an end-to-end ignored integration test that proves a persisted assigned run becomes `RECONCILING` after controller restart and later reaches `RECOVERY_FAILED` when reconciliation times out.

**Architecture:** Reuse the existing Postgres-backed distributed CLI integration harness. Replace the real executing agent in this scenario with a fake agent client that only registers and polls once so the controller persists a deterministic `ASSIGNED` run before restart. Restart the controller against the same metadata schema with a short reconciliation timeout and assert the public API transitions.

**Tech Stack:** Rust, Tokio, tonic gRPC clients, Postgres metadata store, ignored integration tests

---

### Task 1: Add the failing restart integration test

**Files:**
- Modify: `crates/rapidbyte-cli/tests/distributed.rs`
- Test: `crates/rapidbyte-cli/tests/distributed.rs`

**Step 1: Write the failing test**

Add an ignored test that:
- starts a controller with short reconciliation timing
- submits a run
- registers a fake agent and polls once to persist `ASSIGNED`
- stops the controller
- restarts the controller against the same schema
- asserts `GetRun` reports `RECONCILING`, then `RECOVERY_FAILED`
- asserts `last_error.code == "RECOVERY_TIMEOUT"`

**Step 2: Run test to verify it fails**

Run: `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`

Expected: FAIL because the harness is still missing a deterministic fake-agent restart path or the restart assertions are not yet satisfied.

**Step 3: Write minimal implementation**

Add the smallest helper functions and controller spawn wiring needed for the test to create an assigned lease, restart the controller cleanly, and observe the public states.

**Step 4: Run test to verify it passes**

Run the same ignored test and confirm it passes.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/tests/distributed.rs docs/plans/2026-03-13-controller-restart-recovery-e2e.md
git commit -m "test: cover controller restart recovery e2e"
```

### Task 2: Re-run regression coverage

**Files:**
- Test: `crates/rapidbyte-controller/src/server.rs`
- Test: `crates/rapidbyte-cli/tests/distributed.rs`

**Step 1: Run focused verification**

Run:
- `cargo test -p rapidbyte-controller reconciliation_timeout -- --nocapture`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`

**Step 2: Run full verification**

Run:
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-cli`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`

**Step 3: Commit**

```bash
git add crates/rapidbyte-cli/tests/distributed.rs
git commit -m "test: verify restart recovery timeout path"
```
