# Controller Restart Resume E2E Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an end-to-end ignored integration test that proves a persisted assigned run resumes normal execution after controller restart instead of timing out recovery.

**Architecture:** Reuse the Postgres-backed distributed CLI integration harness and the deterministic fake-agent client pattern. Persist an `ASSIGNED` lease before restart, restart the controller against the same metadata schema, wait for `RECONCILING`, then use `ReportProgress` and `CompleteTask` RPCs from the restored assignment to prove the controller accepts the resumed task and transitions back into ordinary execution.

**Tech Stack:** Rust, Tokio, tonic gRPC clients, Postgres metadata store, ignored integration tests

---

### Task 1: Add the failing restart-resume integration test

**Files:**
- Modify: `crates/rapidbyte-cli/tests/distributed.rs`
- Test: `crates/rapidbyte-cli/tests/distributed.rs`

**Step 1: Write the failing test**

Add an ignored test that:
- starts a controller
- submits a run
- registers a fake agent and polls once to persist `ASSIGNED`
- stops and restarts the controller against the same metadata schema
- waits for `RECONCILING`
- sends `ReportProgress` with the persisted `task_id` and `lease_epoch`
- asserts `GetRun` reaches `RUNNING`
- sends `CompleteTask` with a deterministic non-retryable execution error
- asserts final state is `FAILED`, not `RECOVERY_FAILED`
- asserts `last_error.code` matches the injected execution error

**Step 2: Run test to verify it fails**

Run: `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

Expected: FAIL if resumed progress/completion is rejected after restart or if the harness is missing helper wiring.

**Step 3: Write minimal implementation**

Add the smallest helper functions or assertions needed for the fake agent to resume the assignment through controller RPCs. Do not change controller behavior unless the test proves a real recovery bug.

**Step 4: Run test to verify it passes**

Run the same ignored test and confirm it passes.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/tests/distributed.rs docs/plans/2026-03-13-controller-restart-resume-e2e.md
git commit -m "test: cover controller restart resume e2e"
```

### Task 2: Re-run regression coverage

**Files:**
- Test: `crates/rapidbyte-cli/tests/distributed.rs`
- Test: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Run focused verification**

Run:
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

**Step 2: Run full verification**

Run:
- `cargo test -p rapidbyte-cli`
- `cargo test -p rapidbyte-controller`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`

**Step 3: Commit**

```bash
git add crates/rapidbyte-cli/tests/distributed.rs
git commit -m "test: verify restart resume recovery path"
```
