# Controller Transactional Metadata Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make submit, assignment, and report-progress durability crash-safe by moving their related run/task metadata writes into store-level transactions and rolling back in-memory state when transactional persistence fails.

**Architecture:** Add narrow transactional methods to the controller metadata store, switch the reviewed service paths to use them, and cover the new semantics with failure-injection tests and Postgres-backed store tests. Keep the existing single-record methods for the rest of the branch.

**Tech Stack:** Rust, tokio, tonic, tokio-postgres, Postgres transactions, cargo test, cargo clippy

---

### Task 1: Add the failing store-level transactional tests

**Files:**
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing tests**

Add metadata-store tests for:

- `create_run_with_task` commits both records
- `assign_task` commits both records
- `mark_task_running` commits both records

Include one injected-failure path that proves the transaction leaves neither row partially updated.

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidbyte-controller metadata_store_transaction -- --nocapture`

Expected: FAIL because the new transactional methods do not exist yet.

**Step 3: Implement the minimal store API and Postgres transaction bodies**

Add focused transactional methods to `DurableMetadataStore` and implement them in `MetadataStore` using a single SQL transaction per method.

**Step 4: Run the tests to verify they pass**

Run: `cargo test -p rapidbyte-controller metadata_store_transaction -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/store/mod.rs
git commit -m "test(controller): add transactional metadata store coverage"
```

### Task 2: Add the failing `report_progress` rollback test

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write the failing test**

Add a failure-injection test that:

- registers an agent
- submits and polls a task
- injects transactional progress persistence failure
- calls `report_progress`
- asserts the RPC returns `INTERNAL`
- asserts task state is restored to `Assigned`
- asserts run state is restored to `Assigned`

**Step 2: Run the test to verify it fails**

Run: `cargo test -p rapidbyte-controller test_report_progress_rolls_back_when_persist_fails -- --nocapture`

Expected: FAIL because `report_progress` currently leaves in-memory state advanced.

**Step 3: Implement the minimal rollback-safe `report_progress` flow**

Use the new transactional store method and restore in-memory task/run snapshots if durable persistence fails.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p rapidbyte-controller test_report_progress_rolls_back_when_persist_fails -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): make progress persistence atomic"
```

### Task 3: Cut submit over to transactional durability

**Files:**
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write or adjust the failing submit test**

Use existing submit rollback coverage and add any extra assertion needed for the new transactional path.

**Step 2: Run the focused test to verify current failure**

Run: `cargo test -p rapidbyte-controller test_submit_pipeline_rolls_back_when_task_persist_fails -- --nocapture`

Expected: FAIL if the transactional conversion changes behavior assumptions, otherwise proceed without broadening test scope.

**Step 3: Implement the transactional submit path**

Replace separate durable run/task writes with `create_run_with_task`. Keep in-memory rollback if the transaction fails.

**Step 4: Run the focused submit tests**

Run: `cargo test -p rapidbyte-controller test_submit_pipeline_ -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): make submit metadata atomic"
```

### Task 4: Cut assignment over to transactional durability

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Use existing assignment rollback test as the red case**

Focus on `test_poll_task_rolls_back_assignment_when_task_persist_fails`.

**Step 2: Run the test**

Run: `cargo test -p rapidbyte-controller test_poll_task_rolls_back_assignment_when_task_persist_fails -- --nocapture`

Expected: either FAIL during conversion or stay green as a guard while the implementation changes.

**Step 3: Implement the transactional assignment path**

Replace separate durable run/task writes with `assign_task`. Keep in-memory rollback if the transaction fails.

**Step 4: Run the focused assignment tests**

Run: `cargo test -p rapidbyte-controller test_poll_task_ -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): make task assignment metadata atomic"
```

### Task 5: Full verification and branch update

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Format**

Run: `cargo fmt --all`

**Step 2: Lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS with zero warnings

**Step 3: Run controller tests**

Run: `cargo test -p rapidbyte-controller`

Expected: PASS

**Step 4: Run CLI tests**

Run: `cargo test -p rapidbyte-cli`

Expected: PASS

**Step 5: Run ignored Postgres-backed tests**

Run:

- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

Expected: PASS

**Step 6: Commit and push**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): make metadata updates transactional"
git push
```
