# Controller Cancel Atomicity And Metadata TLS Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make queued cancellation durability-safe and allow secure metadata Postgres transport using standard Postgres URL TLS parameters.

**Architecture:** Reuse the branch’s existing snapshot-and-rollback pattern for queued cancel and rejected assignment paths so in-memory state is never accepted ahead of durable writes. Replace the hardcoded `NoTls` metadata connection bootstrap with Postgres-config-driven connection setup that supports both plain and TLS transport without new controller-specific DB TLS flags.

**Tech Stack:** Rust, Tokio, tonic, tokio-postgres, rustls, tokio-rustls, rustls-native-certs, clap, controller unit tests, existing ignored Postgres-backed integration tests.

---

### Task 1: Add failing durability tests for queued cancel

**Files:**
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`

**Step 1: Write the failing tests**

Add controller tests that prove:

- queued cancel rolls back both run and task when task persistence fails
- queued cancel rolls back both run and task when run persistence fails after task persistence

Use the existing `FailingMetadataStore` seam. Add any extra failure knobs needed for task-then-run cancel ordering.

Example test shape:

```rust
#[tokio::test]
async fn test_cancel_queued_run_rolls_back_when_task_persist_fails() {
    let store = FailingMetadataStore::new().fail_task_upsert_on(2);
    let state = ControllerState::with_metadata_store(b"test-signing-key", store);
    let svc = PipelineServiceImpl::new(state.clone());
    let run_id = submit_pipeline(&svc).await;

    let err = svc.cancel_run(Request::new(CancelRunRequest { run_id: run_id.clone() }))
        .await
        .expect_err("cancel should fail when task persistence fails");

    assert_eq!(err.code(), tonic::Code::Internal);
    assert_eq!(state.runs.read().await.get_run(&run_id).unwrap().state, InternalRunState::Pending);
    assert_eq!(state.tasks.read().await.find_by_run_id(&run_id).unwrap().state, TaskState::Pending);
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller test_cancel_queued_run_rolls_back_when_task_persist_fails -- --nocapture
cargo test -p rapidbyte-controller test_cancel_queued_run_rolls_back_when_run_persist_fails -- --nocapture
```

Expected:
- FAIL because queued cancel currently persists run before task and has no rollback

**Step 3: Write minimal implementation**

Implement queued-cancel snapshot capture and rollback support:

- snapshot the pre-cancel run and task
- persist the cancelled task before the cancelled run
- restore durable and in-memory snapshots if either write fails
- publish cancellation only after durability succeeds

Add the smallest metadata-store helpers needed to persist exact task snapshots and any best-effort rollback.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller test_cancel_queued_run_rolls_back_when_task_persist_fails -- --nocapture
cargo test -p rapidbyte-controller test_cancel_queued_run_rolls_back_when_run_persist_fails -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/state.rs
git commit -m "fix(controller): make queued cancel durable"
```

### Task 2: Close the rejected-assignment durability gap

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing test**

Add a focused test proving that when `poll_task` claims a task but run transition rejects the assignment, the rejected task state is durably persisted before returning `None`.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller poll_task_persists_rejected_assignment -- --nocapture
```

Expected:
- FAIL because the rejection branch currently mutates only in memory

**Step 3: Write minimal implementation**

Persist the rejected task state after `reject_assignment` and before returning `Ok(None)`. Keep behavior otherwise unchanged.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-controller poll_task_persists_rejected_assignment -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): persist rejected assignments"
```

### Task 3: Add TLS-capable metadata Postgres bootstrap

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`

**Step 1: Write the failing tests**

Add tests that prove:

- metadata bootstrap accepts URL strings with TLS parameters
- bootstrap path is no longer tied to `NoTls`
- CLI/config validation still only requires a non-empty metadata DB URL

Prefer unit tests around config parsing/connector selection rather than requiring a live TLS Postgres server.

Example test shape:

```rust
#[test]
fn metadata_store_config_parses_sslmode_require() {
    let config = parse_metadata_db_config(
        "postgres://user:pass@db.example.com/app?sslmode=require"
    ).unwrap();

    assert_eq!(config.get_ssl_mode(), tokio_postgres::config::SslMode::Require);
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller metadata_store_config_parses_sslmode_require -- --nocapture
```

Expected:
- FAIL because metadata bootstrap currently hardcodes `NoTls`

**Step 3: Write minimal implementation**

Implement:

- parsed `tokio_postgres::Config` bootstrap from the URL string
- a connector path that supports both non-TLS and TLS-required URLs
- fail-closed error propagation when TLS is required but connection setup fails

Do not add new CLI flags; rely on the Postgres URL.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller metadata_store_config_parses_sslmode_require -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/rapidbyte-controller/Cargo.toml crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-cli/src/commands/controller.rs
git commit -m "feat(controller): support tls metadata postgres urls"
```

### Task 4: Run full verification and push the branch update

**Files:**
- Modify: none expected unless verification reveals defects

**Step 1: Run formatting and lint**

Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- PASS

**Step 2: Run the fast suites**

Run:

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-cli
```

Expected:
- PASS

**Step 3: Run the Postgres-backed ignored tests**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored
```

Expected:
- PASS

**Step 4: Commit any verification-only follow-ups**

```bash
git add -A
git commit -m "chore: finalize queued cancel and metadata tls fixes"
```

Only do this if verification required code changes after the prior commits.

**Step 5: Push**

```bash
git push
```
