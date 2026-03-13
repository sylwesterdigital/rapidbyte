# Persistent Controller State Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make distributed controller metadata durable in Postgres so controller restart preserves run/task state and supports conservative reconciliation of in-flight work.

**Architecture:** Introduce a `ControllerStore` abstraction in `rapidbyte-controller`, keep lifecycle and lease rules in Rust, and add a Postgres-backed implementation that persists runs, tasks, agents, previews, and recovery metadata transactionally. Startup recovery reloads non-terminal work into a reconciliation flow while watchers and long-poll notifications remain in-memory transport concerns.

**Tech Stack:** Rust, Tokio, tonic, PostgreSQL, SQL migrations, clap, existing controller unit tests, controller integration tests.

---

### Task 1: Add controller metadata store configuration and bootstrap

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Create: `crates/rapidbyte-controller/src/store/mod.rs`
- Create: `crates/rapidbyte-controller/src/store/migrations/0001_controller_metadata.sql`
- Test: `crates/rapidbyte-controller/src/server.rs`
- Test: `crates/rapidbyte-cli/src/commands/controller.rs`

**Step 1: Write the failing tests**

Add tests that prove:

- controller CLI/config accepts a required metadata Postgres connection string for distributed controller mode
- controller server bootstrap fails closed when the metadata store is configured incorrectly
- migration bootstrap loads SQL from the checked-in migration file instead of ad hoc inline DDL

Example test shape:

```rust
#[test]
fn controller_execute_requires_metadata_database_url() {
    let err = build_config(
        "[::]:9090",
        Some("signing"),
        Some("secret"),
        false,
        false,
        None,
        None,
        None,
    )
    .unwrap_err();

    assert!(err
        .to_string()
        .contains("controller requires --metadata-database-url"));
}

#[tokio::test]
async fn metadata_store_init_rejects_missing_connection_string() {
    let config = ControllerConfig {
        auth_tokens: vec!["secret".into()],
        signing_key: b"signing".to_vec(),
        metadata_database_url: Some(String::new()),
        ..Default::default()
    };

    let err = init_store(&config).await.unwrap_err();
    assert!(err.to_string().contains("metadata database URL"));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli controller_execute_requires_metadata_database_url
cargo test -p rapidbyte-controller metadata_store_init_rejects_missing_connection_string
```

Expected:
- FAIL because controller config does not yet expose or validate metadata database settings

**Step 3: Write minimal implementation**

Implement:

- `--metadata-database-url` / `RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL` CLI wiring
- `ControllerConfig` fields for metadata store settings
- `store` module skeleton and migration loader
- server bootstrap that initializes the configured store before starting gRPC services

Do not switch request handlers to the new store yet.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-cli controller_execute_requires_metadata_database_url
cargo test -p rapidbyte-controller metadata_store_init_rejects_missing_connection_string
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add Cargo.toml crates/rapidbyte-controller/Cargo.toml crates/rapidbyte-controller/src/lib.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/store/migrations/0001_controller_metadata.sql crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/controller.rs
git commit -m "feat(controller): add metadata store bootstrap"
```

### Task 2: Introduce a store abstraction with parity-preserving in-memory implementation

**Files:**
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Create: `crates/rapidbyte-controller/src/store/in_memory.rs`
- Create: `crates/rapidbyte-controller/src/store/types.rs`
- Test: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Test: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write the failing tests**

Add focused service tests that construct `ControllerState` through the new store abstraction and prove the existing submit/poll/complete flow still works with the in-memory store.

Example test shape:

```rust
#[tokio::test]
async fn submit_pipeline_uses_store_backed_state() {
    let state = ControllerState::for_tests();
    let svc = PipelineServiceImpl::new(state.clone());

    let run_id = submit_test_pipeline(&svc).await;

    let run = state.store().get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.pipeline_name, "test");
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller submit_pipeline_uses_store_backed_state
cargo test -p rapidbyte-controller test_poll_task_returns_pending_task
```

Expected:
- FAIL because no store abstraction exists and service code still reaches directly into in-memory locks

**Step 3: Write minimal implementation**

Implement:

- `ControllerStore` trait and shared store-facing types
- in-memory store adapter built from the existing run/task/registry/preview logic
- `ControllerState` refactor so services depend on the store interface plus in-memory watcher/notify infrastructure

Keep behavior unchanged.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller submit_pipeline_uses_store_backed_state
cargo test -p rapidbyte-controller test_poll_task_returns_pending_task
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/in_memory.rs crates/rapidbyte-controller/src/store/types.rs
git commit -m "refactor(controller): route services through controller store"
```

### Task 3: Implement the Postgres-backed controller store and recovery schema

**Files:**
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`
- Create: `crates/rapidbyte-controller/src/store/postgres.rs`
- Test: `crates/rapidbyte-controller/src/store/postgres.rs`

**Step 1: Write the failing tests**

Add store-level tests for:

- creating a run and initial task in one transaction
- claiming the next pending task and persisting lease metadata
- reloading non-terminal tasks after restart and mapping assigned/running work to `reconciling`

Use a real Postgres-backed test seam if available; otherwise use ignored integration tests that run when a controller metadata database URL is provided.

Example test shape:

```rust
#[tokio::test]
async fn postgres_store_reloads_inflight_tasks_as_reconciling() {
    let store = test_postgres_store().await;
    let run_id = store.create_run_and_task(new_submit()).await.unwrap();
    let claim = store.claim_next_task("agent-1", lease_ttl()).await.unwrap().unwrap();

    drop(store);

    let reopened = test_postgres_store().await;
    let recovered = reopened.load_recovery_snapshot().await.unwrap();
    assert!(recovered.tasks.iter().any(|task| {
        task.task_id == claim.task_id && task.state == PersistedTaskState::Reconciling
    }));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller postgres_store_reloads_inflight_tasks_as_reconciling -- --ignored
```

Expected:
- FAIL because the Postgres store and recovery logic do not exist yet

**Step 3: Write minimal implementation**

Implement:

- async Postgres store with checked-in migrations
- transactional submit/claim/update/complete/cancel methods
- durable run/task/agent/preview/history schema
- startup recovery loader that normalizes in-flight assignments into reconciliation records

Keep SQL centralized in `store/postgres.rs`.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller postgres_store_reloads_inflight_tasks_as_reconciling -- --ignored
```

Expected:
- PASS when the Postgres test environment is available

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/Cargo.toml crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/store/postgres.rs
git commit -m "feat(controller): persist metadata in postgres"
```

### Task 4: Switch controller RPC flows to durable operations and recovery-aware semantics

**Files:**
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/run_state.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/registry.rs`
- Test: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Test: `crates/rapidbyte-controller/src/agent_service.rs`
- Test: `crates/rapidbyte-controller/src/server.rs`

**Step 1: Write the failing tests**

Add tests that prove:

- controller restart preserves submitted runs and pending tasks
- progress/completion after restart is accepted only for matching persisted `task_id` + `lease_epoch`
- stale or foreign-agent reports are rejected after restart
- reconciliation timeout transitions unrecoverable work into a recovery-specific terminal failure

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller restart_preserves_submitted_run
cargo test -p rapidbyte-controller progress_after_restart_requires_matching_lease_epoch
```

Expected:
- FAIL because handlers still depend on process-local memory and have no recovery path

**Step 3: Write minimal implementation**

Implement:

- durable submit/list/get/status/cancel/poll/progress/complete flows
- explicit recovery/reconciling states where needed
- post-restart reattachment and bounded reconciliation timeout handling
- strict fencing against stale completions and heartbeats across restart

Keep watcher publication behavior unchanged except for sourcing terminal/current state from the store.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller restart_preserves_submitted_run
cargo test -p rapidbyte-controller progress_after_restart_requires_matching_lease_epoch
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/run_state.rs crates/rapidbyte-controller/src/scheduler.rs crates/rapidbyte-controller/src/registry.rs
git commit -m "feat(controller): recover durable runs across restart"
```

### Task 5: Add end-to-end verification and document the operational contract

**Files:**
- Modify: `crates/rapidbyte-cli/tests/distributed.rs`
- Modify: `docs/ARCHITECTUREv2.md`
- Modify: `docs/plans/2026-03-13-persistent-controller-state-design.md`
- Test: `crates/rapidbyte-cli/tests/distributed.rs`

**Step 1: Write the failing test**

Add an integration-style test that exercises controller config or restart behavior end-to-end enough to prove the metadata database setting is required and surfaced clearly. If full restart orchestration is too heavy for a normal unit test, add a targeted CLI/config regression test plus an ignored restart integration test.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli --test distributed
```

Expected:
- FAIL until CLI and controller behavior/documentation match the new durable metadata contract

**Step 3: Write minimal implementation**

Update docs and tests so the operational contract is explicit:

- distributed controller requires a metadata Postgres database
- restart recovery is conservative and may require operator action when retry safety is unknown
- status/watch/list-runs surface recovery-specific failures clearly

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-cli --test distributed
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/tests/distributed.rs docs/ARCHITECTUREv2.md docs/plans/2026-03-13-persistent-controller-state-design.md
git commit -m "docs: document durable controller restart recovery"
```
