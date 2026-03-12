# Distributed Agent Preview Runtime Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix distributed completion retries, multi-stream preview serving, actual `max_tasks` concurrency, and standard Flight metadata support.

**Architecture:** Keep one registered agent with shared runtime state and multiple worker loops, and switch preview access from one merged run-level ticket to stream-scoped tickets that each map to exactly one Arrow schema. Update the controller and CLI together so preview control-plane metadata and Flight data-plane behavior stay aligned.

**Tech Stack:** Rust, Tokio, tonic, Arrow Flight, protobuf, Cargo tests

---

### Task 1: Extend Preview Metadata for Stream-Scoped Tickets

**Files:**
- Modify: `proto/rapidbyte/v1/controller.proto`
- Modify: `crates/rapidbyte-controller/src/preview.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-agent/src/ticket.rs`
- Test: `crates/rapidbyte-controller/src/preview.rs`
- Test: `crates/rapidbyte-agent/src/ticket.rs`

**Step 1: Write the failing tests**

Add coverage for a ticket payload that includes `stream_name`, and for preview metadata storing multiple stream entries instead of only a top-level ticket.

**Step 2: Run the focused tests to verify RED**

Run:
- `cargo test -p rapidbyte-controller preview`
- `cargo test -p rapidbyte-agent ticket`

Expected: fail because stream-scoped preview payloads and metadata do not exist yet.

**Step 3: Write the minimal implementation**

- Add stream-scoped preview metadata to the proto and regenerate code through the normal Cargo build.
- Extend controller-side preview ticket payload/signing to include `stream_name`.
- Extend agent-side ticket verification to parse the new payload shape.

**Step 4: Run focused verification**

Run:
- `cargo test -p rapidbyte-controller preview`
- `cargo test -p rapidbyte-agent ticket`

Expected: pass.

**Step 5: Commit**

```bash
git add proto/rapidbyte/v1/controller.proto crates/rapidbyte-controller/src/preview.rs crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-agent/src/ticket.rs
git commit -m "feat: add stream-scoped preview tickets"
```

### Task 2: Make Flight `DoGet` and `GetFlightInfo` Stream-Scoped

**Files:**
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`
- Test: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Write the failing tests**

Add tests that:

- store a dry-run result with at least two streams using different schemas,
- verify `do_get` for one stream returns only that stream’s schema and batches,
- verify `get_flight_info` includes schema, endpoint metadata, and a usable ticket for that same stream.

**Step 2: Run the focused tests to verify RED**

Run: `cargo test -p rapidbyte-agent flight`

Expected: fail because the current Flight service flattens all batches and omits discovery metadata.

**Step 3: Write the minimal implementation**

- Add helpers to look up one dry-run stream by verified ticket payload.
- Make `do_get` emit one schema and that stream’s batches only.
- Make `get_flight_info` return schema, totals, endpoint, and ticket for the stream-scoped descriptor or ticket flow.

**Step 4: Run focused verification**

Run: `cargo test -p rapidbyte-agent flight`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/flight.rs crates/rapidbyte-agent/src/spool.rs
git commit -m "fix(agent): serve preview streams with correct flight metadata"
```

### Task 3: Keep Leases Alive Until `CompleteTask` Reaches a Terminal Controller Response

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Test: `crates/rapidbyte-agent/src/worker.rs`

**Step 1: Write the failing test**

Add a worker test that simulates:

- task execution completes,
- first `CompleteTask` attempt fails at the RPC layer,
- heartbeats still observe the task lease as active,
- a later `CompleteTask` attempt succeeds,
- only then is the lease removed.

**Step 2: Run the focused test to verify RED**

Run: `cargo test -p rapidbyte-agent worker::tests::complete_task_transport_failure_keeps_lease_active -- --exact`

Expected: fail because the current worker removes the lease immediately after the first RPC return.

**Step 3: Write the minimal implementation**

- Extract completion reporting into a retry helper with capped backoff.
- Remove the lease only on `acknowledged: true` or `acknowledged: false`.
- Keep logging explicit for retry, stale completion, and eventual success.

**Step 4: Run focused verification**

Run: `cargo test -p rapidbyte-agent worker::tests::complete_task_transport_failure_keeps_lease_active -- --exact`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs
git commit -m "fix(agent): retain leases until completion is acknowledged"
```

### Task 4: Honor `max_tasks` With Real Concurrent Worker Loops

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Test: `crates/rapidbyte-agent/src/worker.rs`
- Reference: `crates/rapidbyte-controller/src/registry.rs`

**Step 1: Write the failing test**

Add a concurrency test where `max_tasks = 2` and two tasks are made available; prove both tasks begin execution before the first one finishes.

**Step 2: Run the focused test to verify RED**

Run: `cargo test -p rapidbyte-agent worker::tests::max_tasks_allows_parallel_execution -- --exact`

Expected: fail because the current loop executes tasks serially.

**Step 3: Write the minimal implementation**

- Refactor `run` into coordinator + shared runtime state.
- Spawn `max_tasks` independent poll/execute/report loops under one registered `agent_id`.
- Keep heartbeat based on the shared `active_leases` map.

**Step 4: Run focused verification**

Run: `cargo test -p rapidbyte-agent worker::tests::max_tasks_allows_parallel_execution -- --exact`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs
git commit -m "feat(agent): execute up to max_tasks concurrently"
```

### Task 5: Update CLI Preview Retrieval for Stream-Scoped Access

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Test: `crates/rapidbyte-cli/tests/distributed.rs`

**Step 1: Write the failing test**

Add a distributed preview test that returns multiple preview streams and verifies the CLI fetches each stream separately rather than one merged `DoGet`.

**Step 2: Run the focused test to verify RED**

Run: `cargo test -p rapidbyte-cli distributed`

Expected: fail because the current CLI only uses the top-level preview ticket and decodes one merged stream.

**Step 3: Write the minimal implementation**

- Read `PreviewAccess.streams`.
- Fetch each stream preview individually.
- Print each stream result separately.
- Keep a temporary fallback for legacy top-level `ticket` if `streams` is empty.

**Step 4: Run focused verification**

Run: `cargo test -p rapidbyte-cli distributed`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/distributed_run.rs crates/rapidbyte-cli/tests/distributed.rs
git commit -m "fix(cli): fetch distributed previews per stream"
```

### Task 6: Run Cross-Crate Regression Verification

**Files:**
- Reference: `crates/rapidbyte-agent/src/worker.rs`
- Reference: `crates/rapidbyte-agent/src/flight.rs`
- Reference: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Reference: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Run agent tests**

Run: `cargo test -p rapidbyte-agent`

Expected: pass.

**Step 2: Run controller tests**

Run: `cargo test -p rapidbyte-controller`

Expected: pass.

**Step 3: Run CLI distributed tests**

Run: `cargo test -p rapidbyte-cli distributed`

Expected: pass.

**Step 4: Run the end-to-end distributed integration**

Run: `cargo test -p rapidbyte-cli --test distributed`

Expected: pass with the updated preview and concurrency behavior.

**Step 5: Commit verification-only metadata if needed**

If no files changed, no commit. If test fixes were needed, commit them with a focused message.
