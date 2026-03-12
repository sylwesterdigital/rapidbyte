# Controller Agent Auth Cancellation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the remaining controller/agent correctness gaps around agent validation, lease-fenced progress, cooperative cancellation, and optional bearer-auth client support.

**Architecture:** Keep the fixes local to the existing controller registry/scheduler, agent executor, and client wiring. Use controller-owned task state for capacity and lease fencing, treat cancellation as advisory once execution begins, and centralize optional tonic bearer auth in shared client helpers.

**Tech Stack:** Rust, Tokio, tonic gRPC, in-memory controller state, existing unit-test suites.

---

### Task 1: Document Current Failure Modes

**Files:**
- Create: `docs/plans/2026-03-12-controller-agent-auth-cancellation-design.md`
- Create: `docs/plans/2026-03-12-controller-agent-auth-cancellation-plan.md`

**Step 1: Write the approved design**

Capture the controller polling, lease-fencing, cooperative cancellation, and bearer-auth decisions in the design doc.

**Step 2: Save the implementation plan**

Record the task-by-task TDD plan in this file.

**Step 3: Commit**

```bash
git add docs/plans/2026-03-12-controller-agent-auth-cancellation-design.md docs/plans/2026-03-12-controller-agent-auth-cancellation-plan.md
git commit -m "docs: plan controller agent auth cancellation fixes"
```

### Task 2: Lock Down Polling And Progress With Failing Controller Tests

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/registry.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`

**Step 1: Write the failing tests**

Add controller tests for:

- unknown agents cannot poll tasks
- registered agents cannot poll beyond `max_tasks`
- progress is rejected when the lease has been cleared
- stale progress cannot move a reassigned run from `Assigned` to `Running`

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller test_poll_task_rejects_unknown_agent test_poll_task_respects_agent_capacity test_report_progress_rejects_missing_lease test_report_progress_does_not_flip_reassigned_attempt
```

Expected: failures showing the missing registry/capacity/lease fencing behavior.

**Step 3: Write the minimal implementation**

- Add scheduler support for counting active tasks per agent.
- Make `PollTask` validate registry membership and capacity before queue assignment.
- Route progress through strict lease validation and `report_running`.

**Step 4: Run the targeted tests to verify they pass**

Run the same command and expect all four tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/registry.rs crates/rapidbyte-controller/src/scheduler.rs
git commit -m "fix(controller): fence polling and progress by registry leases"
```

### Task 3: Make Agent Cancellation Cooperative

**Files:**
- Modify: `crates/rapidbyte-agent/src/executor.rs`

**Step 1: Write the failing tests**

Add tests for:

- pre-start cancellation still returns `Cancelled`
- cancellation after execution starts does not synthesize `Cancelled`

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent test_pre_cancelled_token_returns_cancelled test_cancellation_after_start_waits_for_pipeline_outcome
```

Expected: the second test fails under the current `select!` cancellation behavior.

**Step 3: Write the minimal implementation**

Remove mid-flight future abortion while preserving the pre-start cancellation fast path.

**Step 4: Run the targeted tests to verify they pass**

Run the same command and expect both tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/executor.rs
git commit -m "fix(agent): make distributed cancellation cooperative"
```

### Task 4: Add Optional Bearer Auth To Shipped Clients

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Write the failing tests**

Add focused tests for client metadata injection when a token is configured.

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent auth
cargo test -p rapidbyte-cli distributed auth
```

Expected: failures showing missing authorization metadata or missing token plumbing.

**Step 3: Write the minimal implementation**

- Add optional token fields to the agent/distributed CLI inputs.
- Wrap tonic clients with an interceptor that injects `authorization` metadata when configured.

**Step 4: Run the targeted tests to verify they pass**

Run the same commands and expect the new auth tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs crates/rapidbyte-cli/src/commands/agent.rs crates/rapidbyte-cli/src/commands/distributed_run.rs crates/rapidbyte-cli/src/main.rs
git commit -m "feat: add optional bearer auth to distributed clients"
```

### Task 5: Full Verification

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/registry.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-agent/src/executor.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Run package tests**

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-agent
cargo test -p rapidbyte-cli distributed
```

**Step 2: Inspect git status**

```bash
git status --short
```

**Step 3: Final commit**

```bash
git add <touched files>
git commit -m "fix distributed controller agent correctness gaps"
```
