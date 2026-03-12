# Cancelled Task State Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist cancelled task completions as `TaskState::Cancelled` instead of `TaskState::Failed`.

**Architecture:** Replace the scheduler's boolean completion API with a small terminal outcome enum and thread the real task outcome from `AgentService::complete_task` into scheduler persistence.

**Tech Stack:** Rust, rapidbyte-controller

---

### Task 1: Add the failing regressions

**Files:**
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write failing tests**

Add:
- a scheduler test proving cancelled completion persists `TaskState::Cancelled`
- task-state assertions in the existing agent-service cancelled completion tests

**Step 2: Run tests to verify they fail**

Run:
- `cargo test -p rapidbyte-controller complete_with_cancelled_outcome_persists_cancelled_state`
- `cargo test -p rapidbyte-controller test_complete_task_cancelled_from_assigned_transitions_run`

Expected: FAIL

### Task 2: Implement the minimal API change

**Files:**
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Introduce a terminal outcome enum**

Replace the `succeeded: bool` parameter with an explicit terminal task outcome enum and map it to the correct `TaskState`.

**Step 2: Thread the real completion outcome through agent service**

Pass the parsed proto `TaskOutcome` into scheduler completion.

**Step 3: Re-run focused tests**

Expected: PASS

### Task 3: Verify package and lint

**Step 1: Run controller tests**

Run: `cargo test -p rapidbyte-controller`

**Step 2: Run lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS
