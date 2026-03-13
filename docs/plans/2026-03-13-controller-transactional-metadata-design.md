# Controller Transactional Metadata Design

**Date:** 2026-03-13

**Problem**

The persistent-controller branch still has two correctness gaps around durable metadata:

1. multi-record controller transitions such as submit and claim persist related run/task records as separate writes with no database transaction boundary, so a process crash between writes can leave restart-unrecoverable partial state in Postgres
2. `report_progress` advances task/run state in memory before durable persistence succeeds, so a persistence failure can leave memory and durable state diverged until restart

These are not just local bugs. They show that the controller still treats Postgres as a write-through mirror in paths that now require crash-safe durability semantics.

## Decision Summary

Introduce transactional metadata-store operations for the controller paths that must update multiple durable records atomically, then make in-memory state follow durable success instead of preceding it.

The first cut should cover:

- submit: atomically create the run row and initial task row
- claim/assignment: atomically persist the assigned task row and assigned run row
- report progress: atomically persist the running task row and running run row

This is the smallest architectural fix that closes the reviewed crash windows while keeping the branch reviewable.

## Design

### Store Interface

Extend `DurableMetadataStore` with focused transactional methods rather than exposing generic transaction objects to service code.

Recommended methods:

- `create_run_with_task(run: &RunRecord, task: &TaskRecord)`
- `assign_task(run: &RunRecord, task: &TaskRecord)`
- `mark_task_running(run: &RunRecord, task: &TaskRecord)`

These methods should execute a single Postgres transaction and either commit both rows or commit neither. The service layer should not need to know transaction mechanics.

Keep the existing single-record helpers for:

- terminal single-row operations
- direct rollback helpers still used elsewhere in the branch
- tests and snapshot persistence utilities

This keeps the abstraction narrow and avoids a late-branch store rewrite.

### Submit

Today submit mutates in memory, snapshots the new run/task, then durably writes run and task separately. That is rollback-safe for write failures but not crash-safe between writes.

The new flow should be:

1. create the new run and initial task in memory as today
2. snapshot both records
3. call `create_run_with_task`
4. if the transaction fails, remove the in-memory run/task and return `INTERNAL`
5. only notify task waiters after the transaction commits

This preserves current API behavior while making the durable snapshot crash-safe.

### Claim / Assignment

Today claim mutates task and run in memory first, then persists run and task separately.

The new flow should be:

1. snapshot the pre-claim task/run records
2. claim in memory and snapshot the assigned task/run records
3. call `assign_task`
4. if the transaction fails, restore the previous in-memory task/run snapshots and return `INTERNAL`
5. return the assignment only after the transaction commits

This keeps the existing lease and scheduling logic in Rust while moving the durability boundary to one atomic store operation.

### Report Progress

`report_progress` should stop exposing non-durable running state.

The new flow should be:

1. snapshot the pre-progress task/run state
2. move the task to running in memory if needed, and move the run to running/reconciling exit state in memory
3. snapshot the updated task/run records
4. call `mark_task_running`
5. if the transaction fails, restore the previous task/run snapshots in memory and return `INTERNAL`
6. only publish progress events after the transaction commits

This closes the memory-vs-durable divergence the review called out.

### Scope Boundary

Do not try to transactionalize every multi-record controller path in this patch. `complete_task` and cancellation already have more involved rollback logic and can move onto the same abstraction in a follow-up once the new store methods are in place.

The goal here is a clean first cut:

- transactional store primitives
- reviewed paths cut over
- new failure-injection and store tests proving the new contract

## Testing Strategy

Add tests before implementation:

- metadata-store tests that prove each new transactional method is all-or-nothing under an injected failure
- a `report_progress` failure-injection test proving task/run state rolls back in memory when transactional persistence fails
- update submit and claim failure coverage only as needed to reflect the new transactional store calls

Verification after implementation:

- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-cli`
- the existing ignored Postgres-backed persistence and restart tests

## Rollout

Ship this as an architectural correctness fix in the branch before merge. The controller now depends on durable metadata for restart recovery, so these reviewed paths need crash-safe atomicity, not best-effort write ordering.
