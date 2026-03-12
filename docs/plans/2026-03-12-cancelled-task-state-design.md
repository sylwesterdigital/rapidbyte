# Cancelled Task State Design

## Problem

The controller run lifecycle distinguishes `Cancelled` from `Failed`, but the scheduler completion API currently collapses terminal task outcomes to a boolean success flag. As a result, `CompleteTask(outcome = Cancelled)` is persisted as `TaskState::Failed`, even though the run becomes `Cancelled`.

This mismatch can pollute failure-oriented metrics and any future logic that inspects terminal task state directly.

## Decision

Replace the scheduler completion boolean with an explicit terminal task outcome enum:

- `Completed`
- `Failed`
- `Cancelled`

`AgentService::complete_task` passes the real proto task outcome into the scheduler, and the scheduler persists the matching terminal `TaskState`.

## Testing

- Add a scheduler regression proving cancelled completion persists `TaskState::Cancelled`.
- Extend the existing agent-service cancelled completion tests to assert the underlying task record is also `Cancelled`.

## Scope

This is a narrow scheduler/controller correctness fix. It does not change run-state transitions, watcher events, or lease fencing.
