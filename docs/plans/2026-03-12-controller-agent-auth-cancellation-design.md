# Controller Agent Auth Cancellation Design

## Context

The distributed controller and agent path has four correctness gaps:

1. `PollTask` trusts arbitrary `agent_id` values and ignores the registered agent's capacity.
2. `ReportProgress` accepts updates after a lease has been cleared, which breaks lease fencing.
3. The agent aborts `run_pipeline` on cancellation, which can hide partial destination writes behind a synthetic cancelled outcome.
4. Controller auth can be enabled on the server, but the shipped CLI and agent clients never send bearer tokens.

## Design

### Poll Validation And Capacity

`PollTask` should only assign work to registered agents. The controller will consult the in-memory registry before polling the queue and reject unknown agents with `NotFound`.

Capacity enforcement should be derived from controller-owned task state rather than heartbeat lag. The scheduler already records `assigned_agent_id` and task state, so it can count assigned and running tasks for a specific agent. `PollTask` will refuse to assign new work when that count is already at the agent's registered `max_tasks`.

### Lease-Fenced Progress

Progress updates should require an active lease with the exact epoch. A missing lease is stale by definition and must be rejected.

The first accepted progress update should advance the scheduler task from `Assigned` to `Running` through `TaskQueue::report_running`, not by inferring run state directly. That keeps the task and run state machines aligned and prevents stale progress from reactivating a new attempt.

### Cooperative Cancellation

The current executor races `run_pipeline` against the cancellation token and drops the future on cancel. That is unsafe because `run_pipeline` performs writes and later persists state.

The safer behavior is:

- If the task is cancelled before execution starts, return `Cancelled`.
- Once pipeline execution has begun, keep the task running to a real terminal engine outcome.
- Stop treating the cancellation token as a reason to drop `run_pipeline` mid-flight.

This makes cancellation advisory for in-flight work until the engine exposes a true cooperative cancellation boundary. It preserves correctness because the controller sees the actual commit-safety metadata from the engine instead of a synthetic cancelled result.

### Client Bearer Auth

Bearer auth should remain optional, but when configured it must be usable by the provided clients. The agent worker and distributed CLI path will accept an optional token and attach `authorization: Bearer <token>` metadata to every RPC.

To avoid duplicating client setup, the code should use a small shared helper that wraps tonic clients with an interceptor when a token is present.

## Testing Strategy

- Controller unit tests for unknown-agent polling and capacity enforcement.
- Controller unit tests for rejecting progress after lease clear and preserving run state for a reassigned attempt.
- Agent executor tests for pre-start cancellation and for ignoring cancellation after execution begins.
- CLI and agent client tests covering bearer metadata injection.
