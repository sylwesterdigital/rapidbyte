# Distributed Agent Preview Runtime Design

**Date:** 2026-03-12

## Goal

Fix four distributed-mode gaps together:

1. keep task leases alive until `CompleteTask` is acknowledged,
2. preserve per-stream schemas in Flight previews,
3. make `max_tasks` reflect actual concurrent execution,
4. return usable metadata from `GetFlightInfo`.

## Current Problems

### Completion lease loss

`crates/rapidbyte-agent/src/worker.rs` removes a task from `active_leases` immediately after the `CompleteTask` RPC returns, even if the RPC failed at the transport layer. Heartbeats then stop renewing that lease and the controller can time out already-finished work.

### Preview schema corruption

`crates/rapidbyte-agent/src/flight.rs` merges every dry-run stream into one `DoGet` payload and publishes only the first schema. This is incompatible with multi-stream dry-runs where streams legitimately have different columns or types.

### Serial worker despite `max_tasks`

The agent registers `max_tasks`, but the current loop polls one task and waits for `execute_task(...).await` to finish before polling again. The advertised capacity is not used.

### Incomplete Flight discovery metadata

`get_flight_info` currently returns only totals. Standard Flight clients need schema and a retrievable endpoint/ticket path in order to issue the follow-up `DoGet`.

## Chosen Approach

Use one registered agent with shared runtime state and up to `max_tasks` identical worker loops. Make preview retrieval stream-scoped instead of run-scoped so one Flight ticket always maps to one Arrow schema.

This keeps the controller contract mostly intact while fixing the broken semantics in the agent and preview data plane.

## Architecture

### Agent execution model

`worker::run` becomes a coordinator:

- register one `agent_id`,
- start the Flight server,
- start the heartbeat loop,
- create shared runtime state,
- spawn `max_tasks` task-runner loops.

Each task-runner loop:

- long-polls `PollTask`,
- inserts the assigned lease into shared `active_leases`,
- executes the pipeline,
- stores preview data if present,
- retries `CompleteTask` until the controller either acknowledges it or explicitly rejects it as stale,
- removes the lease only after one of those terminal controller responses.

Transport failures no longer drop the lease. Heartbeats continue renewing it while the runner retries completion.

### Completion retry state

The shared lease map remains the source of truth for heartbeat renewals. A task stays in that map during the completion retry window.

The retry loop should use bounded exponential backoff so it does not hammer the controller while still preserving liveness:

- initial short delay,
- capped backoff,
- continue until `acknowledged: true` or `acknowledged: false`.

If the controller replies `acknowledged: false`, the worker removes the lease and logs a stale completion. That means the controller has already moved on and there is no further useful retry.

### Preview model

Dry-run previews become per-stream in the public control-plane metadata.

`PreviewAccess.streams` is populated with one entry per dry-run stream. Each stream entry needs:

- stream name,
- row count,
- signed stream-scoped ticket.

The top-level preview block still carries endpoint and expiry information. The legacy top-level `ticket` field can remain empty for the distributed path once clients consume `streams`.

### Ticket payload

Preview tickets need to identify a specific stream. The ticket payload therefore extends from:

- `run_id`,
- `task_id`,
- `lease_epoch`,
- expiry,

to also include:

- `stream_name`.

The controller continues signing tickets so the control plane remains authoritative for preview access.

### Flight server behavior

`PreviewFlightService::do_get` verifies the signed stream-scoped ticket, loads the dry-run result for that task, finds the referenced stream, and emits only that stream's schema and batches.

This guarantees:

- one `DoGet` response has one schema,
- Arrow Flight decoding remains valid,
- different streams can expose different schemas safely.

`get_flight_info` uses the same ticket/stream lookup and returns:

- schema for that stream,
- total records,
- total bytes,
- at least one endpoint,
- a retrievable ticket in that endpoint.

That makes the service usable for standard Flight clients that call `GetFlightInfo` before `DoGet`.

### CLI preview retrieval

The distributed CLI preview path iterates over `PreviewAccess.streams` and fetches each stream separately. It prints each stream preview independently.

For a short transition window, the CLI may keep a fallback path:

- if `streams` is empty and top-level `ticket` exists, use the legacy single-ticket flow.

## Alternatives Considered

### Alternative 1: leave preview as a single merged table

Rejected. The engine already produces one dry-run result per stream, and different schemas are valid. Flattening them into one Flight stream is the direct cause of the bug.

### Alternative 2: central in-agent scheduler with `JoinSet` and explicit task records

Viable, but more stateful than necessary. Independent worker loops with shared lease state are simpler and map cleanly to the existing agent/controller protocol.

### Alternative 3: only fix lease retries and defer the other items

Rejected because the requested scope is all four review comments.

## Testing Strategy

### Agent completion

Add a regression test that simulates a transient `CompleteTask` transport failure and verifies the task lease remains in `active_leases` until a later successful completion response.

### Agent concurrency

Add a test with `max_tasks > 1` where two assigned tasks overlap in execution time and both are active concurrently.

### Preview protocol

Add tests for:

- stream-scoped ticket signing and verification,
- `DoGet` returning only one stream's schema and batches,
- multi-stream dry-runs with incompatible schemas decoding correctly when fetched separately,
- `GetFlightInfo` returning schema, endpoint, and ticket for the referenced stream.

### CLI integration

Add or update distributed preview tests so the CLI fetches multiple stream previews through the new per-stream metadata.

## Risks

### Proto churn

Changing preview metadata affects controller, agent, and CLI together. The fix should be landed as one coordinated change.

### Retry loops that never finish

The completion retry path must retain the lease for correctness, but it also needs bounded backoff and clear logging so a permanently unavailable controller is observable.

### Added concurrency in the agent

Shared structures (`active_leases`, preview spool, controller client creation) need tests to avoid races. The design deliberately keeps the shared state small.
