# Distributed CLI Flight Hardening Design

## Context

The distributed controller/agent path still has five user-visible correctness gaps:

1. `rapidbyte run --controller ...` treats a `WatchRun` stream that ends before a terminal event as success.
2. Distributed dry-runs swallow preview fetch failures and exit 0 even when no preview data can be retrieved.
3. The shipped controller CLI does not wire bearer auth tokens into `ControllerConfig`, so auth cannot actually be enabled from the binary.
4. The agent registers even when its Flight server fails to bind, leaving the controller with a preview-capable worker that cannot serve previews.
5. `get_flight_info` panics for valid zero-row preview streams because it indexes `batches[0]`.

## Design

### CLI Terminal Semantics

The distributed CLI should only return success after it has observed a terminal controller event and, for dry-runs, successfully fetched preview data.

`WatchRun` EOF before `Completed`, `Failed`, or `Cancelled` should become an error that explains the terminal event was never observed. That keeps controller restarts and transport drops from being misreported as successful executions.

Distributed preview retrieval is part of the dry-run result. If preview fetch fails after a `Completed` event, the command should return that error instead of logging and exiting 0.

### Controller CLI Auth Wiring

The global `--auth-token` / `RAPIDBYTE_AUTH_TOKEN` input should also configure the controller server path. The controller CLI will populate `ControllerConfig.auth_tokens` with the provided token when present.

This keeps the control plane auth model simple: zero tokens means auth disabled, one token means all shipped clients and the controller use the same bearer secret.

### Agent Startup Failure

The agent should fail fast if it cannot bind and serve the Flight endpoint it advertises. Instead of spawning an unobserved server task and continuing, startup should:

- bind the Flight listener before registration
- start the server with that bound listener
- surface any bind failure from `run()`

That prevents the controller from scheduling preview work onto an agent whose preview server never started.

### Empty Preview Stream Metadata

`get_flight_info` should support zero-row preview streams without panicking. The safe behavior is:

- if batches exist, use the existing batch schema
- if batches are empty, return `FlightInfo` with endpoint and counts, but omit schema encoding

That keeps discovery working for empty streams while avoiding fake schemas or service crashes.

## Testing Strategy

- CLI unit tests for premature watch EOF and dry-run preview fetch failure propagation.
- Controller CLI unit tests for auth token wiring.
- Agent worker test that startup fails when the Flight port is already bound.
- Flight service test that `get_flight_info` succeeds for an empty preview stream.
