# Controller + Agent v1.1 Design Spec

> Approved spec: `docs/ARCHITECTUREv2.md` (the full design document).
> This file records validation results and implementation-relevant notes.

## Spec Validation Against Codebase

Validated 2026-03-12 against `architecturev2` branch.

### Confirmed Assumptions

| Assumption | Validated |
|---|---|
| `run_pipeline` is `pub async fn` with `(config, options, progress_tx)` | Yes — orchestrator.rs |
| `resolve_plugins`, `ResolvedPlugins`, `create_state_backend` are `pub(crate)` | Yes — resolve.rs |
| `ExecutionOptions` has `dry_run` + `limit` | Yes — execution.rs |
| `ProgressEvent` enum exists with phases | Yes — progress.rs |
| `StateBackend` trait with SQLite + Postgres impls | Yes — state crate |
| CLI uses clap derive with `Commands` enum | Yes — cli/main.rs |
| No existing gRPC/proto infrastructure | Confirmed — nothing to conflict with |
| No existing controller/agent code | Confirmed — greenfield |

### Visibility Changes Required

| Item | File | Current | Target |
|---|---|---|---|
| `mod resolve` | engine/src/lib.rs | `pub(crate)` | `pub` |
| `ResolvedPlugins` | engine/src/resolve.rs | `pub(crate)` | `pub` |
| `resolve_plugins()` | engine/src/resolve.rs | `pub(crate)` | `pub` |
| `create_state_backend()` | engine/src/resolve.rs | `pub(crate)` | `pub` |

### New Crates

| Crate | Type | Dependencies |
|---|---|---|
| `rapidbyte-controller` | lib | types, state, engine, tonic, prost, uuid |
| `rapidbyte-agent` | lib | types, runtime, engine, state, tonic, prost, arrow-flight, uuid |

Both consumed by the CLI binary — no separate executables.

### New Workspace Dependencies

| Crate | Version | Used By |
|---|---|---|
| `tonic` | 0.12 | controller, agent, cli |
| `prost` | 0.13 | controller, agent, cli |
| `tonic-build` | 0.12 | build.rs (controller, agent) |
| `arrow-flight` | 53 | agent, cli |
| `uuid` | 1 | controller, agent |

## Implementation Phases

Per ARCHITECTUREv2.md Section 16:

1. **Control Plane & Leases** — Controller metadata store, agent registry, lease epochs, CLI routing
2. **Remote Execution** — Agent runs `run_pipeline`, progress reporting, retry safety
3. **Cancellation** — `CancelRun` RPC, heartbeat directive delivery, CancellationToken
4. **Dry-Run Preview over Flight** — Preview spool, signed tickets, Flight server
5. **Production Hardening** — Persistent metadata, Tower middleware, bundle hash, secret redaction
