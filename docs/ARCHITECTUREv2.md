# Rapidbyte v1.1 — Distributed Controller/Agent Architecture

> Design document — no code changes yet. Defines the distributed architecture
> for running pipelines across multiple machines via a controller/agent model.

---

## 1. Updated Dependency Graph

```
types (leaf — no internal deps)
  ├── state       → types
  ├── runtime     → types, state
  ├── sdk         → types
  ├── engine      → types, runtime, state
  │   ├── dev     → engine, runtime, types, state
  │   └── cli     → engine, runtime, types, dev, controller, agent
  ├── controller  → types, state, engine          (NEW — control plane only, never loads WASM)
  └── agent       → types, runtime, engine, state  (NEW — control plane + data plane)
```

### New Workspace Dependencies

| Crate        | Version | Notes                                          |
|--------------|---------|------------------------------------------------|
| tonic        | 0.12    | gRPC server + client (control plane)           |
| prost        | 0.13    | Protobuf code generation                       |
| tonic-build  | 0.12    | Build-time proto compilation                   |
| arrow-flight | 53      | Arrow Flight data plane (matches arrow = 53)   |
| uuid         | 1       | v4 generation, serde support                   |

Both `rapidbyte-controller` and `rapidbyte-agent` are library crates consumed by the CLI binary. No separate binaries — keeps deployment as a single `rapidbyte` executable.

### Dependency Assignment

| Crate      | tonic (control) | arrow-flight (data) | Why                                   |
|------------|-----------------|---------------------|---------------------------------------|
| controller | yes             | no                  | Scheduling only — never touches data  |
| agent      | yes             | yes                 | Receives tasks + serves data streams  |
| cli        | yes             | yes                 | Routes commands + receives data       |

---

## 2. Scope

v1.1 is **distributed job execution**, not distributed dataflow.

One submitted pipeline becomes one task. One task runs on one agent. That gives you horizontal scale across many runs, while avoiding cross-agent shuffle, partial commit ambiguity, and pipeline splitting.

---

## 3. Two Operating Modes

### Standalone (current behavior, unchanged)

```
rapidbyte run pipeline.yaml
```

CLI calls `rapidbyte_engine::run_pipeline` directly. No network, no controller.

### Distributed (controller present)

```
rapidbyte --controller grpc://ctrl.internal:9090 run pipeline.yaml
```

CLI routes commands through gRPC to the controller. Agents pull tasks from the controller and execute them.

**Detection priority** (first match wins):

1. `--controller <url>` CLI flag
2. `RAPIDBYTE_CONTROLLER` environment variable
3. `~/.rapidbyte/config.yaml` → `controller.url` field

If none is set, standalone mode is used.

---

## 4. Roles

### Controller

- Owns run/task/agent metadata
- Schedules tasks
- Maintains leases and assignment epochs
- Streams run status/progress to CLI
- Issues short-lived preview tickets
- **Never loads WASM**
- **Never executes check / discover**
- **Never touches batch data**

### Agent

- Registers itself and advertises a reachable Flight endpoint
- Resolves plugins and loads WASM
- Executes the existing engine
- Accesses the pipeline's configured state backend directly
- Reports progress/completion to controller
- Serves dry-run preview replay via Flight

---

## 5. Control Plane vs Data Plane

The distributed architecture separates two concerns:

```
┌──────────────────────────────────────────────────────────┐
│                    CONTROL PLANE                         │
│          tonic/protobuf — low-bandwidth, reliable        │
│                                                          │
│  Task scheduling, heartbeats, agent registry,            │
│  progress events, run state, lease epochs                │
│                                                          │
│  Controller ←──protobuf──→ Agent                         │
│  Controller ←──protobuf──→ CLI                           │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                     DATA PLANE                           │
│       Arrow Flight — high-bandwidth, streaming           │
│                                                          │
│  Arrow RecordBatch transfer: dry-run preview replay,     │
│  cross-agent batch streaming (v2), dev shell queries     │
│                                                          │
│  Agent  ──── Flight DoGet ────→ CLI (v1.1: preview)      │
│  Agent  ──── Flight DoGet ────→ Agent (v2: cross-agent)  │
│  Agent  ──── Flight SQL  ────→ Dev Shell (v2+)           │
└──────────────────────────────────────────────────────────┘
```

### Why This Split

**Control messages** (task assignment, heartbeat, progress counters) are small, structured, and benefit from protobuf's schema evolution and strong typing. Overhead is irrelevant.

**Data messages** (Arrow RecordBatches) are large, columnar, and already in Arrow IPC format internally. Arrow Flight provides:

- **Zero-copy streaming** — batches flow directly from engine's mpsc channels to the network without serialization. Rapidbyte already uses Arrow IPC as its internal frame format; Flight speaks the same wire encoding.
- **Backpressure** — Flight's streaming RPCs (DoGet, DoPut) propagate backpressure from consumer to producer, preventing OOM on slow destinations.
- **Schema negotiation** — GetFlightInfo returns Arrow schemas before data transfer begins, enabling the CLI/dev-shell to prepare display formatters.
- **gRPC underneath** — shares TLS, auth interceptors, and connection infrastructure with the control plane. No second protocol to operate.

### Why Not Protobuf for Data

Serializing Arrow batches to JSON/bytes inside protobuf messages adds unnecessary copies and loses type fidelity. Arrow Flight eliminates this: the same RecordBatch that flows through local mpsc channels is the same RecordBatch that goes over the wire.

---

## 6. State Split

Use two different stores:

### Pipeline State Backend

Existing `rapidbyte_state` backend used by the engine for cursors, run records, DLQ, CAS, etc. The engine already calls `create_state_backend(config)` during execution, and `finalize_successful_run_state` already correlates checkpoints and completes the run.

**The controller does not own pipeline state writes in v1.1.** The engine still creates the pipeline state backend inside execution and finalizes run state, cursor correlation, and DLQ persistence inside the orchestrator path. Centralizing checkpoint ownership on the controller would force a deeper engine refactor than v1.1 warrants.

### Controller Metadata Store

New controller-owned store for runs, tasks, agents, leases, attempts, preview metadata. In-memory for v1.1; Postgres-backed in v2 for durability.

---

## 7. Hard Rules for Distributed Mode

### 7.1 Reject Local SQLite in Distributed Mode

The current code supports both SQLite and Postgres state backends. That is fine for standalone, but not for multi-node reassignment. If an agent dies and another agent retries, a local SQLite file on the first node is not a shared source of truth.

**Rule:**
- Standalone: SQLite or Postgres
- Distributed: **Postgres only** in v1.1
- Optional dev escape hatch: `--allow-unsafe-local-sqlite`

### 7.2 Lease Epochs / Fencing

Every assignment carries:

| Field              | Purpose                                              |
|--------------------|------------------------------------------------------|
| `task_id`          | Unique per task attempt                              |
| `attempt`          | Monotonic per run                                    |
| `lease_epoch`      | Monotonic per run, for fencing stale operations      |
| `lease_expires_at` | Absolute deadline for lease validity                 |

The controller **must reject** stale:
- Heartbeats
- Progress reports
- Completion reports
- Preview access

This is the piece that makes re-enqueue safe.

### 7.3 Reassignment Must Respect Retry Safety

The current engine retry behavior is already conservative: a pipeline error is retryable only when the plugin marked it both `retryable` and `safe_to_retry`, and post-commit states flip `safe_to_retry` off.

Controller policy must match:

| Condition | Action |
|-----------|--------|
| Agent-reported `retryable` + `safe_to_retry` | Auto-requeue allowed |
| Timeout / lost lease | Do **not** assume safe retry |
| Commit state unknown/confirmed | Operator action or explicit policy required |

This avoids controller-side duplicate writes.

---

## 8. Controller Crate (`rapidbyte-controller`)

A **control-plane-only** coordination server. It does **not** load WASM, execute plugins, or handle Arrow data. It schedules work and tracks state.

### 8.1 gRPC Services (tonic — control plane)

Proto file location: `proto/rapidbyte/v1/controller.proto`

#### PipelineService — CLI-facing

| RPC            | Description                                        |
|----------------|----------------------------------------------------|
| SubmitPipeline | Submit a pipeline YAML for execution. Returns run_id. |
| GetRun         | Get current status of a pipeline run.              |
| WatchRun       | Server-stream of progress events for a run.        |
| CancelRun      | Cancel a running pipeline.                         |
| ListRuns       | List recent pipeline runs with filters.            |

#### AgentService — Agent-facing

| RPC            | Description                                        |
|----------------|----------------------------------------------------|
| RegisterAgent  | Agent announces itself on startup. Returns agent_id. |
| Heartbeat      | Periodic liveness signal. Carries load/status info. Controller returns directives. |
| PollTask       | Agent requests next task (long-poll). Returns task or empty. |
| ReportProgress | Agent streams progress events for an assigned task. |
| CompleteTask   | Agent reports task completion with metrics/preview. |

**Deliberately excluded from v1.1:** Check, Discover (these resolve plugins / load manifests / run discover logic that is not a pure controller action).

### 8.2 Internal Modules

| Module           | Responsibility                                                          |
|------------------|-------------------------------------------------------------------------|
| scheduler.rs     | FIFO task queue, assignment tracking, re-enqueue logic                  |
| registry.rs      | Agent registry, heartbeat monitoring, liveness reaping, Flight endpoint directory |
| run_state.rs     | In-memory run state machine (lifecycle below)                           |
| watcher.rs       | Broadcast channels for WatchRun streaming                               |
| ticket.rs        | Signed preview ticket issuance and verification                         |

---

## 9. Agent Crate (`rapidbyte-agent`)

A stateless worker process. Participates in both planes:
- **Control plane:** pulls tasks from controller, reports progress/completion via protobuf
- **Data plane:** runs an Arrow Flight server to serve dry-run preview replay

### 9.1 Key Design Principle

The agent reuses the existing engine as the execution core. A single call to `run_pipeline(config, options, progress_tx, cancel_token)` runs the full source → transform → destination pipeline on the agent using local mpsc channels. v1.1 still avoids distributed orchestrator refactoring, but now threads a cooperative cancellation token so the engine can stop at safe boundaries instead of being aborted mid-commit.

The `progress_tx` channel is bridged to the controller's `ReportProgress` RPC so the CLI can display live progress via `WatchRun`.

### 9.2 Agent Flight Service (data plane)

Each agent runs an Arrow Flight server alongside its control-plane gRPC client. The Flight endpoint is advertised to the controller during `RegisterAgent` so the controller can provide routing information to CLI clients.

#### V1.1 Flight Endpoints

| Flight RPC    | Use Case                                     | Ticket/Descriptor                    |
|---------------|----------------------------------------------|--------------------------------------|
| GetFlightInfo | Schema + metadata for a preview              | Signed preview ticket                |
| ListFlights   | List active runs on this agent               | Diagnostic tooling                   |
| DoGet         | Stream RecordBatches for dry-run preview     | Signed preview ticket                |

#### Dry-Run in v1.1: Replay-Based

Distributed dry-run is **replay-based**, not live Flight tap. The current runtime channel carries IPC-encoded `Frame::Data { payload, checkpoint_id }`, and dry-run collects by draining frames and decoding IPC after the fact. A live "engine mpsc → Flight DoGet" bridge is not the current shape of the code.

**Proposed flow:**

1. CLI submits run with `dry_run: true` to controller
2. Controller assigns task to agent
3. Agent runs normal engine dry-run
4. Agent stores preview results:
   - In memory up to threshold
   - Spill to temp Arrow IPC files above threshold
5. Agent reports `preview_ready` via `CompleteTask`
6. Controller returns signed preview ticket + endpoint via `GetRun`
7. CLI calls `GetFlightInfo` / `DoGet` and replays preview

This matches the current code where dry-run already materializes decoded batches as a completed result.

#### V2 Flight Endpoints (deferred)

| Flight RPC  | Use Case                                     |
|-------------|----------------------------------------------|
| DoPut       | Dest-agent receives batches from source-agent |
| DoExchange  | Bidirectional transform-agent streaming       |
| Flight SQL  | Dev shell queries remote agent DataFusion     |

### 9.3 Agent Lifecycle

```
Start
  → Start Flight server on flight_listen address
  → RegisterAgent(capabilities, flight_advertise_endpoint)
  → loop {
      PollTask(wait_seconds=30)   // long-poll
        → if task: execute pipeline, spool preview (if dry_run), ReportProgress, CompleteTask
        → if empty: Heartbeat
    }
  → DeregisterAgent (on graceful shutdown — not an RPC; agent just stops heartbeating)
  → Stop Flight server
```

---

## 10. Task Model

### V1.1: Task = Full Pipeline

One pipeline submission produces one task. One task is assigned to one agent. The agent runs the entire pipeline locally (source → transform → destination).

This avoids:
- Cross-agent data transport (v2 concern — solved by Flight when ready)
- Splitting the orchestrator across machines
- Complex partial-failure semantics

### Task Lifecycle

```
PENDING  →  ASSIGNED  →  RUNNING  →  COMPLETED
                                  →  FAILED
                      →  TIMED_OUT (lease expired → new PENDING task)
```

| State     | Meaning                                                         |
|-----------|------------------------------------------------------------------|
| PENDING   | Submitted, waiting in scheduler queue                            |
| ASSIGNED  | Claimed by an agent via PollTask                                 |
| RUNNING   | Agent has begun execution                                        |
| COMPLETED | Agent reports success; engine has finalized state internally     |
| FAILED    | Agent reports failure; if retryable + safe → new PENDING task    |
| TIMED_OUT | Lease expired → controller re-enqueues as new PENDING task       |

**Invariant:** At most one active (ASSIGNED/RUNNING) task exists per pipeline run at any time. No split-brain — the controller is the single source of truth for assignment.

### Retry Semantics

On failure or timeout, the controller creates a **new** task (new `task_id`, same `run_id`, incremented `attempt` and `lease_epoch`). The new task inherits the pipeline config. The agent reads fresh cursor state from the shared state backend on startup, so partial progress is preserved via existing checkpoint correlation.

Controller checks `TaskError` fields before re-enqueue:

```
if error.retryable && error.safe_to_retry:
    auto-requeue with new task_id, incremented attempt
elif lease_timeout:
    require operator intervention or explicit policy
else:
    mark run FAILED, no retry
```

---

## 11. CLI Changes

### Global Flag

Add `--controller <url>` to the `Cli` struct:

```rust
#[derive(Parser)]
struct Cli {
    /// Controller gRPC endpoint (enables distributed mode)
    #[arg(long, global = true, env = "RAPIDBYTE_CONTROLLER")]
    controller: Option<String>,
    // ... existing fields unchanged
}
```

Each command checks: if `controller` is set, route via gRPC client to the controller. Otherwise, use the existing standalone code path. No behavioral change when controller is absent.

### New Subcommands

```rust
enum Commands {
    // ... existing: Run, Check, Discover, Plugins, Scaffold, Dev
    /// Start the controller server (long-running)
    Controller {
        /// gRPC listen address
        #[arg(long, default_value = "[::]:9090")]
        listen: String,
    },
    /// Start an agent worker (long-running)
    Agent {
        /// Controller endpoint to connect to
        #[arg(long)]
        controller: String,
        /// Flight server bind address (data plane)
        #[arg(long, default_value = "0.0.0.0:9091")]
        flight_listen: String,
        /// Advertised Flight endpoint for clients (what controller hands out)
        #[arg(long)]
        flight_advertise: String,
        /// Maximum concurrent tasks
        #[arg(long, default_value = "1")]
        max_tasks: u32,
    },
}
```

Both subcommands start long-running processes. The single-binary approach keeps deployment simple — no separate controller/agent binaries to distribute.

**Agent endpoint UX:** Do not register the bind address as the data endpoint by default. Use two flags — `flight_listen` is for binding, `flight_advertise` is what the controller hands to clients. This avoids the classic `0.0.0.0` / NAT / container-network footgun.

### Commands Routed Through Controller (v1.1)

| Command    | Distributed behavior                              |
|------------|---------------------------------------------------|
| `run`      | SubmitPipeline → controller → agent execution     |
| `status`   | GetRun via controller                              |
| `watch`    | WatchRun stream via controller                     |
| `list-runs`| ListRuns via controller                            |

### Commands Kept Local (v1.1)

| Command    | Why                                                |
|------------|----------------------------------------------------|
| `check`    | Resolves plugins / loads manifests / runs validation logic |
| `discover` | Runs discover plugin logic, not a pure controller action |
| `dev`      | Local ArrowWorkspace on local SessionContext        |

### Distributed `--dry-run`

In standalone mode, `--dry-run` prints batches to stdout as before.

In distributed mode:
1. CLI submits pipeline with `dry_run: true` to controller
2. Controller assigns task to agent; agent runs dry-run and spools preview
3. CLI polls `GetRun` until `preview` state is `READY`
4. CLI receives signed preview ticket + agent Flight endpoint from `GetRun`
5. CLI opens Flight `DoGet` to the agent with the signed ticket
6. CLI renders batches using existing output formatters

Same user experience, same data fidelity — just running remotely with replay instead of live tap.

---

## 12. Engine Changes Required

Minimal for v1.1. No signature changes, no behavioral changes.

### 12.1 resolve.rs — make public

Currently `pub(crate)`:

| Item                | Current      | Change to | Reason                                      |
|---------------------|-------------|-----------|-----------------------------------------------|
| `ResolvedPlugins`   | pub(crate)  | pub       | Agent needs to resolve plugins before running |
| `resolve_plugins`   | pub(crate)  | pub       | Agent calls this to locate WASM binaries      |
| `create_state_backend` | pub(crate)| pub       | Agent creates state backend for execution     |

The resolve module itself needs to change from `pub(crate) mod resolve` to `pub mod resolve` in `engine/src/lib.rs`.

### 12.2 orchestrator.rs — no changes

`run_pipeline` is already `pub async fn` and takes `(&PipelineConfig, &ExecutionOptions, Option<UnboundedSender<ProgressEvent>>)`. The agent calls it as-is. No wrapper needed.

### 12.3 Preview spool (new, in agent crate)

A small preview-spool abstraction lives in the **agent** crate, not the engine. It buffers `RecordBatch` results from dry-run in memory up to a threshold, then spills to temp Arrow IPC files. The Flight `DoGet` handler reads from this spool.

The bridge converts `arrow::record_batch::RecordBatch` to `arrow_flight::FlightData` using `arrow_flight::utils::flight_data_from_arrow_batch`. This is a single function call — Arrow Flight and the engine share the same arrow version (53), so no conversion overhead.

### 12.4 Deferred engine changes

- **Cancellation token:** Add an optional cancellation token to `run_pipeline` before promising strong cancel semantics.
- **Checkpoint ownership:** Do **not** refactor checkpoint correlation into the controller in v1.1.

---

## 13. Failure Handling

### Agent Crash

Controller detects via lease expiry (no heartbeat received within lease window, default: 60 s). The assigned task transitions to TIMED_OUT. The controller does **not** auto-requeue — timeout/lost-lease does not imply safe retry. Operator intervention or explicit policy is required.

**Data plane impact:** CLI Flight connections to the crashed agent fail immediately. The CLI retries by polling `GetRun` for a new agent assignment, then reconnects Flight to the new agent. No data corruption — the new agent starts fresh from cursors in the shared Postgres state backend.

### Plugin Error (Retryable)

Handled entirely by the engine's existing retry loop on the agent. The controller is not involved — the agent reports the final outcome (success or exhausted retries) via `CompleteTask` with `TaskError` fields carrying retry safety metadata.

### Task Timeout

Controller-side safety net. Default: 4 hours. If a task stays ASSIGNED/RUNNING beyond this threshold (even with valid heartbeats), the controller cancels it via heartbeat directive and optionally re-enqueues. Protects against hung pipelines.

### Controller Crash (v1.1)

In-memory metadata state is lost. Running agents lose their control-plane gRPC connection and begin retry/reconnect loops. The data plane (agent Flight servers) continues to serve any in-flight DoGet streams until completion, then the agent pauses because it can't report completion.

In-flight pipelines are orphaned and must be resubmitted after controller restart. Cursor state in the shared Postgres state backend is durable — no data loss, just operational disruption.

**Deferred (v2):** Persistent controller metadata store (Postgres-backed) so the controller can recover in-progress runs on restart.

### Flight Connection Failure

If a CLI's Flight DoGet stream to an agent drops mid-transfer:
- The pipeline itself continues running on the agent (Flight is observational, not critical path)
- The CLI can reconnect and issue a new DoGet — the agent serves batches from the spool (v1.1: restart from beginning of preview replay; v2: resumable offsets via Flight tickets)

---

## 14. Security

### V1.1

| Concern          | Approach                                                                 |
|------------------|--------------------------------------------------------------------------|
| Transport        | TLS for all gRPC and Flight (`tonic::transport::ServerTlsConfig`)        |
| CLI auth         | CLI authenticates to controller (bearer token or mTLS)                   |
| Preview tickets  | Controller issues short-lived signed preview tickets containing `run_id`, `task_id`, `lease_epoch`, `subject`, `expiry`, optional stream filter |
| Agent Flight ACL | Agent **verifies ticket signature** before serving DoGet — no shared bearer token directly against agents |
| Credentials      | Pipeline YAML may contain DB credentials → **TLS is mandatory in production** |
| Plugin sandbox   | Agent-side plugin sandbox remains the enforcement point. Controller/agent transport rules do not grant plugins more network reach. |

**Why signed tickets instead of shared bearer token:**

A single shared bearer token gives any holder full access to all agent Flight endpoints. Signed tickets scope access to a specific run, task, and time window. This matches Flight's model — Flight tickets are intentionally opaque binary tokens.

Arrow Flight runs on gRPC, so TLS and auth interceptors are shared infrastructure — no separate credential system for the data plane.

### Deferred (v2+)

- mTLS between agents and controller (covers both planes)
- JWT/OIDC for CLI authentication
- RBAC (role-based access control)
- Agent-side secret injection (Vault, AWS Secrets Manager) so credentials never transit the controller
- Flight ticket signing with asymmetric keys (controller signs, agents verify — prevents unauthorized agent-to-agent connections)

---

## 15. Control Plane: Protobuf Schema

File: `proto/rapidbyte/v1/controller.proto`

All control-plane messages. The data plane (Arrow Flight) uses Arrow IPC encoding natively — no protobuf needed for batch transfer.

```protobuf
syntax = "proto3";
package rapidbyte.v1;

import "google/protobuf/timestamp.proto";

// ──────────────────────────────────────────────
// Pipeline Service — CLI-facing (control plane)
// ──────────────────────────────────────────────

service PipelineService {
  rpc SubmitPipeline(SubmitPipelineRequest) returns (SubmitPipelineResponse);
  rpc GetRun(GetRunRequest)                 returns (GetRunResponse);
  rpc WatchRun(WatchRunRequest)             returns (stream RunEvent);
  rpc CancelRun(CancelRunRequest)           returns (CancelRunResponse);
  rpc ListRuns(ListRunsRequest)             returns (ListRunsResponse);
}

message SubmitPipelineRequest {
  bytes pipeline_yaml_utf8 = 1;     // Raw YAML as UTF-8 bytes
  ExecutionOptions execution = 2;
  string idempotency_key = 3;       // Client-generated, prevents duplicate submissions
}

message ExecutionOptions {
  bool dry_run = 1;
  optional uint64 limit = 2;        // Max rows per stream (only with dry_run)
}

message SubmitPipelineResponse {
  string run_id = 1;                // Controller-assigned UUID v4
}

message GetRunRequest { string run_id = 1; }
message WatchRunRequest { string run_id = 1; }
message CancelRunRequest { string run_id = 1; }

message CancelRunResponse {
  bool accepted = 1;
  string message = 2;              // e.g. "queued run cancelled", "running — cancel best-effort"
}

message GetRunResponse {
  string run_id = 1;
  RunState state = 2;
  string pipeline_name = 3;
  google.protobuf.Timestamp submitted_at = 4;
  optional google.protobuf.Timestamp started_at = 5;
  optional google.protobuf.Timestamp completed_at = 6;
  optional TaskRef current_task = 7;
  optional PreviewAccess preview = 8;
  optional TaskError last_error = 9;
}

message ListRunsRequest {
  int32 limit = 1;                  // Default 20
  optional RunState filter_state = 2;
}

message ListRunsResponse {
  repeated RunSummary runs = 1;
}

message RunSummary {
  string run_id = 1;
  string pipeline_name = 2;
  RunState state = 3;
  google.protobuf.Timestamp submitted_at = 4;
}

message RunEvent {
  string run_id = 1;
  oneof event {
    ProgressUpdate progress = 2;
    RunCompleted completed = 3;
    RunFailed failed = 4;
    RunCancelled cancelled = 5;
    RunStatus status = 6;
  }
}

message ProgressUpdate {
  string stream = 1;
  string phase = 2;                 // Maps to engine::progress::Phase
  uint64 records = 3;
  uint64 bytes = 4;
}

message RunCompleted {
  uint64 total_records = 1;
  uint64 total_bytes = 2;
  double elapsed_seconds = 3;
  uint64 cursors_advanced = 4;
}

message RunFailed {
  TaskError error = 1;
  uint32 attempt = 2;
}

message RunCancelled {}

message RunStatus {
  RunState state = 1;
  string message = 2;
}

// ──────────────────────────────────────────────
// Agent Service — Agent-facing (control plane)
// ──────────────────────────────────────────────

service AgentService {
  rpc RegisterAgent(RegisterAgentRequest)       returns (RegisterAgentResponse);
  rpc Heartbeat(HeartbeatRequest)               returns (HeartbeatResponse);
  rpc PollTask(PollTaskRequest)                 returns (PollTaskResponse);
  rpc ReportProgress(ReportProgressRequest)     returns (ReportProgressResponse);
  rpc CompleteTask(CompleteTaskRequest)          returns (CompleteTaskResponse);
}

message RegisterAgentRequest {
  uint32 max_tasks = 1;
  string flight_advertise_endpoint = 2;  // Reachable Flight address for clients
  string plugin_bundle_hash = 3;         // Hash of local plugin inventory
  repeated string available_plugins = 4; // Informational in v1.1
  uint64 memory_bytes = 5;
}

message RegisterAgentResponse {
  string agent_id = 1;              // Controller-assigned UUID v4
}

message PollTaskRequest {
  string agent_id = 1;
  uint32 wait_seconds = 2;          // Long-poll timeout (e.g. 30)
}

message PollTaskResponse {
  oneof result {
    TaskAssignment task = 1;
    NoTask no_task = 2;
  }
}

message NoTask {}

message TaskAssignment {
  string task_id = 1;               // UUID v4
  string run_id = 2;
  uint32 attempt = 3;               // Monotonic per run
  uint64 lease_epoch = 4;           // Monotonic per run, for fencing
  google.protobuf.Timestamp lease_expires_at = 5;
  bytes pipeline_yaml_utf8 = 6;     // Full pipeline config
  ExecutionOptions execution = 7;   // dry_run, limit
}

message HeartbeatRequest {
  string agent_id = 1;
  repeated ActiveLease active_leases = 2;
  uint32 active_tasks = 3;
  double cpu_usage = 4;             // 0.0–1.0
  uint64 memory_used_bytes = 5;
}

message ActiveLease {
  string task_id = 1;
  uint64 lease_epoch = 2;
}

message HeartbeatResponse {
  repeated AgentDirective directives = 1;
}

message AgentDirective {
  oneof directive {
    CancelTask cancel_task = 1;
  }
}

message CancelTask {
  string task_id = 1;
  uint64 lease_epoch = 2;
}

message ReportProgressRequest {
  string agent_id = 1;
  string task_id = 2;
  uint64 lease_epoch = 3;
  ProgressUpdate progress = 4;
}

message ReportProgressResponse {}

message CompleteTaskRequest {
  string agent_id = 1;
  string task_id = 2;
  uint64 lease_epoch = 3;
  TaskOutcome outcome = 4;
  optional TaskError error = 5;     // Set if FAILED or CANCELLED
  TaskMetrics metrics = 6;
  optional PreviewAccess preview = 7; // Set if dry_run and preview ready
  int64 backend_run_id = 8;         // Engine's internal run ID (debugging/reconciliation)
}

message CompleteTaskResponse {
  bool acknowledged = 1;            // false if lease_epoch is stale
}

message TaskMetrics {
  uint64 records_processed = 1;
  uint64 bytes_processed = 2;
  double elapsed_seconds = 3;
  uint64 cursors_advanced = 4;
}

message TaskError {
  string code = 1;                  // Plugin error code
  string message = 2;
  bool retryable = 3;
  bool safe_to_retry = 4;          // Engine's safety determination
  string commit_state = 5;         // "before_commit", "after_commit_unknown", "after_commit_confirmed"
}

// ──────────────────────────────────────────────
// Shared types
// ──────────────────────────────────────────────

message TaskRef {
  string task_id = 1;
  string agent_id = 2;
  uint32 attempt = 3;
  uint64 lease_epoch = 4;
  google.protobuf.Timestamp assigned_at = 5;
}

message PreviewAccess {
  PreviewState state = 1;
  string flight_endpoint = 2;       // Agent's advertised Flight address
  bytes ticket = 3;                 // Signed opaque token
  google.protobuf.Timestamp expires_at = 4;
  repeated StreamPreview streams = 5;
}

message StreamPreview {
  string stream = 1;
  uint64 rows = 2;
}

enum RunState {
  RUN_STATE_UNSPECIFIED = 0;
  RUN_STATE_PENDING = 1;
  RUN_STATE_ASSIGNED = 2;
  RUN_STATE_RUNNING = 3;
  RUN_STATE_PREVIEW_READY = 4;
  RUN_STATE_COMPLETED = 5;
  RUN_STATE_FAILED = 6;
  RUN_STATE_CANCELLED = 7;
  RUN_STATE_RECONCILING = 8;
  RUN_STATE_RECOVERY_FAILED = 9;
}

enum TaskOutcome {
  TASK_OUTCOME_UNSPECIFIED = 0;
  TASK_OUTCOME_COMPLETED = 1;
  TASK_OUTCOME_FAILED = 2;
  TASK_OUTCOME_CANCELLED = 3;
}

enum PreviewState {
  PREVIEW_STATE_UNSPECIFIED = 0;
  PREVIEW_STATE_NONE = 1;
  PREVIEW_STATE_BUILDING = 2;
  PREVIEW_STATE_READY = 3;
  PREVIEW_STATE_EXPIRED = 4;
}
```

### What Was Deliberately Excluded from Proto

| Removed RPC/Field         | Reason                                                                   |
|---------------------------|--------------------------------------------------------------------------|
| Check                     | Resolves plugins / loads manifests — not a pure controller action        |
| Discover                  | Runs discover plugin logic — not a pure controller action                |
| `checkpoints_json`        | Controller does not own checkpoint correlation in v1.1                   |
| `agent_flight_endpoint`   | Replaced by signed `PreviewAccess` tickets with proper scoping           |
| Controller-assigned Flight port | Agents manage their own Flight binding                              |

---

## 16. Data Plane: Arrow Flight Protocol

The data plane uses Arrow Flight directly — no custom protobuf messages for batch transfer.

### Flight Ticket Format

Agents receive **signed opaque tickets** from the controller (via `PreviewAccess.ticket`). The ticket is an opaque binary token containing:

```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "task_id": "...",
  "lease_epoch": 42,
  "subject": "cli-user-abc",
  "expires_at": "2026-03-12T15:04:05Z",
  "stream": "users"
}
```

The agent verifies the ticket signature before serving data. This prevents unauthorized access even if the agent's Flight endpoint is reachable.

### Flight <> Engine Integration

The engine's dry-run already materializes decoded batches as a completed result. The agent spools these into a preview buffer. The Flight adapter converts them to `FlightData` using arrow-flight's built-in utilities:

```
Engine dry-run output        Agent preview spool          CLI
─────────────────            ─────────────────            ─────────────────
RecordBatch ──→ spool (memory/disk) ──→ flight_data_from_arrow_batch() ──→ FlightData stream
                                        (zero-copy — same Arrow version)   (gRPC/HTTP2)
```

Since both the engine (`arrow = "53"`) and Flight (`arrow-flight = "53"`) use the same Arrow version, conversion is a pointer cast + metadata header — no deserialization.

### V1.1 Flight Endpoints (agent)

| Flight RPC    | Purpose                                  | When Used                     |
|---------------|------------------------------------------|-------------------------------|
| GetFlightInfo | Return schema + row counts for a preview | CLI pre-allocates display buffer |
| ListFlights   | List active previews on this agent       | Diagnostic tooling            |
| DoGet         | Stream RecordBatches for a dry-run preview | `--dry-run` in distributed mode |

### Compression

V1.1 Flight streams use uncompressed Arrow IPC. The engine already supports lz4/zstd compression for its internal frame transport — extending this to Flight is a v2 feature via Arrow Flight's built-in codec negotiation (`FlightDescriptor` + `FlightInfo.schema` metadata).

---

## 17. V2 Vision: Cross-Agent Data Flow via Flight

V1.1 runs full pipelines on a single agent. V2 decomposes pipelines into per-stage tasks assigned to different agents, with Arrow Flight as the data transport between them.

### Architecture

```
Controller (control plane only — provides routing, never touches data)
    │
    │  "source task → agent-1, dest task → agent-2"
    │  "agent-1 Flight endpoint: flight://agent1:9091"
    │
    ▼
┌──────────┐  Flight DoGet    ┌──────────┐
│ Agent 1  │ ───────────────→ │ Agent 2  │
│ (source) │  RecordBatches   │  (dest)  │
│          │  Arrow IPC wire  │          │
└──────────┘                  └──────────┘
```

The controller orchestrates by telling the destination agent which source agent to connect to — analogous to HDFS NameNode providing block locations while DataNodes transfer data directly. The controller never proxies data, avoiding it becoming a bottleneck.

### Per-Stage Task Model (v2)

```
Pipeline submission → Controller decomposes into:
  Task A: source    (assigned to agent-1, produces batches via Flight DoGet)
  Task B: transform (assigned to agent-2, consumes from agent-1, produces via DoGet)
  Task C: dest      (assigned to agent-3, consumes from agent-2 via DoGet)
```

Each task's `TaskAssignment` would include upstream Flight endpoints:

```protobuf
message TaskAssignment {
  string task_id = 1;
  string run_id = 2;
  string pipeline_yaml = 3;
  repeated FlightEndpoint upstream_sources = 4;  // v2: where to DoGet input data
}

message FlightEndpoint {
  string endpoint = 1;              // "flight://agent1:9091"
  string stream = 2;               // stream name filter
  bytes ticket = 3;                 // signed ticket for auth
}
```

### Why Flight Over Custom Transport

| Concern              | Custom transport            | Arrow Flight                           |
|----------------------|----------------------------|----------------------------------------|
| Wire format          | Need to design             | Arrow IPC (already used internally)    |
| Backpressure         | Need to implement          | Built-in (gRPC flow control)           |
| TLS                  | Need to integrate          | Shared with control plane              |
| Auth                 | Need to design             | gRPC interceptors + ticket signing     |
| Schema negotiation   | Need to design             | GetFlightInfo / GetSchema              |
| Client libraries     | Need to write              | Available in Rust, Python, Java, Go    |
| Compression          | Already have lz4/zstd      | Built-in codec negotiation             |
| Observability        | Need to instrument         | gRPC metrics + OpenTelemetry           |

### Dev Shell Remote Queries (v2+)

Arrow Flight SQL extends Flight with a SQL interface. The agent's DataFusion instance (used by `transform-sql`) can serve as a Flight SQL endpoint, letting the dev shell query remote agent data:

```
rapidbyte dev --controller grpc://ctrl.internal:9090
  → REPL connects to controller
  → Controller provides agent Flight SQL endpoints
  → Dev shell runs SQL queries against remote agents
  → Results stream back as Arrow batches
```

---

## 18. V1.1 vs Deferred

### V1.1 Scope

- Full-pipeline-per-agent task model (one task = one complete pipeline run)
- FIFO scheduling (no priority, no affinity)
- Long-poll task assignment + lease-epoch fencing
- Agent-managed state backend (shared Postgres required in distributed mode)
- Controller metadata store (in-memory)
- Signed preview tickets (scoped, time-limited)
- TLS for all gRPC and Flight
- CLI routing: standalone vs distributed based on `--controller`
- Progress streaming via `WatchRun` (control plane)
- Dry-run preview replay via Flight `DoGet` (data plane)
- Retry with safety metadata (`retryable`, `safe_to_retry`, `commit_state`)
- Local `check` / `discover` / `dev` (not routed through controller)

### Deferred (v2+)

| Feature                          | Why Deferred                                           | Flight Role                       |
|----------------------------------|--------------------------------------------------------|-----------------------------------|
| Cross-agent data flow            | Per-stage task decomposition needed first               | DoGet/DoPut between agents        |
| Per-stream task decomposition    | Requires splitting the orchestrator                     | Each stream = separate Flight ticket |
| Persistent controller state      | Needs Postgres-backed run/task tables                   | —                                 |
| Plugin registry                  | Controller distributing WASM to agents                  | Could use DoPut for WASM transfer |
| Scheduling policies              | Priority, affinity, resource-aware placement            | —                                 |
| Multi-controller HA              | Leader election, state replication                      | —                                 |
| Observability                    | Prometheus metrics, OpenTelemetry tracing               | gRPC interceptors for free        |
| Web dashboard                    | HTTP API layer + frontend                               | —                                 |
| Secret management                | Vault / AWS SM integration on agents                    | —                                 |
| RBAC                             | Role-based access control for multi-tenant              | Flight ticket signing             |
| Warm plugin pools                | Pre-loaded WASM instances on agents for fast start      | —                                 |
| Flight compression               | Needs codec negotiation implementation                  | Built-in lz4/zstd support         |
| Dev shell remote SQL             | Needs Flight SQL server on agents                       | Flight SQL protocol               |
| Resumable data streams           | Needs offset tracking in Flight tickets                 | Ticket-based resume               |
| Remote check/discover            | Needs probe task model on agents                        | —                                 |
| Cooperative cancel               | Needs cancellation token in `run_pipeline`              | —                                 |

---

## 19. Data Flow Diagrams

### Standalone Mode (unchanged)

```
┌─────────┐     run_pipeline()     ┌────────┐
│   CLI   │ ──────────────────────→│ Engine │
└─────────┘                        └────────┘
                                   Source → Transform → Dest
                                      (local mpsc channels)
```

### Distributed Mode — V1.1

```
CONTROL PLANE (protobuf)                DATA PLANE (Arrow Flight)
────────────────────────                ─────────────────────────

┌─────────┐  SubmitPipeline   ┌────────────┐  PollTask    ┌─────────┐
│   CLI   │ ─────────────────→│ Controller │←─────────────│  Agent  │
│         │←─────────────────│            │──────────────→│         │
│         │  WatchRun         │            │  TaskAssign   │         │
│         │  (progress events)│            │               │         │
│         │                   │  Scheduler │  Heartbeat    │ Engine  │
│         │  GetRun           │  Registry  │←─────────────│ Runtime │
│         │─────────────────→│  Leases    │  Progress     │ WASM    │
│         │←─────────────────│  Tickets   │←─────────────│         │
│         │  (preview ticket) │            │  Complete     │ Flight  │
│         │                   └────────────┘  (+preview)   │ Server  │
│         │                                               │         │
│         │──── Flight DoGet (signed ticket) ────────────→│         │
│         │←─── RecordBatch replay (Arrow IPC) ──────────│         │
└─────────┘                                               └─────────┘
                                                                │
                                                          ┌─────────────┐
                                                          │ Shared PG   │
                                                          │ State Backend│
                                                          └─────────────┘
```

### Distributed Mode — V2 (cross-agent)

```
CONTROL PLANE                    DATA PLANE

┌─────────┐  Submit    ┌────────────┐               ┌──────────┐
│   CLI   │ ──────────→│ Controller │──TaskAssign──→│ Agent 1  │
│         │            │            │               │ (source)  │
│         │            │            │               │          │
│         │            │            │               │ Flight   │──── DoGet ────┐
│         │            │            │               │          │               │
│         │            │            │               └──────────┘               │
│         │            │            │                                    Arrow IPC
│         │            │            │               ┌──────────┐               │
│         │            │            │──TaskAssign──→│ Agent 2  │←─────────────┘
│         │            │            │               │  (dest)  │
│         │←──Watch───│            │←──Complete───│          │
└─────────┘            └────────────┘               └──────────┘
```

### Task Lifecycle Flow

```
CLI: SubmitPipeline(yaml, execution_options, idempotency_key)
  → Controller: parse, validate, create run (PENDING)
  → Controller: enqueue task with lease_epoch

Agent: PollTask(wait_seconds=30)
  → Controller: assign task (PENDING → ASSIGNED), set lease_expires_at
  → Agent: run_pipeline(config, options, progress_tx)
    → Agent: ReportProgress(lease_epoch) → controller → WatchRun to CLI
    → If dry_run: agent spools preview batches
  → Agent: CompleteTask(lease_epoch, outcome, metrics, preview?)
    → If preview: controller stores PreviewAccess with signed ticket
  → Controller: run state → COMPLETED (or PREVIEW_READY if dry_run)

CLI (dry-run):
  → GetRun → receives PreviewAccess with signed ticket + Flight endpoint
  → Flight DoGet(ticket) to agent → replayed RecordBatch stream

(on failure)
  → Agent: CompleteTask(failed, retryable, safe_to_retry, commit_state)
  → Controller: if retryable && safe_to_retry:
      create new task (new task_id, same run_id, attempt++, lease_epoch++) → PENDING
    else:
      run state → FAILED, surface last_error via GetRun
```

---

## 20. Implementation Phases

### Phase 1 — Control Plane and Leases

- Add controller metadata store trait (in-memory impl)
- Add agent registry with heartbeat monitoring and liveness reaping
- Add long-poll `PollTask` with lease epoch fencing
- Add `lease_epoch` validation on all agent-facing RPCs
- CLI routing for `run` / `status` / `watch` / `list-runs`
- Reject distributed mode when `state.backend = sqlite`

### Phase 2 — Remote Execution

- Agent runs existing `run_pipeline`
- Controller tracks attempts, failures, and retries
- Completion carries retry safety metadata (`retryable`, `safe_to_retry`, `commit_state`)
- No remote `check` / `discover` / `dev`

### Phase 3 — Dry-Run Replay over Flight

- Preview spool in agent: memory buffer + spill-to-disk threshold
- Controller-issued signed preview tickets
- Agent `GetFlightInfo` + `DoGet` serving preview replay
- TTL cleanup for preview artifacts

### Phase 4 — Production Hardening

- Durable controller metadata store (Postgres-backed)
- Secret redaction in pipeline YAML transit
- Tower middleware for auth, rate limits, tracing, deadlines
- Plugin bundle hash enforcement across agent pool

### Phase 5 — Optional Follow-Ons

- Cooperative cancel for running tasks (cancellation token in `run_pipeline`)
- Remote probe tasks for `check` / `discover`
- Cross-agent pipeline decomposition (per-stage tasks)
- Flight SQL / remote dev shell
- Resumable Flight streams with offset tracking
