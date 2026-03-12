# Controller + Agent v1.1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add distributed pipeline execution via a controller/agent architecture — phases 1-5 of ARCHITECTUREv2.md.

**Architecture:** Controller coordinates task scheduling and agent registry via gRPC (tonic/protobuf). Agents pull tasks via long-poll, execute pipelines using the existing engine, and serve dry-run previews via Arrow Flight. CLI routes commands to controller when `--controller` flag is set, otherwise runs locally as before.

**Tech Stack:** Rust, tonic 0.12, prost 0.13, arrow-flight 53, uuid 1, tokio, Tower middleware

---

## File Structure

### New crates

| Path | Responsibility |
|------|---------------|
| `crates/rapidbyte-controller/` | Control-plane gRPC server (scheduling, registry, leases, preview tickets) |
| `crates/rapidbyte-agent/` | Agent worker (task execution, progress forwarding, Flight preview server) |

### New files — proto

| Path | Responsibility |
|------|---------------|
| `proto/rapidbyte/v1/controller.proto` | Protobuf service + message definitions (control plane) |

### New files — controller crate

| Path | Responsibility |
|------|---------------|
| `crates/rapidbyte-controller/Cargo.toml` | Crate manifest |
| `crates/rapidbyte-controller/build.rs` | tonic-build proto compilation |
| `crates/rapidbyte-controller/src/lib.rs` | Crate root, module declarations, public API |
| `crates/rapidbyte-controller/src/proto.rs` | Generated protobuf module inclusion |
| `crates/rapidbyte-controller/src/server.rs` | gRPC server startup and wiring |
| `crates/rapidbyte-controller/src/pipeline_service.rs` | PipelineService RPC implementations |
| `crates/rapidbyte-controller/src/agent_service.rs` | AgentService RPC implementations |
| `crates/rapidbyte-controller/src/scheduler.rs` | FIFO task queue, assignment, lease epoch management |
| `crates/rapidbyte-controller/src/registry.rs` | Agent registry, heartbeat monitoring, liveness reaping |
| `crates/rapidbyte-controller/src/run_state.rs` | Run state machine with attempt tracking |
| `crates/rapidbyte-controller/src/lease.rs` | Lease epoch generation, validation, expiry |
| `crates/rapidbyte-controller/src/preview.rs` | Signed ticket issuance, TTL management |
| `crates/rapidbyte-controller/src/watcher.rs` | Broadcast channels for WatchRun streaming |

### New files — agent crate

| Path | Responsibility |
|------|---------------|
| `crates/rapidbyte-agent/Cargo.toml` | Crate manifest |
| `crates/rapidbyte-agent/build.rs` | tonic-build proto compilation |
| `crates/rapidbyte-agent/src/lib.rs` | Crate root, module declarations, public API |
| `crates/rapidbyte-agent/src/proto.rs` | Generated protobuf module inclusion |
| `crates/rapidbyte-agent/src/worker.rs` | Main agent loop (register, poll, heartbeat, execute) |
| `crates/rapidbyte-agent/src/executor.rs` | Task execution wrapper around engine::run_pipeline |
| `crates/rapidbyte-agent/src/progress.rs` | ProgressEvent → ReportProgress forwarding |
| `crates/rapidbyte-agent/src/flight.rs` | Arrow Flight server for preview replay |
| `crates/rapidbyte-agent/src/spool.rs` | Preview spool (memory + spill-to-disk) |
| `crates/rapidbyte-agent/src/ticket.rs` | Ticket validation (shared with controller) |

### Modified files

| Path | Change |
|------|--------|
| `Cargo.toml` (root) | Add workspace members + dependencies |
| `crates/rapidbyte-engine/src/lib.rs` | `pub(crate) mod resolve` → `pub mod resolve` |
| `crates/rapidbyte-engine/src/resolve.rs` | `pub(crate)` → `pub` on 4 items |
| `crates/rapidbyte-cli/Cargo.toml` | Add controller, agent, tonic, arrow-flight deps |
| `crates/rapidbyte-cli/src/main.rs` | Add `--controller` global flag, `Controller`/`Agent` subcommands |
| `crates/rapidbyte-cli/src/commands/mod.rs` | Add controller, agent, distributed_run modules |
| `crates/rapidbyte-cli/src/commands/run.rs` | Route through controller when `--controller` is set |

### New CLI command files

| Path | Responsibility |
|------|---------------|
| `crates/rapidbyte-cli/src/commands/controller.rs` | `rapidbyte controller` — start controller server |
| `crates/rapidbyte-cli/src/commands/agent.rs` | `rapidbyte agent` — start agent worker |
| `crates/rapidbyte-cli/src/commands/distributed_run.rs` | Submit + watch pipeline via controller gRPC |

---

## Chunk 1: Proto + Workspace Setup + Engine Visibility

### Task 1: Add workspace dependencies

**Files:**
- Modify: `Cargo.toml` (root workspace)

- [ ] **Step 1: Add new workspace dependencies**

Add to `[workspace.dependencies]`:

```toml
tonic = { version = "0.12", features = ["tls"] }
prost = "0.13"
prost-types = "0.13"
tonic-build = "0.12"
arrow-flight = { version = "53", features = ["tls"] }
uuid = { version = "1", features = ["v4", "serde"] }
hmac = "0.12"
sha2 = "0.10"
bytes = "1"
tower = { version = "0.5", features = ["full"] }
tokio-util = { version = "0.7", features = ["rt"] }
```

- [ ] **Step 2: Verify workspace file parses**

Run: `cargo metadata --no-deps --format-version 1 > /dev/null`
Expected: success (exit 0)

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "build: add gRPC, Flight, and crypto workspace dependencies"
```

### Task 2: Create proto file

**Files:**
- Create: `proto/rapidbyte/v1/controller.proto`

- [ ] **Step 1: Create proto directory and file**

Write `proto/rapidbyte/v1/controller.proto` with the full protobuf schema from ARCHITECTUREv2.md Section 11 (lines 667-942). Copy it verbatim.

- [ ] **Step 2: Commit**

```bash
git add proto/
git commit -m "proto: add controller.proto with PipelineService and AgentService"
```

### Task 3: Widen engine resolve.rs visibility

**Files:**
- Modify: `crates/rapidbyte-engine/src/lib.rs:33`
- Modify: `crates/rapidbyte-engine/src/resolve.rs:16,25,55,115`

- [ ] **Step 1: Write test that accesses resolve module from outside the crate**

Create `crates/rapidbyte-engine/tests/resolve_visibility.rs`:

```rust
//! Verify that resolve module items are publicly accessible.

use rapidbyte_engine::resolve::{create_state_backend, resolve_plugins, ResolvedPlugins};

#[test]
fn resolve_types_are_public() {
    // Compile-time check: these types are importable from outside the crate.
    let _: fn(&rapidbyte_engine::PipelineConfig) -> Result<ResolvedPlugins, _> = resolve_plugins;
    let _: fn(&rapidbyte_engine::PipelineConfig) -> anyhow::Result<_> = create_state_backend;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine --test resolve_visibility 2>&1 | head -20`
Expected: compilation error — `module resolve is private`

- [ ] **Step 3: Change resolve module visibility**

In `crates/rapidbyte-engine/src/lib.rs` line 33, change:
```rust
pub(crate) mod resolve;
```
to:
```rust
pub mod resolve;
```

- [ ] **Step 4: Change struct and function visibility in resolve.rs**

In `crates/rapidbyte-engine/src/resolve.rs`:

Line 16: `pub(crate) struct ResolvedPlugins` → `pub struct ResolvedPlugins`
Lines 17-22: all `pub(crate)` fields → `pub`
Line 25: `pub(crate) fn resolve_plugins` → `pub fn resolve_plugins`
Line 55: `pub(crate) fn load_and_validate_manifest` → `pub fn load_and_validate_manifest`
Line 86: `pub(crate) fn validate_config_against_schema` → `pub fn validate_config_against_schema`
Line 115: `pub(crate) fn create_state_backend` → `pub fn create_state_backend`
Line 140: `pub(crate) fn check_state_backend` → `pub fn check_state_backend`
Line 161: `pub(crate) fn build_sandbox_overrides` → `pub fn build_sandbox_overrides`

- [ ] **Step 5: Run the visibility test**

Run: `cargo test -p rapidbyte-engine --test resolve_visibility`
Expected: PASS

- [ ] **Step 6: Run full workspace tests to ensure no regressions**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-engine/
git commit -m "refactor(engine): make resolve module public for agent consumption"
```

### Task 4: Create controller crate skeleton

**Files:**
- Create: `crates/rapidbyte-controller/Cargo.toml`
- Create: `crates/rapidbyte-controller/build.rs`
- Create: `crates/rapidbyte-controller/src/lib.rs`
- Create: `crates/rapidbyte-controller/src/proto.rs`
- Modify: `Cargo.toml` (add workspace member)

- [ ] **Step 1: Create crate directory**

```bash
mkdir -p crates/rapidbyte-controller/src
```

- [ ] **Step 2: Write Cargo.toml**

```toml
[package]
name = "rapidbyte-controller"
version.workspace = true
edition.workspace = true

[dependencies]
rapidbyte-types = { path = "../rapidbyte-types" }
rapidbyte-engine = { path = "../rapidbyte-engine" }
serde = { workspace = true }
serde_yaml = { workspace = true }
tonic = { workspace = true }
prost = { workspace = true }
prost-types = { workspace = true }
tokio = { workspace = true }
uuid = { workspace = true }
tracing = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
hmac = { workspace = true }
sha2 = { workspace = true }
bytes = { workspace = true }
tower = { workspace = true }

[build-dependencies]
tonic-build = { workspace = true }

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
```

- [ ] **Step 3: Write build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["../../../proto/rapidbyte/v1/controller.proto"],
            &["../../../proto"],
        )?;
    Ok(())
}
```

- [ ] **Step 4: Write src/proto.rs**

```rust
//! Generated protobuf types for the controller control plane.

pub mod rapidbyte {
    pub mod v1 {
        tonic::include_proto!("rapidbyte.v1");
    }
}
```

- [ ] **Step 5: Write src/lib.rs**

```rust
//! Rapidbyte controller — control-plane coordination server.
//!
//! Schedules pipeline tasks across agents, manages leases,
//! and streams progress to CLI watchers. Does NOT load WASM
//! or execute plugins.
//!
//! # Crate structure
//!
//! | Module             | Responsibility |
//! |--------------------|----------------|
//! | `proto`            | Generated protobuf types |
//! | `server`           | gRPC server startup and wiring |
//! | `pipeline_service` | PipelineService RPC implementations |
//! | `agent_service`    | AgentService RPC implementations |
//! | `scheduler`        | FIFO task queue, assignment, lease epochs |
//! | `registry`         | Agent registry, heartbeat monitoring |
//! | `run_state`        | Run state machine with attempt tracking |
//! | `lease`            | Lease epoch generation, validation, expiry |
//! | `preview`          | Signed ticket issuance, TTL |
//! | `watcher`          | Broadcast channels for WatchRun |

#![warn(clippy::pedantic)]

pub mod proto;
```

- [ ] **Step 6: Add to workspace members**

In root `Cargo.toml`, add `"crates/rapidbyte-controller"` to the `members` array.

- [ ] **Step 7: Verify it compiles**

Run: `cargo check -p rapidbyte-controller`
Expected: success (proto compiles, crate builds)

- [ ] **Step 8: Commit**

```bash
git add crates/rapidbyte-controller/ Cargo.toml Cargo.lock
git commit -m "feat(controller): scaffold crate with proto compilation"
```

### Task 5: Create agent crate skeleton

**Files:**
- Create: `crates/rapidbyte-agent/Cargo.toml`
- Create: `crates/rapidbyte-agent/build.rs`
- Create: `crates/rapidbyte-agent/src/lib.rs`
- Create: `crates/rapidbyte-agent/src/proto.rs`
- Modify: `Cargo.toml` (add workspace member)

- [ ] **Step 1: Create crate directory**

```bash
mkdir -p crates/rapidbyte-agent/src
```

- [ ] **Step 2: Write Cargo.toml**

```toml
[package]
name = "rapidbyte-agent"
version.workspace = true
edition.workspace = true

[dependencies]
rapidbyte-types = { path = "../rapidbyte-types" }
rapidbyte-engine = { path = "../rapidbyte-engine" }
rapidbyte-runtime = { path = "../rapidbyte-runtime" }
rapidbyte-state = { path = "../rapidbyte-state", features = ["postgres"] }
tonic = { workspace = true }
prost = { workspace = true }
prost-types = { workspace = true }
tokio = { workspace = true }
uuid = { workspace = true }
tracing = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
arrow = { workspace = true }
arrow-flight = { workspace = true }
bytes = { workspace = true }
hmac = { workspace = true }
sha2 = { workspace = true }
tokio-util = { workspace = true }

[build-dependencies]
tonic-build = { workspace = true }

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
```

- [ ] **Step 3: Write build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["../../../proto/rapidbyte/v1/controller.proto"],
            &["../../../proto"],
        )?;
    Ok(())
}
```

- [ ] **Step 4: Write src/proto.rs**

```rust
//! Generated protobuf types for agent ↔ controller communication.

pub mod rapidbyte {
    pub mod v1 {
        tonic::include_proto!("rapidbyte.v1");
    }
}
```

- [ ] **Step 5: Write src/lib.rs**

```rust
//! Rapidbyte agent — stateless pipeline worker.
//!
//! Pulls tasks from the controller, executes pipelines via the engine,
//! reports progress, and serves dry-run previews via Arrow Flight.
//!
//! # Crate structure
//!
//! | Module     | Responsibility |
//! |------------|----------------|
//! | `proto`    | Generated protobuf types |
//! | `worker`   | Main agent loop (register, poll, heartbeat, execute) |
//! | `executor` | Task execution wrapper around engine::run_pipeline |
//! | `progress` | ProgressEvent → ReportProgress forwarding |
//! | `flight`   | Arrow Flight server for preview replay |
//! | `spool`    | Preview spool (memory + spill-to-disk) |
//! | `ticket`   | Ticket validation |

#![warn(clippy::pedantic)]

pub mod proto;
```

- [ ] **Step 6: Add to workspace members**

In root `Cargo.toml`, add `"crates/rapidbyte-agent"` to the `members` array.

- [ ] **Step 7: Verify it compiles**

Run: `cargo check -p rapidbyte-agent`
Expected: success

- [ ] **Step 8: Commit**

```bash
git add crates/rapidbyte-agent/ Cargo.toml Cargo.lock
git commit -m "feat(agent): scaffold crate with proto compilation"
```

---

## Chunk 2: Controller Core — Lease, Registry, Scheduler, Run State

### Task 6: Implement lease module

**Files:**
- Create: `crates/rapidbyte-controller/src/lease.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write tests for lease module**

At the bottom of a new `crates/rapidbyte-controller/src/lease.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn epoch_generator_starts_at_one() {
        let gen = EpochGenerator::new();
        assert_eq!(gen.next(), 1);
    }

    #[test]
    fn epoch_generator_increments() {
        let gen = EpochGenerator::new();
        assert_eq!(gen.next(), 1);
        assert_eq!(gen.next(), 2);
        assert_eq!(gen.next(), 3);
    }

    #[test]
    fn lease_is_valid_before_expiry() {
        let lease = Lease::new(1, Duration::from_secs(60));
        assert!(lease.is_valid(1));
    }

    #[test]
    fn lease_rejects_stale_epoch() {
        let lease = Lease::new(2, Duration::from_secs(60));
        assert!(!lease.is_valid(1));
    }

    #[test]
    fn lease_rejects_after_expiry() {
        let lease = Lease::new(1, Duration::from_secs(0));
        std::thread::sleep(Duration::from_millis(10));
        assert!(!lease.is_valid(1));
    }

    #[test]
    fn lease_renew_extends_expiry() {
        let mut lease = Lease::new(1, Duration::from_secs(0));
        lease.renew(Duration::from_secs(60));
        assert!(lease.is_valid(1));
    }
}
```

- [ ] **Step 2: Implement lease module above the tests**

```rust
//! Lease epoch generation, validation, and expiry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Monotonically increasing epoch generator.
pub struct EpochGenerator {
    counter: AtomicU64,
}

impl EpochGenerator {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    /// Generate the next epoch value (starts at 1, never returns 0).
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// A lease with an epoch and expiry time.
#[derive(Debug, Clone)]
pub struct Lease {
    pub epoch: u64,
    pub expires_at: Instant,
}

impl Lease {
    pub fn new(epoch: u64, ttl: Duration) -> Self {
        Self {
            epoch,
            expires_at: Instant::now() + ttl,
        }
    }

    /// Check if this lease is valid for the given epoch and has not expired.
    pub fn is_valid(&self, epoch: u64) -> bool {
        self.epoch == epoch && Instant::now() < self.expires_at
    }

    /// Check if this lease has expired.
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Extend the lease by the given duration from now.
    pub fn renew(&mut self, ttl: Duration) {
        self.expires_at = Instant::now() + ttl;
    }
}
```

- [ ] **Step 3: Add module to lib.rs**

Add `pub mod lease;` to `crates/rapidbyte-controller/src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-controller -- lease`
Expected: all 6 tests pass

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement lease epoch generation and validation"
```

### Task 7: Implement run_state module

**Files:**
- Create: `crates/rapidbyte-controller/src/run_state.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write the run_state module with tests**

Create `crates/rapidbyte-controller/src/run_state.rs`. This module manages the state machine for runs:

```
PENDING → ASSIGNED → RUNNING → COMPLETED | FAILED | CANCELLED
                              → TIMED_OUT
```

Core types:
- `RunState` enum — Pending, Assigned, Running, PreviewReady, Completed, Failed, Cancelling, Cancelled, TimedOut (note: Cancelling is internal, not exposed in proto)
- `RunRecord` — holds run_id, pipeline_name, state, timestamps, current task, error, preview access, attempt count, idempotency_key
- `RunStore` — `HashMap<String, RunRecord>` + `HashMap<String, String>` for idempotency dedup. Methods: `create_run`, `get_run`, `transition`, `list_runs`, `find_by_idempotency_key`
- State transitions validated (e.g., can't go from COMPLETED to RUNNING)
- Valid transitions: Pending→Assigned, Assigned→Running, Running→Completed|Failed|Cancelled|TimedOut|PreviewReady, PreviewReady→Completed, Pending→Cancelled (direct cancel), Running→Cancelling (internal), Cancelling→Cancelled

Tests should cover:
- Creating a run puts it in Pending state
- Valid transitions succeed (Pending→Assigned, Assigned→Running, Running→Completed, Running→PreviewReady, PreviewReady→Completed, etc.)
- Invalid transitions return error (Completed→Running, Failed→Running)
- Cancelling is reachable from Running, Cancelling→Cancelled works
- list_runs with optional state filter
- get_run returns None for unknown run_id
- Idempotency: create_run with same key returns existing run_id
- Idempotency: create_run with different key creates new run

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- run_state`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement run state machine with transitions"
```

### Task 8: Implement registry module

**Files:**
- Create: `crates/rapidbyte-controller/src/registry.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write registry module with tests**

Create `crates/rapidbyte-controller/src/registry.rs`. This module tracks registered agents:

Core types:
- `AgentRecord` — agent_id, max_tasks, active_tasks, flight_endpoint, plugin_bundle_hash, last_heartbeat, available_plugins, memory_bytes
- `AgentRegistry` — `HashMap<String, AgentRecord>` with methods: `register`, `heartbeat`, `get`, `remove`, `reap_dead(timeout: Duration) -> Vec<String>`, `list`

Tests should cover:
- Register an agent, get it back
- Heartbeat updates last_heartbeat timestamp
- Heartbeat for unknown agent returns error
- reap_dead removes agents past timeout
- reap_dead preserves live agents
- Remove agent

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- registry`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement agent registry with heartbeat and reaping"
```

### Task 9: Implement scheduler module

**Files:**
- Create: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write scheduler module with tests**

Create `crates/rapidbyte-controller/src/scheduler.rs`. FIFO task queue with lease management:

Core types:
- `TaskRecord` — task_id, run_id, attempt, lease (Option<Lease>), state (TaskState enum), pipeline_yaml, execution_options, assigned_agent_id
- `TaskState` — Pending, Assigned, Running, Completed, Failed, Cancelled, TimedOut
- `TaskQueue` — VecDeque for pending tasks, HashMap for all tasks. Methods:
  - `enqueue(run_id, pipeline_yaml, execution_options) -> task_id`
  - `poll(agent_id, lease_ttl: Duration, epoch_gen: &EpochGenerator) -> Option<TaskAssignment>` — dequeue next pending, assign lease
  - `report_running(task_id, lease_epoch) -> Result<()>` — validate epoch, transition to Running
  - `complete(task_id, lease_epoch, outcome) -> Result<bool>` — validate epoch, transition; returns false if stale
  - `cancel(task_id) -> Result<()>` — mark cancelled (or remove from queue if Pending)
  - `expire_leases() -> Vec<(String, String)>` — find expired leases, transition to TimedOut, return (task_id, run_id) pairs
  - `get(task_id) -> Option<&TaskRecord>`

Tests should cover:
- Enqueue + poll returns the task with valid lease
- Poll on empty queue returns None
- Complete with valid epoch succeeds
- Complete with stale epoch returns false (acknowledged=false)
- expire_leases catches timed-out tasks
- Cancel a pending task removes it from queue
- Cancel a running task marks it Cancelled
- FIFO ordering preserved

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- scheduler`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement FIFO task scheduler with lease fencing"
```

### Task 10: Implement watcher module

**Files:**
- Create: `crates/rapidbyte-controller/src/watcher.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write watcher module with tests**

Create `crates/rapidbyte-controller/src/watcher.rs`. Broadcast channels for WatchRun streaming:

Core types:
- `RunWatchers` — `HashMap<String, tokio::sync::broadcast::Sender<RunEvent>>` where `RunEvent` is the proto type
- Methods: `subscribe(run_id) -> broadcast::Receiver`, `publish(run_id, event)`, `remove(run_id)`

Use `tokio::sync::broadcast` with a reasonable buffer (256 events).

Tests:
- Subscribe then publish receives the event
- Multiple subscribers all receive
- Publish to non-existent run is a no-op
- Remove cleans up

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- watcher`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement run event broadcast for WatchRun"
```

### Task 11: Implement preview module (signed tickets)

**Files:**
- Create: `crates/rapidbyte-controller/src/preview.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write preview module with tests**

Create `crates/rapidbyte-controller/src/preview.rs`. Signed preview tickets:

Core types:
- `TicketSigner` — wraps HMAC-SHA256 key. Methods: `new(key: &[u8])`, `sign(payload: &TicketPayload) -> bytes::Bytes`, `verify(ticket: &[u8]) -> Result<TicketPayload>`
- `TicketPayload` — run_id, task_id, lease_epoch, subject, expires_at (serialized with prost or simple bincode-like format)
- `PreviewStore` — tracks preview metadata per run_id: flight_endpoint, ticket, expires_at, stream info. Methods: `store`, `get`, `cleanup_expired`

Ticket format: `payload_bytes || hmac_signature` (32-byte HMAC appended). Payload is simple: length-prefixed fields or JSON bytes (keep it simple for v1.1).

Tests:
- Sign and verify roundtrip succeeds
- Tampered ticket fails verification
- Expired ticket fails verification
- PreviewStore stores and retrieves
- PreviewStore cleanup removes expired entries

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- preview`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement signed preview tickets with HMAC-SHA256"
```

---

## Chunk 3: Controller gRPC Services

### Task 12: Implement ControllerState shared state container

**Files:**
- Create: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Write ControllerState**

This wraps all the internal modules behind `Arc<Mutex<_>>` or `Arc<RwLock<_>>` for concurrent access from gRPC handlers:

```rust
//! Shared controller state accessed by gRPC service handlers.

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::lease::EpochGenerator;
use crate::preview::{PreviewStore, TicketSigner};
use crate::registry::AgentRegistry;
use crate::run_state::RunStore;
use crate::scheduler::TaskQueue;
use crate::watcher::RunWatchers;

/// Shared state container for the controller.
///
/// All gRPC service implementations hold an `Arc<ControllerState>`.
#[derive(Clone)]
pub struct ControllerState {
    pub runs: Arc<RwLock<RunStore>>,
    pub tasks: Arc<RwLock<TaskQueue>>,
    pub registry: Arc<RwLock<AgentRegistry>>,
    pub watchers: Arc<RwLock<RunWatchers>>,
    pub previews: Arc<RwLock<PreviewStore>>,
    pub epoch_gen: Arc<EpochGenerator>,
    pub ticket_signer: Arc<TicketSigner>,
}

impl ControllerState {
    pub fn new(signing_key: &[u8]) -> Self {
        Self {
            runs: Arc::new(RwLock::new(RunStore::new())),
            tasks: Arc::new(RwLock::new(TaskQueue::new())),
            registry: Arc::new(RwLock::new(AgentRegistry::new())),
            watchers: Arc::new(RwLock::new(RunWatchers::new())),
            previews: Arc::new(RwLock::new(PreviewStore::new())),
            epoch_gen: Arc::new(EpochGenerator::new()),
            ticket_signer: Arc::new(TicketSigner::new(signing_key)),
        }
    }
}
```

- [ ] **Step 2: Add module to lib.rs**

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-controller`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): add shared ControllerState container"
```

### Task 13: Implement PipelineService gRPC handlers

**Files:**
- Create: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Implement PipelineService**

Implement the `PipelineService` trait generated by tonic for the 5 RPCs:
- `submit_pipeline` — validate YAML (parse to check structure), reject sqlite backend in distributed mode, check idempotency_key for duplicates, create run, enqueue task, return run_id
- `get_run` — look up run, include preview access if available
- `watch_run` — subscribe to watcher broadcast, stream events
- `cancel_run` — if Pending: remove from queue; if Running: mark CANCELLING (delivered via heartbeat)
- `list_runs` — delegate to RunStore

Each handler takes `&self` (which holds `Arc<ControllerState>`), acquires the appropriate lock, and performs the operation.

Key validation in `submit_pipeline`: parse the YAML, check if `state.backend` is `sqlite` → reject with error message about distributed mode requiring postgres.

- [ ] **Step 2: Write tests for PipelineService handlers**

Test in-process (create `ControllerState`, call handler methods directly):

```rust
#[tokio::test]
async fn test_submit_pipeline_returns_run_id() { ... }

#[tokio::test]
async fn test_submit_pipeline_rejects_sqlite_backend() { ... }

#[tokio::test]
async fn test_submit_pipeline_idempotency_key_dedup() { ... }

#[tokio::test]
async fn test_get_run_returns_status() { ... }

#[tokio::test]
async fn test_cancel_pending_run_removes_from_queue() { ... }

#[tokio::test]
async fn test_cancel_completed_run_returns_not_accepted() { ... }
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p rapidbyte-controller -- pipeline_service`
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement PipelineService gRPC handlers"
```

### Task 14: Implement AgentService gRPC handlers

**Files:**
- Create: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Implement AgentService**

Implement the `AgentService` trait for 5 RPCs:
- `register_agent` — generate agent_id (uuid v4), add to registry, return agent_id
- `heartbeat` — update registry, validate active leases, return cancel directives for any runs marked CANCELLING
- `poll_task` — delegate to scheduler.poll with agent_id, use tokio::time::timeout for long-poll (wait_seconds). Use `tokio::sync::Notify` to wake pollers when a new task is enqueued.
- `report_progress` — validate lease epoch, forward event to watcher broadcast
- `complete_task` — validate lease epoch via scheduler.complete, update run state, store preview if provided. **Implement retry safety policy** from spec Section 8:
  - If `safe_to_retry=true` and `retryable=true`: auto-requeue allowed (new attempt, new lease epoch)
  - If `safe_to_retry=false` OR `commit_state` is `after_commit_unknown` or `after_commit_confirmed`: **no auto-retry**, transition to FAILED
  - If lease timeout (no agent report): **no auto-retry**, transition to TIMED_OUT (operator must manually requeue)

The long-poll in `poll_task` needs a `Notify` on the TaskQueue. When `enqueue` is called, notify waiters so they wake up and check for tasks. Add `task_notify: Arc<tokio::sync::Notify>` to `ControllerState`.

- [ ] **Step 2: Write tests for AgentService handlers**

Test these in-process (create `ControllerState`, call handler methods directly):

```rust
#[tokio::test]
async fn test_register_agent_returns_uuid() { ... }

#[tokio::test]
async fn test_poll_task_returns_pending_task() { ... }

#[tokio::test]
async fn test_complete_task_with_stale_epoch_returns_unacknowledged() { ... }

#[tokio::test]
async fn test_complete_task_safe_to_retry_requeues() { ... }

#[tokio::test]
async fn test_complete_task_unsafe_does_not_requeue() { ... }

#[tokio::test]
async fn test_heartbeat_returns_cancel_directive_for_cancelling_run() { ... }
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p rapidbyte-controller -- agent_service`
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement AgentService gRPC handlers with retry safety"
```

### Task 15: Implement controller server startup

**Files:**
- Create: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

- [ ] **Step 1: Implement server module**

```rust
//! gRPC server startup and wiring.

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Server;
use tracing::info;

use crate::agent_service::AgentServiceImpl;
use crate::pipeline_service::PipelineServiceImpl;
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;
use crate::state::ControllerState;

/// Configuration for the controller server.
pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub signing_key: Vec<u8>,
    pub agent_reap_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub lease_check_interval: Duration,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".parse().unwrap(),
            signing_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            agent_reap_interval: Duration::from_secs(15),
            agent_reap_timeout: Duration::from_secs(60),
            lease_check_interval: Duration::from_secs(10),
        }
    }
}

/// Start the controller gRPC server.
pub async fn run(config: ControllerConfig) -> anyhow::Result<()> {
    let state = ControllerState::new(&config.signing_key);

    // Background task: reap dead agents
    let reap_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.agent_reap_interval);
        loop {
            interval.tick().await;
            let dead = reap_state.registry.write().await.reap_dead(config.agent_reap_timeout);
            for agent_id in &dead {
                info!(agent_id, "Reaped dead agent");
            }
        }
    });

    // Background task: expire leases
    let lease_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.lease_check_interval);
        loop {
            interval.tick().await;
            let expired = lease_state.tasks.write().await.expire_leases();
            for (task_id, run_id) in &expired {
                info!(task_id, run_id, "Task lease expired");
                // Transition run to timed out
                let _ = lease_state.runs.write().await.transition(run_id, /* TimedOut */);
            }
        }
    });

    let pipeline_svc = PipelineServiceServer::new(PipelineServiceImpl::new(state.clone()));
    let agent_svc = AgentServiceServer::new(AgentServiceImpl::new(state));

    info!(addr = %config.listen_addr, "Controller listening");

    Server::builder()
        .add_service(pipeline_svc)
        .add_service(agent_svc)
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
```

- [ ] **Step 2: Add public re-exports in lib.rs**

```rust
pub use server::{run, ControllerConfig};
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-controller`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): implement gRPC server startup with background tasks"
```

---

## Chunk 4: Agent Core — Worker Loop, Executor, Progress

### Task 16: Implement agent executor module

**Files:**
- Create: `crates/rapidbyte-agent/src/executor.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Write executor module**

Wraps `engine::run_pipeline` to produce `TaskOutcome` + `TaskError` + `TaskMetrics` from proto types:

```rust
//! Task execution wrapper around engine::run_pipeline.

use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use rapidbyte_engine::progress::ProgressEvent;
use rapidbyte_engine::{orchestrator, PipelineError};
use tokio::sync::mpsc;

/// Result of executing a task on the agent.
pub struct TaskExecutionResult {
    pub outcome: TaskOutcomeKind,
    pub metrics: TaskMetrics,
    pub dry_run_result: Option<rapidbyte_engine::DryRunResult>,
    pub backend_run_id: Option<i64>,
}

pub enum TaskOutcomeKind {
    Completed,
    Failed(TaskErrorInfo),
    Cancelled,
}

pub struct TaskErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub safe_to_retry: bool,
    pub commit_state: String,
}

pub struct TaskMetrics {
    pub records_processed: u64,
    pub bytes_processed: u64,
    pub elapsed_seconds: f64,
    pub cursors_advanced: u64,
}

/// Execute a pipeline task.
///
/// Parses the YAML, runs the pipeline, and returns structured results.
pub async fn execute_task(
    pipeline_yaml: &[u8],
    dry_run: bool,
    limit: Option<u64>,
    progress_tx: Option<mpsc::UnboundedSender<ProgressEvent>>,
) -> TaskExecutionResult {
    let yaml_str = match std::str::from_utf8(pipeline_yaml) {
        Ok(s) => s,
        Err(e) => {
            return TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "INVALID_YAML".into(),
                    message: format!("Pipeline YAML is not valid UTF-8: {e}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                metrics: TaskMetrics::zero(),
                dry_run_result: None,
                backend_run_id: None,
            };
        }
    };

    let config = match parser::parse_pipeline_str(yaml_str) {
        Ok(c) => c,
        Err(e) => {
            return TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "PARSE_FAILED".into(),
                    message: format!("{e:#}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                metrics: TaskMetrics::zero(),
                dry_run_result: None,
                backend_run_id: None,
            };
        }
    };

    if let Err(e) = validator::validate_pipeline(&config) {
        return TaskExecutionResult {
            outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                code: "VALIDATION_FAILED".into(),
                message: format!("{e:#}"),
                retryable: false,
                safe_to_retry: false,
                commit_state: "before_commit".into(),
            }),
            metrics: TaskMetrics::zero(),
            dry_run_result: None,
            backend_run_id: None,
        };
    }

    let options = ExecutionOptions { dry_run, limit };
    let start = std::time::Instant::now();

    match orchestrator::run_pipeline(&config, &options, progress_tx).await {
        Ok(outcome) => {
            let elapsed = start.elapsed().as_secs_f64();
            match outcome {
                PipelineOutcome::Run(result) => TaskExecutionResult {
                    outcome: TaskOutcomeKind::Completed,
                    metrics: TaskMetrics {
                        records_processed: result.counts.records_written,
                        bytes_processed: result.counts.bytes_written,
                        elapsed_seconds: elapsed,
                        cursors_advanced: 0, // TODO: extract from result
                    },
                    dry_run_result: None,
                    backend_run_id: None,
                },
                PipelineOutcome::DryRun(dr) => TaskExecutionResult {
                    outcome: TaskOutcomeKind::Completed,
                    metrics: TaskMetrics {
                        records_processed: dr.streams.iter().map(|s| s.total_rows).sum(),
                        bytes_processed: dr.streams.iter().map(|s| s.total_bytes).sum(),
                        elapsed_seconds: elapsed,
                        cursors_advanced: 0,
                    },
                    dry_run_result: Some(dr),
                    backend_run_id: None,
                },
            }
        }
        Err(e) => {
            let elapsed = start.elapsed().as_secs_f64();
            let error_info = match &e {
                PipelineError::Plugin(pe) => TaskErrorInfo {
                    code: pe.code.clone(),
                    message: pe.message.clone(),
                    retryable: pe.retryable,
                    safe_to_retry: pe.safe_to_retry,
                    commit_state: pe
                        .commit_state
                        .map(|cs| format!("{cs:?}").to_lowercase())
                        .unwrap_or_else(|| "before_commit".into()),
                },
                PipelineError::Infrastructure(e) => TaskErrorInfo {
                    code: "INFRASTRUCTURE".into(),
                    message: format!("{e:#}"),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                },
            };
            TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(error_info),
                metrics: TaskMetrics {
                    records_processed: 0,
                    bytes_processed: 0,
                    elapsed_seconds: elapsed,
                    cursors_advanced: 0,
                },
                dry_run_result: None,
                backend_run_id: None,
            }
        }
    }
}

impl TaskMetrics {
    fn zero() -> Self {
        Self {
            records_processed: 0,
            bytes_processed: 0,
            elapsed_seconds: 0.0,
            cursors_advanced: 0,
        }
    }
}
```

Note: This requires `parse_pipeline_str` — a variant of `parse_pipeline` that takes a `&str` instead of a `&Path`. If it doesn't exist, add it to `crates/rapidbyte-engine/src/config/parser.rs` as a thin wrapper that deserializes from string instead of reading a file.

- [ ] **Step 2: Check if parse_pipeline_str exists**

Run: `grep -n "parse_pipeline_str\|parse_pipeline" crates/rapidbyte-engine/src/config/parser.rs | head -10`

If it doesn't exist, add it:

```rust
pub fn parse_pipeline_str(yaml: &str) -> anyhow::Result<PipelineConfig> {
    let config: PipelineConfig = serde_yaml::from_str(yaml)
        .context("Failed to parse pipeline YAML")?;
    Ok(config)
}
```

- [ ] **Step 3: Add module to lib.rs**

Add `pub mod executor;` to agent's lib.rs.

- [ ] **Step 4: Write unit tests for executor error paths**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_utf8_returns_failed() {
        let result = execute_task(&[0xFF, 0xFE], false, None, None).await;
        assert!(matches!(result.outcome, TaskOutcomeKind::Failed(_)));
        if let TaskOutcomeKind::Failed(info) = &result.outcome {
            assert_eq!(info.code, "INVALID_YAML");
            assert!(!info.retryable);
        }
    }

    #[tokio::test]
    async fn test_invalid_yaml_returns_failed() {
        let result = execute_task(b"not: [valid: yaml", false, None, None).await;
        assert!(matches!(result.outcome, TaskOutcomeKind::Failed(_)));
        if let TaskOutcomeKind::Failed(info) = &result.outcome {
            assert_eq!(info.code, "PARSE_FAILED");
        }
    }

    #[tokio::test]
    async fn test_zero_metrics_on_early_failure() {
        let result = execute_task(&[0xFF], false, None, None).await;
        assert_eq!(result.metrics.records_processed, 0);
        assert_eq!(result.metrics.bytes_processed, 0);
        assert_eq!(result.metrics.elapsed_seconds, 0.0);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-agent -- executor`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-agent/src/ crates/rapidbyte-engine/src/
git commit -m "feat(agent): implement task executor wrapping engine::run_pipeline"
```

### Task 17: Implement agent progress forwarding

**Files:**
- Create: `crates/rapidbyte-agent/src/progress.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Write progress forwarder**

Bridges `mpsc::UnboundedReceiver<ProgressEvent>` → gRPC `ReportProgress` calls:

```rust
//! ProgressEvent → ReportProgress gRPC forwarding.

use rapidbyte_engine::progress::ProgressEvent;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::warn;

use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::{ProgressUpdate, ReportProgressRequest};

/// Forward engine progress events to the controller.
///
/// Runs until the receiver is closed (engine finished).
pub async fn forward_progress(
    mut rx: mpsc::UnboundedReceiver<ProgressEvent>,
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    task_id: String,
    lease_epoch: u64,
) {
    while let Some(event) = rx.recv().await {
        let progress = match &event {
            ProgressEvent::BatchEmitted { bytes } => Some(ProgressUpdate {
                stream: String::new(),
                phase: "running".into(),
                records: 0,
                bytes: *bytes,
            }),
            ProgressEvent::StreamCompleted { stream } => Some(ProgressUpdate {
                stream: stream.clone(),
                phase: "completed".into(),
                records: 0,
                bytes: 0,
            }),
            ProgressEvent::PhaseChange { phase } => Some(ProgressUpdate {
                stream: String::new(),
                phase: format!("{phase:?}").to_lowercase(),
                records: 0,
                bytes: 0,
            }),
            ProgressEvent::Retry { .. } => None,
        };

        if let Some(progress) = progress {
            let req = ReportProgressRequest {
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                lease_epoch,
                progress: Some(progress),
            };
            if let Err(e) = client.report_progress(req).await {
                warn!(error = %e, "Failed to report progress to controller");
            }
        }
    }
}
```

- [ ] **Step 2: Add module to lib.rs**

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-agent`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/
git commit -m "feat(agent): implement progress event forwarding to controller"
```

### Task 18: Implement agent worker loop

**Files:**
- Create: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Write worker module**

The main agent lifecycle: register → poll → execute → complete → repeat. Plus a background heartbeat task.

```rust
//! Main agent loop: register, poll tasks, execute, report.

use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::executor::{self, TaskOutcomeKind};
use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::*;

pub struct AgentConfig {
    pub controller_url: String,
    pub flight_listen: String,
    pub flight_advertise: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller_url: "http://[::]:9090".into(),
            flight_listen: "[::]:9091".into(),
            flight_advertise: "localhost:9091".into(),
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
        }
    }
}

pub async fn run(config: AgentConfig) -> anyhow::Result<()> {
    let channel = Channel::from_shared(config.controller_url.clone())?
        .connect()
        .await?;
    let mut client = AgentServiceClient::new(channel.clone());

    // Register
    let resp = client
        .register_agent(RegisterAgentRequest {
            max_tasks: config.max_tasks,
            flight_advertise_endpoint: config.flight_advertise.clone(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        })
        .await?;
    let agent_id = resp.into_inner().agent_id;
    info!(agent_id, "Registered with controller");

    // Shared active lease tracking
    let active_leases: ActiveLeaseMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<(String, u64)>();

    // Spawn heartbeat loop
    let hb_client = AgentServiceClient::new(channel.clone());
    let hb_agent_id = agent_id.clone();
    let hb_interval = config.heartbeat_interval;
    let hb_leases = active_leases.clone();
    tokio::spawn(async move {
        heartbeat_loop(hb_client, hb_agent_id, hb_interval, hb_leases, cancel_tx).await;
    });

    // Main task poll loop
    loop {
        let resp = client
            .poll_task(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: config.poll_wait_seconds,
            })
            .await?;

        let task = match resp.into_inner().result {
            Some(poll_task_response::Result::Task(t)) => t,
            Some(poll_task_response::Result::NoTask(_)) | None => continue,
        };

        info!(
            task_id = task.task_id,
            run_id = task.run_id,
            attempt = task.attempt,
            lease_epoch = task.lease_epoch,
            "Received task"
        );

        // Track active lease
        active_leases.write().await.insert(task.task_id.clone(), task.lease_epoch);

        let exec_opts = task.execution.as_ref();
        let dry_run = exec_opts.map_or(false, |e| e.dry_run);
        let limit = exec_opts.and_then(|e| e.limit);

        // Set up cancellation token (Phase 3)
        let cancel_token = tokio_util::sync::CancellationToken::new();
        // Spawn cancel listener
        let ct = cancel_token.clone();
        let task_id_for_cancel = task.task_id.clone();
        let task_epoch_for_cancel = task.lease_epoch;
        tokio::spawn(async move {
            while let Some((tid, epoch)) = cancel_rx.recv().await {
                if tid == task_id_for_cancel && epoch == task_epoch_for_cancel {
                    ct.cancel();
                    break;
                }
            }
        });

        // Set up progress forwarding
        let (progress_tx, progress_rx) = mpsc::unbounded_channel();
        let progress_client = AgentServiceClient::new(channel.clone());
        let progress_handle = tokio::spawn(crate::progress::forward_progress(
            progress_rx,
            progress_client,
            agent_id.clone(),
            task.task_id.clone(),
            task.lease_epoch,
        ));

        // Execute
        let result = executor::execute_task(
            &task.pipeline_yaml_utf8,
            dry_run,
            limit,
            Some(progress_tx),
        )
        .await;

        // Wait for progress forwarding to finish
        let _ = progress_handle.await;

        // Build completion request
        let (outcome, error) = match &result.outcome {
            TaskOutcomeKind::Completed => (TaskOutcome::Completed as i32, None),
            TaskOutcomeKind::Failed(info) => (
                TaskOutcome::Failed as i32,
                Some(TaskError {
                    code: info.code.clone(),
                    message: info.message.clone(),
                    retryable: info.retryable,
                    safe_to_retry: info.safe_to_retry,
                    commit_state: info.commit_state.clone(),
                }),
            ),
            TaskOutcomeKind::Cancelled => (TaskOutcome::Cancelled as i32, None),
        };

        let complete_resp = client
            .complete_task(CompleteTaskRequest {
                agent_id: agent_id.clone(),
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome,
                error,
                metrics: Some(crate::proto::rapidbyte::v1::TaskMetrics {
                    records_processed: result.metrics.records_processed,
                    bytes_processed: result.metrics.bytes_processed,
                    elapsed_seconds: result.metrics.elapsed_seconds,
                    cursors_advanced: result.metrics.cursors_advanced,
                }),
                preview: None, // Phase 4 adds preview
                backend_run_id: result.backend_run_id.unwrap_or(0),
            })
            .await;

        // Remove active lease tracking
        active_leases.write().await.remove(&task.task_id);

        match complete_resp {
            Ok(resp) if !resp.into_inner().acknowledged => {
                warn!(task_id = task.task_id, "Stale lease — completion rejected");
            }
            Err(e) => {
                error!(task_id = task.task_id, error = %e, "Failed to report completion");
            }
            _ => {
                info!(task_id = task.task_id, "Task completed");
            }
        }
    }
}
```

Note: Add `tokio-util = { version = "0.7", features = ["rt"] }` to agent `Cargo.toml` and workspace dependencies for `CancellationToken`. Also add `use std::collections::HashMap;` and `use std::sync::Arc;` to worker imports.

/// Shared state for tracking active leases across worker and heartbeat.
type ActiveLeaseMap = Arc<tokio::sync::RwLock<HashMap<String, u64>>>; // task_id -> lease_epoch

async fn heartbeat_loop(
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    interval: Duration,
    active_leases: ActiveLeaseMap,
    cancel_tx: mpsc::UnboundedSender<(String, u64)>, // (task_id, lease_epoch) to cancel
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let leases: Vec<ActiveLease> = active_leases
            .read()
            .await
            .iter()
            .map(|(task_id, epoch)| ActiveLease {
                task_id: task_id.clone(),
                lease_epoch: *epoch,
            })
            .collect();
        let active_count = leases.len() as u32;
        let resp = client
            .heartbeat(HeartbeatRequest {
                agent_id: agent_id.clone(),
                active_leases: leases,
                active_tasks: active_count,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            })
            .await;
        match resp {
            Ok(resp) => {
                // Process cancel directives
                for directive in resp.into_inner().directives {
                    if let Some(agent_directive::Directive::CancelTask(cancel)) = directive.directive {
                        let _ = cancel_tx.send((cancel.task_id, cancel.lease_epoch));
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Heartbeat failed");
            }
        }
    }
}
```

Note: The worker loop should handle graceful shutdown via `tokio::signal::ctrl_c()`. Wrap the main loop in `tokio::select!` with a shutdown signal:

```rust
let shutdown = tokio::signal::ctrl_c();
tokio::pin!(shutdown);

loop {
    tokio::select! {
        _ = &mut shutdown => {
            info!("Shutdown signal received, deregistering...");
            // Best-effort deregister
            let _ = client.deregister_agent(/* ... */).await;
            break;
        }
        resp = client.poll_task(/* ... */) => {
            // ... existing task handling
        }
    }
}
```

Note: `DeregisterAgent` RPC is not in the current proto. For v1.1, the agent simply disconnects and the controller reaps it via heartbeat timeout. The graceful shutdown just stops polling and lets in-flight tasks complete.

- [ ] **Step 2: Add module to lib.rs and public re-exports**

```rust
pub mod worker;
pub use worker::{run, AgentConfig};
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-agent`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/
git commit -m "feat(agent): implement main worker loop with register, poll, execute, complete"
```

---

## Chunk 5: CLI Integration

### Task 19: Add --controller flag and new subcommands to CLI

**Files:**
- Modify: `crates/rapidbyte-cli/Cargo.toml`
- Modify: `crates/rapidbyte-cli/src/main.rs`

- [ ] **Step 1: Add dependencies to CLI Cargo.toml**

Add to `[dependencies]`:
```toml
rapidbyte-controller = { path = "../rapidbyte-controller" }
rapidbyte-agent = { path = "../rapidbyte-agent" }
tonic = { workspace = true }
prost-types = { workspace = true }
arrow-flight = { workspace = true }
uuid = { workspace = true }
```

- [ ] **Step 2: Add --controller global flag and new subcommands**

In `crates/rapidbyte-cli/src/main.rs`, add to `Cli` struct:

```rust
/// Controller gRPC endpoint (enables distributed mode)
#[arg(long, global = true, env = "RAPIDBYTE_CONTROLLER")]
controller: Option<String>,
```

Add to `Commands` enum:

```rust
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
    #[arg(long, default_value = "[::]:9091")]
    flight_listen: String,
    /// Flight endpoint advertised to clients (must be reachable)
    #[arg(long)]
    flight_advertise: String,
    /// Maximum concurrent tasks
    #[arg(long, default_value = "1")]
    max_tasks: u32,
},
```

Add controller URL resolution that checks (in priority order):
1. `--controller` CLI flag
2. `RAPIDBYTE_CONTROLLER` env var (handled by clap `env` attribute)
3. `~/.rapidbyte/config.yaml` → `controller.url` field

```rust
fn resolve_controller_url(cli_flag: Option<&str>) -> Option<String> {
    if let Some(url) = cli_flag {
        return Some(url.to_string());
    }
    // clap handles env var via `env = "RAPIDBYTE_CONTROLLER"`
    // Fallback: config file
    let home = std::env::var("HOME").ok()?;
    let config_path = std::path::PathBuf::from(home).join(".rapidbyte").join("config.yaml");
    if config_path.exists() {
        let contents = std::fs::read_to_string(&config_path).ok()?;
        let val: serde_yaml::Value = serde_yaml::from_str(&contents).ok()?;
        val.get("controller")?.get("url")?.as_str().map(String::from)
    } else {
        None
    }
}
```

Update the match in `main()`:
```rust
Commands::Controller { listen } => commands::controller::execute(&listen).await,
Commands::Agent {
    controller,
    flight_listen,
    flight_advertise,
    max_tasks,
} => commands::agent::execute(&controller, &flight_listen, &flight_advertise, max_tasks).await,
```

And update `Commands::Run` dispatch to pass `cli.controller.as_deref()`.

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/
git commit -m "feat(cli): add --controller flag and controller/agent subcommands"
```

### Task 20: Implement CLI controller command

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Write controller command**

```rust
//! Controller server subcommand.

use anyhow::Result;

pub async fn execute(listen: &str) -> Result<()> {
    let addr = listen.parse().map_err(|e| anyhow::anyhow!("Invalid listen address: {e}"))?;
    let config = rapidbyte_controller::ControllerConfig {
        listen_addr: addr,
        ..Default::default()
    };
    rapidbyte_controller::run(config).await
}
```

- [ ] **Step 2: Add module to commands/mod.rs**

Add `pub mod controller;` to commands/mod.rs.

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/
git commit -m "feat(cli): implement controller subcommand"
```

### Task 21: Implement CLI agent command

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Write agent command**

```rust
//! Agent worker subcommand.

use anyhow::Result;
use std::time::Duration;

pub async fn execute(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
) -> Result<()> {
    let config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        flight_listen: flight_listen.into(),
        flight_advertise: flight_advertise.into(),
        max_tasks,
        heartbeat_interval: Duration::from_secs(10),
        poll_wait_seconds: 30,
    };
    rapidbyte_agent::run(config).await
}
```

- [ ] **Step 2: Add module to commands/mod.rs**

Add `pub mod agent;` to commands/mod.rs.

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/
git commit -m "feat(cli): implement agent subcommand"
```

### Task 22: Implement distributed run command (CLI → controller)

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`

- [ ] **Step 1: Write distributed_run module**

Handles `rapidbyte run pipeline.yaml` when `--controller` is set:
1. Read pipeline YAML from disk
2. Connect to controller via gRPC
3. Call `SubmitPipeline` with YAML bytes + ExecutionOptions
4. Call `WatchRun` to stream progress events
5. Print progress (reuse existing spinner logic where possible)
6. On completion, print summary
7. On failure, print error

```rust
//! Distributed pipeline execution via controller.

use std::path::Path;

use anyhow::{Context, Result};
use tonic::transport::Channel;

use crate::Verbosity;

// Use the controller's proto types via the controller crate
use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::*;

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
) -> Result<()> {
    let yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;

    let channel = Channel::from_shared(controller_url.to_string())?
        .connect()
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = PipelineServiceClient::new(channel);

    // Submit
    let resp = client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml_utf8: yaml,
            execution: Some(ExecutionOptions {
                dry_run,
                limit,
            }),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
        })
        .await?;
    let run_id = resp.into_inner().run_id;

    if verbosity != Verbosity::Quiet {
        eprintln!("Submitted run: {run_id}");
    }

    // Watch
    let mut stream = client
        .watch_run(WatchRunRequest {
            run_id: run_id.clone(),
        })
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        if let Some(evt) = event.event {
            match evt {
                run_event::Event::Progress(p) => {
                    if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
                        eprintln!("  [{}] {} — {} records, {} bytes", p.stream, p.phase, p.records, p.bytes);
                    }
                }
                run_event::Event::Completed(c) => {
                    if verbosity != Verbosity::Quiet {
                        eprintln!(
                            "Completed: {} records, {} bytes in {:.1}s",
                            c.total_records, c.total_bytes, c.elapsed_seconds,
                        );
                    }
                    return Ok(());
                }
                run_event::Event::Failed(f) => {
                    let msg = f.error.map(|e| e.message).unwrap_or_default();
                    anyhow::bail!("Run failed (attempt {}): {msg}", f.attempt);
                }
                run_event::Event::Cancelled(_) => {
                    anyhow::bail!("Run was cancelled");
                }
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Add module to commands/mod.rs**

Add `pub mod distributed_run;` to commands/mod.rs.

- [ ] **Step 3: Update run.rs to route based on --controller**

Modify `crates/rapidbyte-cli/src/commands/run.rs` execute function signature to accept `controller: Option<&str>`:

```rust
pub async fn execute(
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
    controller: Option<&str>,
) -> Result<()> {
    // If controller is set, route to distributed mode
    if let Some(url) = controller {
        return super::distributed_run::execute(url, pipeline_path, dry_run, limit, verbosity).await;
    }

    // ... rest of existing code unchanged
```

Update `main.rs` to pass `cli.controller.as_deref()` to the run command.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: success

- [ ] **Step 5: Run existing tests**

Run: `cargo test --workspace`
Expected: all pass (no regressions)

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-cli/
git commit -m "feat(cli): implement distributed run via controller with WatchRun streaming"
```

---

## Chunk 6: Phase 3 — Cancellation

### Task 23: Add cancellation support

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/run_state.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

- [ ] **Step 1: Add CANCELLING internal state to RunStore**

Add a `Cancelling` variant to the run state enum. `CancelRun` sets this. Heartbeat response checks for it and emits `CancelTask` directive.

- [ ] **Step 2: Implement CancelRun in PipelineService**

- If run is Pending: remove from task queue, transition to Cancelled
- If run is Assigned/Running: set to Cancelling (delivered via heartbeat)
- If run is already terminal: return `accepted: false`

- [ ] **Step 3: Update heartbeat in AgentService**

Check if any of the agent's active tasks have runs in Cancelling state. If so, include `CancelTask` directive in response.

- [ ] **Step 4: Handle cancel directive in agent worker**

In the heartbeat loop, check response directives. If `CancelTask` is received, use a `tokio::CancellationToken` to signal the executor. For v1.1: the executor can check `token.is_cancelled()` between major phases. Add `CancellationToken` parameter to `execute_task`.

- [ ] **Step 5: Add tests for cancel flows**

Test: cancel of pending run removes from queue
Test: cancel of running run delivers directive on next heartbeat
Test: cancel of completed run returns accepted=false

- [ ] **Step 6: Run tests**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-controller/ crates/rapidbyte-agent/
git commit -m "feat: implement cooperative cancellation via heartbeat directives"
```

---

## Chunk 7: Phase 4 — Dry-Run Preview over Flight

### Task 24: Implement preview spool

**Files:**
- Create: `crates/rapidbyte-agent/src/spool.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Write spool module with tests**

In-memory spool that holds `DryRunResult` with TTL:

```rust
//! Preview spool — holds dry-run results for Flight replay.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rapidbyte_engine::DryRunResult;

pub struct PreviewSpool {
    entries: HashMap<String, SpoolEntry>,
    default_ttl: Duration,
}

struct SpoolEntry {
    result: DryRunResult,
    created_at: Instant,
    ttl: Duration,
}

impl PreviewSpool {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            default_ttl,
        }
    }

    pub fn store(&mut self, task_id: String, result: DryRunResult) {
        self.entries.insert(task_id, SpoolEntry {
            result,
            created_at: Instant::now(),
            ttl: self.default_ttl,
        });
    }

    pub fn get(&self, task_id: &str) -> Option<&DryRunResult> {
        let entry = self.entries.get(task_id)?;
        if entry.created_at.elapsed() < entry.ttl {
            Some(&entry.result)
        } else {
            None
        }
    }

    pub fn cleanup_expired(&mut self) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, e| e.created_at.elapsed() < e.ttl);
        before - self.entries.len()
    }
}
```

Tests: store + get, expired entries return None, cleanup removes expired.

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-agent -- spool`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-agent/src/
git commit -m "feat(agent): implement preview spool with TTL cleanup"
```

### Task 25: Implement ticket validation in agent

**Files:**
- Create: `crates/rapidbyte-agent/src/ticket.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Write ticket module**

Mirror the controller's `TicketSigner` verification logic. The agent receives the signing key during registration (or via shared config). For v1.1, the agent needs `TicketSigner::verify` to validate incoming Flight tickets.

Since both controller and agent need ticket signing/verification, extract a shared `ticket` module. For now, duplicate the verification logic in the agent crate (the signing key is shared during `RegisterAgent`).

Tests: verify valid ticket, reject tampered ticket, reject expired ticket.

- [ ] **Step 2: Run tests and commit**

```bash
git add crates/rapidbyte-agent/src/
git commit -m "feat(agent): implement ticket validation for Flight preview access"
```

### Task 26: Implement Arrow Flight server on agent

**Files:**
- Create: `crates/rapidbyte-agent/src/flight.rs`
- Modify: `crates/rapidbyte-agent/src/lib.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

- [ ] **Step 1: Write Flight server**

Implement `arrow_flight::flight_service_server::FlightService` trait for the agent:
- `get_flight_info` — validate ticket, return schema + row count from spool
- `do_get` — validate ticket, stream RecordBatches from spool using `arrow_flight::utils::batches_to_flight_data`

Wire the Flight server to start in the worker's `run()` function before registering with the controller.

- [ ] **Step 2: Update worker to start Flight server and store previews**

In `worker.rs`, start the Flight server on `config.flight_listen`. After executing a dry-run task, store the `DryRunResult` in the spool. Include `PreviewAccess` in the `CompleteTask` request.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-agent`
Expected: success

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/
git commit -m "feat(agent): implement Arrow Flight server for preview replay"
```

### Task 27: Update CLI distributed_run for preview

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`

- [ ] **Step 1: Add preview fetch after dry-run completion**

After WatchRun reports completion for a dry-run, call `GetRun` to get `PreviewAccess`. Connect to the agent's Flight endpoint, call `DoGet` with the ticket, receive RecordBatches, and display them using the existing `pretty_format_batches` logic.

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: success

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/
git commit -m "feat(cli): fetch and display dry-run preview via Flight in distributed mode"
```

---

## Chunk 8: Phase 5 — Production Hardening

**Deferred from Phase 5 (noted for v1.2):**
- Persistent controller metadata store with WAL / Postgres — spec says "V1.1: in-memory with WAL for crash recovery" but WAL adds complexity. For the initial implementation, in-memory only. Postgres-backed metadata is a v2 item per the spec.
- Secret redaction in pipeline YAML transit — requires identifying secret fields in arbitrary plugin configs. Deferred to a follow-up where a `secrets` field or annotation mechanism is added to the config schema.

### Task 28: Add Tower middleware for auth and tracing

**Files:**
- Create: `crates/rapidbyte-controller/src/middleware.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`

- [ ] **Step 1: Write middleware module**

Add Tower layers:
- `BearerAuthLayer` — extracts `Authorization: Bearer <token>` from metadata, validates against configured tokens
- Tracing layer — logs request/response with `tracing::Span`
- Deadline layer — enforces request timeouts

```rust
//! Tower middleware for auth, tracing, and deadlines.

use std::task::{Context, Poll};
use tonic::{Request, Status};
use tower::{Layer, Service};

/// Bearer token authentication layer.
pub struct BearerAuthLayer {
    valid_tokens: Vec<String>,
}

impl BearerAuthLayer {
    pub fn new(tokens: Vec<String>) -> Self {
        Self { valid_tokens: tokens }
    }
}

// ... Layer + Service impl that checks authorization metadata
```

For v1.1, the auth layer is optional (enabled via config). When no tokens are configured, auth is bypassed.

Tests:
- Request with valid bearer token passes through
- Request with invalid token is rejected with UNAUTHENTICATED
- Request with no token when auth is configured is rejected
- Request with no token when auth is disabled passes through

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller -- middleware`
Expected: all pass

- [ ] **Step 3: Wire middleware into server.rs**

Add `.layer(...)` calls to the tonic `Server::builder()`.

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): add Tower middleware for auth, tracing, and deadlines"
```

### Task 29: Add SQLite rejection for distributed mode

**Files:**
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`

- [ ] **Step 1: Enforce sqlite rejection in SubmitPipeline**

In the `submit_pipeline` handler, after parsing the YAML, check if `state.backend` is `sqlite`. If so, return a gRPC `INVALID_ARGUMENT` status with a clear message:

```
Distributed mode requires a shared state backend (postgres).
SQLite is a local file and would be unreachable after agent reassignment.
Use --allow-unsafe-local-state to override.
```

Check for `allow_unsafe_local_state` in the execution options (add a bool field if needed).

- [ ] **Step 2: Add test**

Test that SubmitPipeline with sqlite backend returns INVALID_ARGUMENT.

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): reject SQLite state backend in distributed mode"
```

### Task 30: Add plugin bundle hash enforcement

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/registry.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`

- [ ] **Step 1: Store bundle hash during registration**

AgentRegistry already stores `plugin_bundle_hash`. During `poll_task`, the scheduler can optionally verify that the agent's bundle hash matches expected plugins for the pipeline.

For v1.1: log a warning if agents have different bundle hashes. Don't block task assignment — just warn.

- [ ] **Step 2: Commit**

```bash
git add crates/rapidbyte-controller/src/
git commit -m "feat(controller): log bundle hash mismatches across agent pool"
```

### Task 31: Role-specific runtime hardening

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

- [ ] **Step 1: Controller hardening**

Controller refuses to resolve WASM paths — it doesn't need `rapidbyte-runtime` plugin resolution. Already the case since controller crate doesn't depend on runtime directly for WASM ops.

- [ ] **Step 2: Agent hardening**

Agent validates plugin inventory at startup by calling `resolve_plugins` on a test config or listing the plugins directory. Log available plugins.

- [ ] **Step 3: CLI hardening**

CLI doesn't start server listeners when not in controller/agent mode. Already the case — `run` command only makes outbound connections.

- [ ] **Step 4: Commit**

```bash
git add crates/
git commit -m "feat: add role-specific runtime hardening for controller, agent, and CLI"
```

---

## Chunk 9: Integration Test + Final Verification

### Task 32: Write integration test for distributed pipeline

**Files:**
- Create: `tests/distributed.rs` or add to existing e2e infrastructure

- [ ] **Step 1: Write an integration test**

Test the full distributed flow:
1. Start controller in-process (spawn a task)
2. Start agent in-process (spawn a task)
3. Submit a pipeline via PipelineService client
4. WatchRun until completion
5. Verify the run completed successfully
6. Verify the agent reported metrics

This requires a test pipeline YAML that uses a state backend (can use sqlite for tests since we control the environment).

- [ ] **Step 2: Run the test**

Run: `cargo test --test distributed`
Expected: pass

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test: add distributed pipeline integration test"
```

### Task 33: Run full workspace tests and lint

- [ ] **Step 1: Run all tests**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no formatting issues

- [ ] **Step 4: Fix any issues found**

- [ ] **Step 5: Final commit if needed**

```bash
git commit -m "chore: fix lint and formatting issues"
```
