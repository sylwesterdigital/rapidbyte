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

pub mod lease;
pub mod pipeline_service;
pub mod preview;
pub mod proto;
pub mod registry;
pub mod run_state;
pub mod scheduler;
pub mod state;
pub mod watcher;
