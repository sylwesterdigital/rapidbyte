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
//! | `executor` | Task execution wrapper around `engine::run_pipeline` |
//! | `progress` | `ProgressEvent` to `ReportProgress` forwarding |
//! | `flight`   | Arrow Flight server for preview replay |
//! | `spool`    | Preview spool (memory + spill-to-disk) |
//! | `ticket`   | Ticket validation |

#![warn(clippy::pedantic)]

pub mod executor;
pub mod flight;
pub mod progress;
pub mod proto;
pub mod spool;
pub mod ticket;
pub mod worker;

pub use worker::{run, AgentConfig};
