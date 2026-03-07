//! Interactive dev shell for exploring data pipelines.
//!
//! Provides a REPL that connects to source connectors, streams data
//! into an in-memory Arrow workspace, and queries with SQL via DataFusion.
//!
//! # Crate structure
//!
//! | Module     | Responsibility |
//! |------------|----------------|
//! | `commands` | Dot-command parser |
//! | `display`  | Table rendering and styled output |
//! | `completer`| Tab completion for dot-commands |
//! | `highlighter`| SQL keyword and dot-command highlighting |
//! | `repl`     | REPL loop, command dispatch, source operations |
//! | `workspace`| Arrow workspace with DataFusion SQL |

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub(crate) mod commands;
pub(crate) mod completer;
pub(crate) mod display;
pub(crate) mod highlighter;
pub(crate) mod repl;
pub mod workspace;

/// Entry point for the dev shell.
///
/// # Errors
///
/// Returns an error if the REPL encounters a fatal error.
pub async fn run() -> anyhow::Result<()> {
    repl::run().await
}
