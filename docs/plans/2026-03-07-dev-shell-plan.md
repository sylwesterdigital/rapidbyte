# `rapidbyte dev` Interactive Dev Shell Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a terminal REPL (`rapidbyte dev`) for exploring sources, streaming data into an in-memory Arrow workspace, and querying with SQL.

**Architecture:** New `rapidbyte-dev` crate with reedline REPL + host-side DataFusion. Source operations go through existing Wasm runtime. Arrow workspace holds streamed data as `RecordBatch`es registered as DataFusion MemTables. CLI crate adds `Dev` subcommand that delegates to `rapidbyte-dev`.

**Tech Stack:** Rust, reedline, datafusion, arrow (prettyprint), rapidbyte-engine, rapidbyte-runtime, rapidbyte-types, console, indicatif

---

### Task 1: Create rapidbyte-dev crate scaffold

**Files:**
- Create: `crates/rapidbyte-dev/Cargo.toml`
- Create: `crates/rapidbyte-dev/src/lib.rs`
- Modify: `Cargo.toml` (workspace root — add member)

**References to read before writing:**
- `Cargo.toml` (workspace root — workspace.dependencies, members list)
- `crates/rapidbyte-engine/Cargo.toml` (dependency style reference)

**Step 1: Create `crates/rapidbyte-dev/Cargo.toml`**

```toml
[package]
name = "rapidbyte-dev"
version.workspace = true
edition.workspace = true

[dependencies]
rapidbyte-engine = { path = "../rapidbyte-engine" }
rapidbyte-runtime = { path = "../rapidbyte-runtime" }
rapidbyte-types = { path = "../rapidbyte-types" }
rapidbyte-state = { path = "../rapidbyte-state" }
tokio = { workspace = true }
arrow = { version = "53", features = ["prettyprint"] }
datafusion = "43"
reedline = "0.38"
anyhow = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
console = { workspace = true }
indicatif = { workspace = true }
```

**Step 2: Create `crates/rapidbyte-dev/src/lib.rs`**

```rust
//! Interactive dev shell for exploring data pipelines.
//!
//! Provides a REPL that connects to source connectors, streams data
//! into an in-memory Arrow workspace, and queries with SQL via DataFusion.

#![warn(clippy::pedantic)]

/// Entry point for the dev shell.
///
/// # Errors
///
/// Returns an error if the REPL setup fails.
pub async fn run() -> anyhow::Result<()> {
    eprintln!("rapidbyte dev shell — not yet implemented");
    Ok(())
}
```

**Step 3: Add crate to workspace root `Cargo.toml`**

Add `"crates/rapidbyte-dev"` to the `members` array (after `rapidbyte-cli`).

**Step 4: Verify it compiles**

Run: `cargo check -p rapidbyte-dev`
Expected: compiles with no errors (may have warnings about unused deps, that's fine)

**Step 5: Commit**

```bash
git add crates/rapidbyte-dev/ Cargo.toml Cargo.lock
git commit -m "feat(dev): scaffold rapidbyte-dev crate

New crate for interactive dev shell. Dependencies: reedline, datafusion,
arrow (prettyprint), engine, runtime, types."
```

---

### Task 2: Wire `Dev` subcommand into CLI

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/dev.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/Cargo.toml`

**References to read before writing:**
- `crates/rapidbyte-cli/src/main.rs` (existing subcommand pattern: `Commands` enum, dispatch in `main()`)
- `crates/rapidbyte-cli/src/commands/mod.rs` (module declarations)
- `crates/rapidbyte-cli/Cargo.toml` (dependency list)

**Step 1: Add rapidbyte-dev dependency to CLI Cargo.toml**

Add to `[dependencies]`:
```toml
rapidbyte-dev = { path = "../rapidbyte-dev" }
```

**Step 2: Create `crates/rapidbyte-cli/src/commands/dev.rs`**

```rust
//! Interactive dev shell subcommand.

use anyhow::Result;

/// Launch the interactive dev shell.
///
/// # Errors
///
/// Returns `Err` if the REPL encounters a fatal error.
pub async fn execute() -> Result<()> {
    rapidbyte_dev::run().await
}
```

**Step 3: Add module to `crates/rapidbyte-cli/src/commands/mod.rs`**

Add `pub mod dev;` after the existing module declarations.

**Step 4: Add `Dev` variant to `Commands` enum in `main.rs`**

Add to the `Commands` enum (after `Scaffold`):
```rust
    /// Launch interactive dev shell
    Dev,
```

Add dispatch arm in the `match cli.command` block (after `Commands::Scaffold`):
```rust
        Commands::Dev => commands::dev::execute().await,
```

**Step 5: Verify it compiles and runs**

Run: `cargo run -- dev`
Expected: prints "rapidbyte dev shell — not yet implemented" and exits 0

**Step 6: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/dev.rs crates/rapidbyte-cli/src/commands/mod.rs crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/Cargo.toml Cargo.lock
git commit -m "feat(cli): add 'dev' subcommand wired to rapidbyte-dev crate"
```

---

### Task 3: Build the ArrowWorkspace

**Files:**
- Create: `crates/rapidbyte-dev/src/workspace.rs`
- Modify: `crates/rapidbyte-dev/src/lib.rs`

**References to read before writing:**
- `connectors/transform-sql/src/transform.rs` (DataFusion `SessionContext`, `MemTable` usage)
- `crates/rapidbyte-engine/src/arrow.rs` (`ipc_to_record_batches` for understanding batch decode)

The workspace is the central data structure: a `HashMap<String, Vec<RecordBatch>>` backed by a DataFusion `SessionContext` for SQL.

**Step 1: Write tests for ArrowWorkspace**

Create `crates/rapidbyte-dev/src/workspace.rs` with tests at the bottom:

```rust
//! In-memory Arrow workspace backed by DataFusion.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::*;

/// Summary of a single table in the workspace.
pub struct TableSummary {
    pub name: String,
    pub rows: u64,
    pub columns: usize,
    pub memory_bytes: u64,
}

/// In-memory Arrow workspace with DataFusion SQL support.
pub struct ArrowWorkspace {
    tables: HashMap<String, (SchemaRef, Vec<RecordBatch>)>,
    session: SessionContext,
}

impl ArrowWorkspace {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            session: SessionContext::new(),
        }
    }

    /// Insert (or replace) a table in the workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion MemTable creation or registration fails.
    pub fn insert(
        &mut self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        // Deregister previous table if exists
        let _ = self.session.deregister_table(name);

        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![batches.clone()],
        )
        .context("Failed to create MemTable")?;

        self.session
            .register_table(name, Arc::new(mem_table))
            .context("Failed to register table")?;

        self.tables.insert(name.to_string(), (schema, batches));
        Ok(())
    }

    /// Execute a SQL query against workspace tables.
    ///
    /// # Errors
    ///
    /// Returns an error if SQL parsing or execution fails.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .session
            .sql(sql)
            .await
            .context("SQL query failed")?;
        let batches = df.collect().await.context("Failed to collect results")?;
        Ok(batches)
    }

    /// List all tables with summary stats.
    pub fn summary(&self) -> Vec<TableSummary> {
        let mut summaries: Vec<TableSummary> = self
            .tables
            .iter()
            .map(|(name, (schema, batches))| {
                let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let memory_bytes: u64 = batches
                    .iter()
                    .map(|b| b.get_array_memory_size() as u64)
                    .sum();
                TableSummary {
                    name: name.clone(),
                    rows,
                    columns: schema.fields().len(),
                    memory_bytes,
                }
            })
            .collect();
        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        summaries
    }

    /// Drop a single table or all tables.
    pub fn clear(&mut self, table: Option<&str>) {
        match table {
            Some(name) => {
                let _ = self.session.deregister_table(name);
                self.tables.remove(name);
            }
            None => {
                for name in self.tables.keys().cloned().collect::<Vec<_>>() {
                    let _ = self.session.deregister_table(&name);
                }
                self.tables.clear();
            }
        }
    }

    /// Check if a table exists in the workspace.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Returns true if the workspace has no tables.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
        vec![RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()]
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        let batches = test_batches(&schema);
        ws.insert("users", schema, batches).unwrap();

        let results = ws.query("SELECT * FROM users").await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_insert_replaces_existing() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();

        // Insert first version
        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();

        // Insert replacement (different data)
        let new_batches = vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["dave"])),
            ],
        )
        .unwrap()];
        ws.insert("users", schema, new_batches).unwrap();

        let results = ws.query("SELECT * FROM users").await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_summary() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();

        let summaries = ws.summary();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].name, "users");
        assert_eq!(summaries[0].rows, 3);
        assert_eq!(summaries[0].columns, 2);
        assert!(summaries[0].memory_bytes > 0);
    }

    #[tokio::test]
    async fn test_clear_single() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();
        ws.insert("orders", schema.clone(), test_batches(&schema))
            .unwrap();

        ws.clear(Some("users"));
        assert!(!ws.has_table("users"));
        assert!(ws.has_table("orders"));
    }

    #[tokio::test]
    async fn test_clear_all() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();
        ws.insert("orders", schema.clone(), test_batches(&schema))
            .unwrap();

        ws.clear(None);
        assert!(ws.is_empty());
    }

    #[tokio::test]
    async fn test_sql_error_returns_err() {
        let ws = ArrowWorkspace::new();
        let result = ws.query("SELECT * FROM nonexistent").await;
        assert!(result.is_err());
    }
}
```

**Step 2: Add module to lib.rs**

Add `pub mod workspace;` to `crates/rapidbyte-dev/src/lib.rs`.

**Step 3: Run tests**

Run: `cargo test -p rapidbyte-dev`
Expected: all 6 tests pass

**Step 4: Commit**

```bash
git add crates/rapidbyte-dev/src/workspace.rs crates/rapidbyte-dev/src/lib.rs
git commit -m "feat(dev): add ArrowWorkspace with DataFusion SQL support

In-memory workspace holds streamed RecordBatches, registers as DataFusion
MemTables for SQL queries. Supports insert, replace, clear, summary."
```

---

### Task 4: Build the display module

**Files:**
- Create: `crates/rapidbyte-dev/src/display.rs`
- Modify: `crates/rapidbyte-dev/src/lib.rs`

**References to read before writing:**
- `crates/rapidbyte-cli/src/output/format.rs` (format_bytes, format_count helpers)
- `crates/rapidbyte-cli/src/output/summary.rs` (styled output with console crate)
- Arrow `pretty_format_batches` API: `arrow::util::pretty::pretty_format_batches(&batches)`

**Step 1: Write display.rs**

```rust
//! Display helpers for the dev shell — table rendering and styled output.

#![allow(clippy::cast_precision_loss)]

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use console::style;

/// Format bytes into human-readable form.
pub(crate) fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

/// Format a count with comma separators.
pub(crate) fn format_count(n: u64) -> String {
    if n < 1000 {
        return n.to_string();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Print Arrow RecordBatches as a Unicode box-drawing table with row count footer.
pub(crate) fn print_batches(batches: &[RecordBatch]) {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        eprintln!("0 rows");
        return;
    }

    match pretty_format_batches(batches) {
        Ok(table) => {
            eprintln!("{table}");
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "{} row{}",
                format_count(total_rows as u64),
                if total_rows == 1 { "" } else { "s" },
            );
        }
        Err(e) => {
            print_error(&format!("Failed to format results: {e}"));
        }
    }
}

/// Print a success message with green checkmark.
pub(crate) fn print_success(msg: &str) {
    eprintln!("{} {msg}", style("\u{2713}").green().bold());
}

/// Print an error message with red cross.
pub(crate) fn print_error(msg: &str) {
    eprintln!("{} {msg}", style("\u{2718}").red().bold());
}

/// Print an info/hint message (dim).
pub(crate) fn print_hint(msg: &str) {
    eprintln!("{}", style(msg).dim());
}
```

**Step 2: Add module to lib.rs**

Add `pub(crate) mod display;` to `crates/rapidbyte-dev/src/lib.rs`.

**Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-dev`
Expected: compiles without errors

**Step 4: Commit**

```bash
git add crates/rapidbyte-dev/src/display.rs crates/rapidbyte-dev/src/lib.rs
git commit -m "feat(dev): add display module for table rendering and styled output

Arrow pretty_format_batches for Unicode box tables, format_bytes/count
helpers, styled success/error/hint messages."
```

---

### Task 5: Build the command parser

**Files:**
- Create: `crates/rapidbyte-dev/src/commands/mod.rs`
- Modify: `crates/rapidbyte-dev/src/lib.rs`

This task builds the command enum and parser that dispatches dot-commands vs SQL. No command implementations yet — just the parsing layer.

**Step 1: Write command parser with tests**

```rust
//! Dot-command parser for the dev shell.
//!
//! Lines starting with `.` are parsed as dot-commands.
//! Everything else is forwarded as SQL.

/// Parsed REPL input.
#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    /// .source <connector> [--key value ...]
    Source {
        connector: String,
        args: Vec<(String, String)>,
    },
    /// .tables
    Tables,
    /// .schema <table>
    Schema { table: String },
    /// .stream <table> [--limit N]
    Stream { table: String, limit: Option<u64> },
    /// .workspace
    Workspace,
    /// .clear [table]
    Clear { table: Option<String> },
    /// .help
    Help,
    /// .quit
    Quit,
    /// Raw SQL query
    Sql(String),
}

/// Parse a line of REPL input into a Command.
///
/// Returns `None` for blank lines. Returns `Some(Err(...))` for malformed dot-commands.
pub(crate) fn parse(line: &str) -> Option<Result<Command, String>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    if !trimmed.starts_with('.') {
        return Some(Ok(Command::Sql(trimmed.to_string())));
    }

    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    let cmd = parts[0].to_lowercase();

    Some(match cmd.as_str() {
        ".quit" | ".exit" | ".q" => Ok(Command::Quit),
        ".help" | ".h" => Ok(Command::Help),
        ".tables" => Ok(Command::Tables),
        ".workspace" | ".ws" => Ok(Command::Workspace),
        ".schema" => {
            if parts.len() < 2 {
                Err("Usage: .schema <table>".to_string())
            } else {
                Ok(Command::Schema {
                    table: parts[1].to_string(),
                })
            }
        }
        ".stream" => parse_stream(&parts[1..]),
        ".source" => parse_source(&parts[1..]),
        ".clear" => Ok(Command::Clear {
            table: parts.get(1).map(|s| (*s).to_string()),
        }),
        _ => Err(format!("Unknown command: {cmd}. Type .help for commands.")),
    })
}

fn parse_stream(args: &[&str]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("Usage: .stream <table> [--limit N]".to_string());
    }
    let table = args[0].to_string();
    let mut limit: Option<u64> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--limit" {
            i += 1;
            if i >= args.len() {
                return Err("--limit requires a value".to_string());
            }
            limit = Some(
                args[i]
                    .parse()
                    .map_err(|_| format!("Invalid limit: {}", args[i]))?,
            );
        }
        i += 1;
    }
    Ok(Command::Stream { table, limit })
}

fn parse_source(args: &[&str]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("Usage: .source <connector> [--key value ...]".to_string());
    }
    let connector = args[0].to_string();
    let mut kv_args: Vec<(String, String)> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if let Some(key) = args[i].strip_prefix("--") {
            i += 1;
            if i >= args.len() {
                return Err(format!("--{key} requires a value"));
            }
            kv_args.push((key.to_string(), args[i].to_string()));
        }
        i += 1;
    }
    Ok(Command::Source {
        connector,
        args: kv_args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blank_line() {
        assert!(parse("").is_none());
        assert!(parse("   ").is_none());
    }

    #[test]
    fn test_sql() {
        let cmd = parse("SELECT * FROM users").unwrap().unwrap();
        assert_eq!(cmd, Command::Sql("SELECT * FROM users".to_string()));
    }

    #[test]
    fn test_quit() {
        assert_eq!(parse(".quit").unwrap().unwrap(), Command::Quit);
        assert_eq!(parse(".exit").unwrap().unwrap(), Command::Quit);
        assert_eq!(parse(".q").unwrap().unwrap(), Command::Quit);
    }

    #[test]
    fn test_help() {
        assert_eq!(parse(".help").unwrap().unwrap(), Command::Help);
        assert_eq!(parse(".h").unwrap().unwrap(), Command::Help);
    }

    #[test]
    fn test_tables() {
        assert_eq!(parse(".tables").unwrap().unwrap(), Command::Tables);
    }

    #[test]
    fn test_workspace() {
        assert_eq!(parse(".workspace").unwrap().unwrap(), Command::Workspace);
        assert_eq!(parse(".ws").unwrap().unwrap(), Command::Workspace);
    }

    #[test]
    fn test_schema() {
        let cmd = parse(".schema users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Schema {
                table: "users".to_string()
            }
        );
    }

    #[test]
    fn test_schema_missing_arg() {
        assert!(parse(".schema").unwrap().is_err());
    }

    #[test]
    fn test_stream_basic() {
        let cmd = parse(".stream public.users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Stream {
                table: "public.users".to_string(),
                limit: None,
            }
        );
    }

    #[test]
    fn test_stream_with_limit() {
        let cmd = parse(".stream users --limit 1000").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Stream {
                table: "users".to_string(),
                limit: Some(1000),
            }
        );
    }

    #[test]
    fn test_stream_missing_arg() {
        assert!(parse(".stream").unwrap().is_err());
    }

    #[test]
    fn test_source() {
        let cmd = parse(".source postgres --host localhost --port 5432")
            .unwrap()
            .unwrap();
        assert_eq!(
            cmd,
            Command::Source {
                connector: "postgres".to_string(),
                args: vec![
                    ("host".to_string(), "localhost".to_string()),
                    ("port".to_string(), "5432".to_string()),
                ],
            }
        );
    }

    #[test]
    fn test_source_missing_connector() {
        assert!(parse(".source").unwrap().is_err());
    }

    #[test]
    fn test_clear_all() {
        let cmd = parse(".clear").unwrap().unwrap();
        assert_eq!(cmd, Command::Clear { table: None });
    }

    #[test]
    fn test_clear_one() {
        let cmd = parse(".clear users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Clear {
                table: Some("users".to_string()),
            }
        );
    }

    #[test]
    fn test_unknown_command() {
        assert!(parse(".foobar").unwrap().is_err());
    }
}
```

**Step 2: Add module to lib.rs**

Add `pub(crate) mod commands;` to `crates/rapidbyte-dev/src/lib.rs`.

**Step 3: Run tests**

Run: `cargo test -p rapidbyte-dev`
Expected: all tests pass (workspace + command parser)

**Step 4: Commit**

```bash
git add crates/rapidbyte-dev/src/commands/ crates/rapidbyte-dev/src/lib.rs
git commit -m "feat(dev): add dot-command parser

Parses .source, .tables, .schema, .stream, .workspace, .clear, .help,
.quit. Non-dot lines forwarded as SQL."
```

---

### Task 6: Build the REPL loop

**Files:**
- Create: `crates/rapidbyte-dev/src/repl.rs`
- Modify: `crates/rapidbyte-dev/src/lib.rs`

**References to read before writing:**
- reedline API: `Reedline::create()`, `.read_line(&prompt)`, `Signal::Success/CtrlD/CtrlC`
- `crates/rapidbyte-cli/src/main.rs` (version from clap, exit patterns)

This task builds the REPL loop with reedline. Command implementations are stubs that print "not yet implemented" — real implementations come in later tasks.

**Step 1: Write repl.rs**

```rust
//! REPL loop: reedline setup, input dispatch, command execution stubs.

use std::borrow::Cow;

use reedline::{DefaultPrompt, DefaultPromptSegment, Reedline, Signal};

use crate::commands::{self, Command};
use crate::display;
use crate::workspace::ArrowWorkspace;

/// REPL state for the dev shell session.
pub(crate) struct ReplState {
    pub workspace: ArrowWorkspace,
    pub source: Option<ConnectedSource>,
}

/// A connected source connector with its discovered catalog.
pub(crate) struct ConnectedSource {
    pub connector_ref: String,
    pub config: serde_json::Value,
    pub catalog: rapidbyte_types::catalog::Catalog,
    pub loaded_module: rapidbyte_runtime::LoadedComponent,
    pub permissions: Option<rapidbyte_types::manifest::Permissions>,
}

/// Run the interactive REPL.
pub(crate) async fn run() -> anyhow::Result<()> {
    print_banner();

    let mut editor = Reedline::create();
    let prompt = build_prompt();
    let mut state = ReplState {
        workspace: ArrowWorkspace::new(),
        source: None,
    };

    loop {
        match editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                if let Some(result) = commands::parse(&line) {
                    match result {
                        Ok(cmd) => {
                            if handle_command(cmd, &mut state).await {
                                break;
                            }
                        }
                        Err(msg) => display::print_error(&msg),
                    }
                }
            }
            Ok(Signal::CtrlD) => break,
            Ok(Signal::CtrlC) => continue,
            Err(e) => {
                display::print_error(&format!("Input error: {e}"));
                break;
            }
        }
    }

    Ok(())
}

/// Returns `true` if the REPL should exit.
async fn handle_command(cmd: Command, state: &mut ReplState) -> bool {
    match cmd {
        Command::Quit => return true,
        Command::Help => print_help(),
        Command::Source { connector, args } => handle_source(&connector, &args, state).await,
        Command::Tables => handle_tables(state),
        Command::Schema { table } => handle_schema(&table, state),
        Command::Stream { table, limit } => handle_stream(&table, limit, state).await,
        Command::Workspace => handle_workspace(state),
        Command::Clear { table } => handle_clear(table.as_deref(), state),
        Command::Sql(query) => handle_sql(&query, state).await,
    }
    false
}

fn print_banner() {
    eprintln!(
        "{} v{}",
        console::style("Rapidbyte Dev Shell").bold(),
        env!("CARGO_PKG_VERSION"),
    );
    display::print_hint("Type .help for commands, SQL to query workspace tables, .quit to exit");
    eprintln!();
}

fn build_prompt() -> DefaultPrompt {
    DefaultPrompt::new(
        DefaultPromptSegment::Basic("rb".to_string()),
        DefaultPromptSegment::Empty,
    )
}

fn print_help() {
    eprintln!("  {:<38} {}", ".source <connector> [--key value ...]", "Connect to a source");
    eprintln!("  {:<38} {}", ".tables", "List discovered streams");
    eprintln!("  {:<38} {}", ".schema <table>", "Show column schema");
    eprintln!("  {:<38} {}", ".stream <table> [--limit N]", "Stream data into workspace");
    eprintln!("  {:<38} {}", ".workspace", "Show workspace tables");
    eprintln!("  {:<38} {}", ".clear [table]", "Clear workspace (or one table)");
    eprintln!("  {:<38} {}", "SELECT ...", "Query workspace with SQL");
    eprintln!("  {:<38} {}", ".quit", "Exit");
}

async fn handle_source(connector: &str, args: &[(String, String)], state: &mut ReplState) {
    // Build config JSON from key-value args
    let mut config = serde_json::Map::new();
    for (k, v) in args {
        // Try to parse as number, otherwise keep as string
        if let Ok(n) = v.parse::<i64>() {
            config.insert(k.clone(), serde_json::Value::Number(n.into()));
        } else {
            config.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
    }
    let config_value = serde_json::Value::Object(config);

    // Resolve connector name to "source-<name>" if bare name given
    let connector_ref = if connector.starts_with("source-") {
        connector.to_string()
    } else {
        format!("source-{connector}")
    };

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}", "\u{2827}", "\u{2807}", "\u{280f}"]),
    );
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    spinner.set_message("Connecting...");

    match connect_source(&connector_ref, &config_value).await {
        Ok(connected) => {
            let stream_count = connected.catalog.streams.len();
            spinner.finish_and_clear();
            display::print_success(&format!(
                "Connected \u{2014} {} stream{} discovered",
                stream_count,
                if stream_count == 1 { "" } else { "s" },
            ));
            state.source = Some(connected);
        }
        Err(e) => {
            spinner.finish_and_clear();
            display::print_error(&format!("Connection failed: {e:#}"));
        }
    }
}

async fn connect_source(
    connector_ref: &str,
    config: &serde_json::Value,
) -> anyhow::Result<ConnectedSource> {
    let wasm_path = rapidbyte_runtime::resolve_connector_path(connector_ref)?;
    let manifest = rapidbyte_runtime::load_connector_manifest(&wasm_path)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());

    let config_clone = config.clone();
    let connector_ref_owned = connector_ref.to_string();
    let permissions_clone = permissions.clone();

    let (catalog, loaded_module) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let runtime = rapidbyte_runtime::WasmRuntime::new()?;
        let module = runtime.load_module(&wasm_path)?;

        // Run discover using the loaded module
        let state_backend = std::sync::Arc::new(rapidbyte_state::SqliteStateBackend::in_memory()?);
        let mut builder = rapidbyte_runtime::ComponentHostState::builder()
            .pipeline("dev")
            .connector_id(&connector_ref_owned)
            .stream("discover")
            .state_backend(state_backend)
            .config(&config_clone)
            .compression(None);
        if let Some(ref p) = permissions_clone {
            builder = builder.permissions(p);
        }
        let host_state = builder.build()?;

        let mut store = module.new_store(host_state, None);
        let linker = rapidbyte_runtime::create_component_linker(&module.engine, "source", |linker| {
            rapidbyte_runtime::source_bindings::RapidbyteSource::add_to_linker::<
                _,
                rapidbyte_runtime::wasmtime_reexport::HasSelf<_>,
            >(linker, |state| state)?;
            Ok(())
        })?;
        let bindings = rapidbyte_runtime::source_bindings::RapidbyteSource::instantiate(
            &mut store,
            &module.component,
            &linker,
        )?;
        let iface = bindings.rapidbyte_connector_source();
        let config_json = serde_json::to_string(&config_clone)?;

        let session = iface
            .call_open(&mut store, &config_json)?
            .map_err(rapidbyte_runtime::source_error_to_sdk)
            .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

        let discover_json = iface
            .call_discover(&mut store, session)?
            .map_err(rapidbyte_runtime::source_error_to_sdk)
            .map_err(|e| anyhow::anyhow!("Discover failed: {e}"))?;

        let catalog: rapidbyte_types::catalog::Catalog =
            serde_json::from_str(&discover_json)?;

        if let Err(err) = iface.call_close(&mut store, session)? {
            tracing::warn!(
                "Source close failed after discover: {}",
                rapidbyte_runtime::source_error_to_sdk(err)
            );
        }

        Ok((catalog, module))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Discover task panicked: {e}"))??;

    Ok(ConnectedSource {
        connector_ref: connector_ref.to_string(),
        config: config.clone(),
        catalog,
        loaded_module,
        permissions,
    })
}

fn require_source(state: &ReplState) -> bool {
    if state.source.is_none() {
        display::print_error("No source connected. Use .source first.");
        return false;
    }
    true
}

fn handle_tables(state: &ReplState) {
    if !require_source(state) {
        return;
    }
    let catalog = &state.source.as_ref().unwrap().catalog;
    if catalog.streams.is_empty() {
        display::print_hint("No streams found.");
        return;
    }

    let name_w = catalog
        .streams
        .iter()
        .map(|s| s.name.len())
        .max()
        .unwrap_or(6)
        .max(6);
    let sync_w = 12;

    eprintln!(
        "  {:<name_w$}   {:<sync_w$}   Columns",
        console::style("Stream").bold(),
        console::style("Sync").bold(),
    );
    let rule_len = name_w + sync_w + 14;
    eprintln!("  {}", "\u{2500}".repeat(rule_len));

    for stream in &catalog.streams {
        let sync = if stream.supported_sync_modes.contains(&rapidbyte_types::wire::SyncMode::Cdc) {
            "cdc"
        } else if stream
            .supported_sync_modes
            .contains(&rapidbyte_types::wire::SyncMode::Incremental)
        {
            "incremental"
        } else {
            "full"
        };
        eprintln!(
            "  {:<name_w$}   {:<sync_w$}   {}",
            stream.name,
            sync,
            stream.schema.len(),
        );
    }
    eprintln!();
}

fn handle_schema(table: &str, state: &ReplState) {
    if !require_source(state) {
        return;
    }
    let catalog = &state.source.as_ref().unwrap().catalog;
    let Some(stream) = catalog.streams.iter().find(|s| {
        s.name == table || s.name.ends_with(&format!(".{table}"))
    }) else {
        display::print_error(&format!("Stream '{table}' not found in catalog."));
        return;
    };

    eprintln!(
        "  {:<28} {:<16} {}",
        console::style("Column").bold(),
        console::style("Type").bold(),
        console::style("Nullable").bold(),
    );
    eprintln!("  {}", "\u{2500}".repeat(52));
    for col in &stream.schema {
        let nullable = if col.nullable { "YES" } else { "NO" };
        eprintln!(
            "  {:<28} {:<16} {}",
            console::style(&col.name).cyan(),
            format!("{}", col.data_type),
            nullable,
        );
    }
    eprintln!();
}

async fn handle_stream(table: &str, limit: Option<u64>, state: &mut ReplState) {
    if !require_source(state) {
        return;
    }

    let source = state.source.as_ref().unwrap();
    let catalog = &source.catalog;

    // Find stream in catalog
    let Some(stream_def) = catalog.streams.iter().find(|s| {
        s.name == table || s.name.ends_with(&format!(".{table}"))
    }) else {
        display::print_error(&format!("Stream '{table}' not found in catalog."));
        return;
    };

    let stream_name = stream_def.name.clone();
    let schema_hint = rapidbyte_types::catalog::SchemaHint::Columns(stream_def.schema.clone());
    let sync_mode = if stream_def
        .supported_sync_modes
        .contains(&rapidbyte_types::wire::SyncMode::FullRefresh)
    {
        rapidbyte_types::wire::SyncMode::FullRefresh
    } else {
        stream_def.supported_sync_modes[0]
    };

    let stream_ctx = rapidbyte_types::stream::StreamContext {
        stream_name: stream_name.clone(),
        source_stream_name: None,
        schema: schema_hint,
        sync_mode,
        cursor_info: None,
        limits: {
            let mut limits = rapidbyte_types::stream::StreamLimits::default();
            limits.max_records = limit;
            limits
        },
        policies: rapidbyte_types::stream::StreamPolicies::default(),
        write_mode: None,
        selected_columns: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    };

    let module = source.loaded_module.clone();
    let config = source.config.clone();
    let connector_ref = source.connector_ref.clone();
    let permissions = source.permissions.clone();

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}", "\u{2827}", "\u{2807}", "\u{280f}"]),
    );
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    spinner.set_message(format!("Streaming {stream_name}..."));

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        let (tx, rx) = std::sync::mpsc::channel::<rapidbyte_runtime::Frame>();

        let state_backend = std::sync::Arc::new(rapidbyte_state::SqliteStateBackend::in_memory()?);
        let mut builder = rapidbyte_runtime::ComponentHostState::builder()
            .pipeline("dev")
            .connector_id(&connector_ref)
            .stream(stream_ctx.stream_name.clone())
            .state_backend(state_backend)
            .sender(tokio::sync::mpsc::channel(32).0) // dummy — we use sync channel below
            .config(&config)
            .compression(None);
        if let Some(ref p) = permissions {
            builder = builder.permissions(p);
        }
        let host_state = builder.build()?;

        // We actually need to use the mpsc sender from ComponentHostState.
        // Since the host calls emit_batch which sends frames through the sender,
        // we need to set up the real tokio channel and collect from it.
        // Let's use a simpler approach: create a bounded tokio channel,
        // pass the sender to host state, and collect frames.
        drop(tx);
        drop(rx);

        let (tokio_tx, mut tokio_rx) = tokio::sync::mpsc::channel::<rapidbyte_runtime::Frame>(64);

        let state_backend2 = std::sync::Arc::new(rapidbyte_state::SqliteStateBackend::in_memory()?);
        let mut builder2 = rapidbyte_runtime::ComponentHostState::builder()
            .pipeline("dev")
            .connector_id(&connector_ref)
            .stream(stream_ctx.stream_name.clone())
            .state_backend(state_backend2)
            .sender(tokio_tx)
            .config(&config)
            .compression(None);
        if let Some(ref p) = permissions {
            builder2 = builder2.permissions(p);
        }
        let host_state2 = builder2.build()?;

        let mut store = module.new_store(host_state2, None);
        let linker = rapidbyte_runtime::create_component_linker(&module.engine, "source", |linker| {
            rapidbyte_runtime::source_bindings::RapidbyteSource::add_to_linker::<
                _,
                rapidbyte_runtime::wasmtime_reexport::HasSelf<_>,
            >(linker, |state| state)?;
            Ok(())
        })?;
        let bindings = rapidbyte_runtime::source_bindings::RapidbyteSource::instantiate(
            &mut store,
            &module.component,
            &linker,
        )?;
        let iface = bindings.rapidbyte_connector_source();
        let config_json = serde_json::to_string(&config)?;

        let session = iface
            .call_open(&mut store, &config_json)?
            .map_err(rapidbyte_runtime::source_error_to_sdk)
            .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

        let ctx_json = serde_json::to_string(&stream_ctx)?;
        let run_request = rapidbyte_runtime::source_bindings::rapidbyte::connector::types::RunRequest {
            phase: rapidbyte_runtime::source_bindings::rapidbyte::connector::types::RunPhase::Read,
            stream_context_json: ctx_json,
            dry_run: false,
            max_records: limit,
        };

        let _run_result = iface
            .call_run(&mut store, session, &run_request)?
            .map_err(rapidbyte_runtime::source_error_to_sdk)
            .map_err(|e| anyhow::anyhow!("Source read failed: {e}"))?;

        let _ = iface.call_close(&mut store, session);

        // Collect all frames from the channel
        let mut all_batches = Vec::new();
        // Close the sender by dropping the store (which drops host state)
        drop(store);

        while let Ok(frame) = tokio_rx.try_recv() {
            match frame {
                rapidbyte_runtime::Frame::Data(bytes) => {
                    let batches = rapidbyte_engine::arrow::ipc_to_record_batches(&bytes)?;
                    all_batches.extend(batches);
                }
                rapidbyte_runtime::Frame::EndStream => break,
            }
        }

        Ok(all_batches)
    })
    .await;

    spinner.finish_and_clear();

    match result {
        Ok(Ok(batches)) => {
            if batches.is_empty() {
                display::print_success(&format!("0 rows streamed from {stream_name}"));
                return;
            }

            let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let total_bytes: u64 = batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .sum();
            let schema = batches[0].schema();
            let col_count = schema.fields().len();

            // Short table name (strip schema prefix for workspace key)
            let short_name = table
                .rsplit('.')
                .next()
                .unwrap_or(table);

            if let Err(e) = state.workspace.insert(short_name, schema, batches) {
                display::print_error(&format!("Failed to register in workspace: {e:#}"));
                return;
            }

            display::print_success(&format!(
                "{} rows \u{2192} workspace:{short_name} ({col_count} columns, {})",
                display::format_count(total_rows),
                display::format_bytes(total_bytes),
            ));
        }
        Ok(Err(e)) => display::print_error(&format!("Stream failed: {e:#}")),
        Err(e) => display::print_error(&format!("Stream task panicked: {e}")),
    }
}

fn handle_workspace(state: &ReplState) {
    let summaries = state.workspace.summary();
    if summaries.is_empty() {
        display::print_hint("Workspace is empty. Use .stream to load data.");
        return;
    }

    eprintln!(
        "  {:<20} {:>10} {:>10} {:>10}",
        console::style("Table").bold(),
        console::style("Rows").bold(),
        console::style("Columns").bold(),
        console::style("Memory").bold(),
    );
    eprintln!("  {}", "\u{2500}".repeat(54));
    for s in &summaries {
        eprintln!(
            "  {:<20} {:>10} {:>10} {:>10}",
            s.name,
            display::format_count(s.rows),
            s.columns,
            display::format_bytes(s.memory_bytes),
        );
    }
    eprintln!();
}

fn handle_clear(table: Option<&str>, state: &mut ReplState) {
    state.workspace.clear(table);
    match table {
        Some(name) => display::print_success(&format!("Dropped table '{name}'")),
        None => display::print_success("Workspace cleared"),
    }
}

async fn handle_sql(query: &str, state: &ReplState) {
    if state.workspace.is_empty() {
        display::print_hint("Workspace is empty. Use .stream to load data first.");
        return;
    }

    match state.workspace.query(query).await {
        Ok(batches) => display::print_batches(&batches),
        Err(e) => display::print_error(&format!("{e:#}")),
    }
}
```

**Step 2: Update lib.rs to use repl**

Replace `crates/rapidbyte-dev/src/lib.rs` with:

```rust
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
//! | `repl`     | REPL loop, command dispatch, source operations |
//! | `workspace`| Arrow workspace with DataFusion SQL |

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub(crate) mod commands;
pub(crate) mod display;
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
```

**Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-dev`
Expected: compiles (there may be warnings about unused code paths — the real test is Task 7 when we run it end-to-end)

**Step 4: Commit**

```bash
git add crates/rapidbyte-dev/src/repl.rs crates/rapidbyte-dev/src/lib.rs
git commit -m "feat(dev): implement REPL loop with all command handlers

reedline REPL with .source (Wasm connector discover), .tables, .schema,
.stream (reads into workspace), .workspace, .clear, SQL queries, .help.
Spinner during async operations. Styled output."
```

---

### Task 7: Add reedline completer and highlighter

**Files:**
- Create: `crates/rapidbyte-dev/src/completer.rs`
- Create: `crates/rapidbyte-dev/src/highlighter.rs`
- Modify: `crates/rapidbyte-dev/src/repl.rs` (wire in completer + highlighter)
- Modify: `crates/rapidbyte-dev/src/lib.rs` (add modules)

**Step 1: Write completer.rs**

```rust
//! Context-aware tab completion for the dev shell.

use reedline::{Completer, Span, Suggestion};

/// Completer for dot-commands, table names, and SQL keywords.
pub(crate) struct DevCompleter {
    dot_commands: Vec<String>,
    table_names: Vec<String>,
}

impl DevCompleter {
    pub fn new() -> Self {
        Self {
            dot_commands: vec![
                ".source".into(),
                ".tables".into(),
                ".schema".into(),
                ".stream".into(),
                ".workspace".into(),
                ".clear".into(),
                ".help".into(),
                ".quit".into(),
            ],
            table_names: Vec::new(),
        }
    }

    /// Update the list of known table names (from catalog or workspace).
    pub fn set_table_names(&mut self, names: Vec<String>) {
        self.table_names = names;
    }
}

impl Completer for DevCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = &line[..pos];
        let word_start = prefix.rfind(char::is_whitespace).map_or(0, |i| i + 1);
        let word = &prefix[word_start..];

        if word.is_empty() {
            return Vec::new();
        }

        let span = Span::new(word_start, pos);

        if word.starts_with('.') {
            return self
                .dot_commands
                .iter()
                .filter(|c| c.starts_with(word))
                .map(|c| Suggestion {
                    value: c.clone(),
                    description: None,
                    style: None,
                    extra: None,
                    span,
                    append_whitespace: true,
                })
                .collect();
        }

        // After .schema, .stream, .clear, FROM, JOIN — suggest table names
        let lower = prefix.to_lowercase();
        let suggest_tables = lower.contains(".schema ")
            || lower.contains(".stream ")
            || lower.contains(".clear ")
            || lower.contains(" from ")
            || lower.contains(" join ");

        if suggest_tables {
            return self
                .table_names
                .iter()
                .filter(|t| t.starts_with(word) || t.to_lowercase().starts_with(&word.to_lowercase()))
                .map(|t| Suggestion {
                    value: t.clone(),
                    description: None,
                    style: None,
                    extra: None,
                    span,
                    append_whitespace: true,
                })
                .collect();
        }

        Vec::new()
    }
}
```

**Step 2: Write highlighter.rs**

```rust
//! Syntax highlighting for the dev shell.

use reedline::{Highlighter, StyledText};
use nu_ansi_term::{Color, Style};

/// Highlighter for SQL keywords and dot-commands.
pub(crate) struct DevHighlighter;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "GROUP", "BY",
    "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT",
    "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
    "LIKE", "BETWEEN", "EXISTS", "TRUE", "FALSE", "ASC", "DESC",
];

impl Highlighter for DevHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();

        if line.starts_with('.') {
            styled.push((Style::new().fg(Color::Green).bold(), line.to_string()));
            return styled;
        }

        // Tokenize and highlight SQL
        let mut chars = line.chars().peekable();
        let mut current = String::new();

        while let Some(&ch) = chars.peek() {
            if ch == '\'' {
                // String literal
                flush_word(&mut current, &mut styled);
                let mut literal = String::new();
                literal.push(chars.next().unwrap());
                while let Some(&c) = chars.peek() {
                    literal.push(chars.next().unwrap());
                    if c == '\'' {
                        break;
                    }
                }
                styled.push((Style::new().fg(Color::Yellow), literal));
            } else if ch.is_ascii_digit() && current.is_empty() {
                // Number
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' {
                        num.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                styled.push((Style::new().fg(Color::Cyan), num));
            } else if ch.is_whitespace() || "(),;*".contains(ch) {
                flush_word(&mut current, &mut styled);
                styled.push((Style::default(), chars.next().unwrap().to_string()));
            } else {
                current.push(chars.next().unwrap());
            }
        }

        flush_word(&mut current, &mut styled);
        styled
    }
}

fn flush_word(word: &mut String, styled: &mut StyledText) {
    if word.is_empty() {
        return;
    }
    let upper = word.to_uppercase();
    if SQL_KEYWORDS.contains(&upper.as_str()) {
        styled.push((Style::new().bold(), word.clone()));
    } else {
        styled.push((Style::default(), word.clone()));
    }
    word.clear();
}
```

**Step 3: Wire into repl.rs**

In `repl::run()`, replace the `Reedline::create()` line with:

```rust
    let completer = Box::new(crate::completer::DevCompleter::new());
    let highlighter = Box::new(crate::highlighter::DevHighlighter);
    let mut editor = Reedline::create()
        .with_completer(completer)
        .with_highlighter(highlighter);
```

Note: Updating completer table names dynamically after `.source` and `.stream` requires shared state. For simplicity in MVP, the completer is initialized empty and we can add dynamic updates later.

**Step 4: Add modules to lib.rs**

Add:
```rust
pub(crate) mod completer;
pub(crate) mod highlighter;
```

**Step 5: Add `nu-ansi-term` dependency to Cargo.toml**

reedline re-exports `nu-ansi-term` but we need it directly for the highlighter:
```toml
nu-ansi-term = "0.50"
```

**Step 6: Verify it compiles**

Run: `cargo check -p rapidbyte-dev`
Expected: compiles

**Step 7: Commit**

```bash
git add crates/rapidbyte-dev/src/completer.rs crates/rapidbyte-dev/src/highlighter.rs crates/rapidbyte-dev/src/repl.rs crates/rapidbyte-dev/src/lib.rs crates/rapidbyte-dev/Cargo.toml Cargo.lock
git commit -m "feat(dev): add tab completion and syntax highlighting

Dot-command completion, table name suggestions after FROM/JOIN/.stream,
SQL keyword highlighting (bold), string literals (yellow), numbers (cyan),
dot-commands (green)."
```

---

### Task 8: End-to-end manual test and polish

**Files:**
- Potentially modify any files from Tasks 1-7 based on test findings

**This task has no code to write — it's a verification and fix-up pass.**

**Step 1: Start dev environment**

Run: `just dev-up`
Expected: Docker Postgres running, connectors built, data seeded

**Step 2: Launch dev shell**

Run: `cargo run -- dev`
Expected: See banner:
```
Rapidbyte Dev Shell v0.1.0
Type .help for commands, SQL to query workspace tables, .quit to exit
```

**Step 3: Test .help**

Type: `.help`
Expected: Help text with all commands listed

**Step 4: Test .source**

Type: `.source postgres --host localhost --port 5432 --user postgres --password postgres --database rapidbyte_source`
Expected: `✓ Connected — N streams discovered`

**Step 5: Test .tables**

Type: `.tables`
Expected: Table showing discovered streams with sync modes and column counts

**Step 6: Test .schema**

Type: `.schema <stream_name>` (use a stream name from .tables output)
Expected: Column table with names, types, nullable

**Step 7: Test .stream**

Type: `.stream <stream_name> --limit 100`
Expected: Spinner → `✓ 100 rows → workspace:<name> (N columns, X KB)`

**Step 8: Test SQL**

Type: `SELECT * FROM <name> LIMIT 5;`
Expected: Unicode box-drawing table with 5 rows and row count footer

**Step 9: Test .workspace**

Type: `.workspace`
Expected: Table showing workspace contents with rows, columns, memory

**Step 10: Test .clear and .quit**

Type: `.clear`, then `.workspace` (should be empty), then `.quit`

**Step 11: Fix any issues found**

If something doesn't compile or behave correctly, fix it. Common things:
- Import paths may need adjustment
- The `mpsc::Sender` vs `tokio::sync::mpsc::Sender` in ComponentHostState builder may need adaptation based on what the builder actually accepts
- `nu-ansi-term` version may need to match what reedline exports

**Step 12: Commit fixes**

```bash
git add -A
git commit -m "fix(dev): polish from end-to-end testing

<describe what was fixed>"
```

---

### Task 9: Update Justfile and docs

**Files:**
- Modify: `Justfile` (add `dev` recipe)
- Modify: `README.md` (add `rapidbyte dev` to CLI table)
- Modify: `CLAUDE.md` (add rapidbyte-dev to project structure and dependency graph)

**Step 1: Add `dev` recipe to Justfile**

Add (near the `run` recipe):
```
# Launch interactive dev shell
dev *ARGS:
    cargo run {{MODE_FLAG}} -- dev {{ARGS}}
```

**Step 2: Add to README CLI table**

Add row after `rapidbyte scaffold`:
```markdown
| `rapidbyte dev` | Launch interactive dev shell (explore, query, test) |
```

**Step 3: Update CLAUDE.md**

Add `rapidbyte-dev/` to the project structure listing with description "Interactive dev shell (REPL, Arrow workspace, DataFusion)".

Update the crate dependency graph to include:
```
  +-- dev      -> engine, runtime, types, state
```

**Step 4: Commit**

```bash
git add Justfile README.md CLAUDE.md
git commit -m "docs: add rapidbyte dev to Justfile, README, and CLAUDE.md"
```
