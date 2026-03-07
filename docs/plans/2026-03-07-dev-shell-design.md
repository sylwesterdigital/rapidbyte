# `rapidbyte dev` Interactive Dev Shell Design

**Goal:** A terminal REPL for exploring sources, streaming data into an in-memory Arrow workspace, and querying with SQL — without writing pipeline YAML.

**MVP Scope:** Core explore-and-query loop (6 dot-commands + raw SQL). No destination, transforms, or pipeline export in v1.

## 1. MVP Commands

| Command | Purpose |
|---------|---------|
| `.source <connector> --host ... --port ...` | Connect to a source via Wasm connector |
| `.tables` | List discovered streams with sync mode and column count |
| `.schema <table>` | Show column names, types, nullability |
| `.stream <table> [--limit N]` | Stream rows into the Arrow workspace |
| `.workspace` | Show workspace tables with row counts and memory usage |
| `SELECT ...` (raw SQL) | Query workspace tables via host-side DataFusion |
| `.clear [table]` | Drop all tables (or one) from workspace |
| `.help` | List commands |
| `.quit` | Exit |

**Input dispatch:** Lines starting with `.` are dot-commands. Everything else is forwarded to DataFusion as SQL.

## 2. Architecture

```
crates/rapidbyte-dev/          # New crate (isolated deps)
  src/
    lib.rs                     # pub async fn run_repl() entry point
    repl.rs                    # reedline setup, input loop, dispatch
    commands/
      mod.rs                   # Command enum + parser
      source.rs                # .source — load connector, discover
      tables.rs                # .tables — list streams
      schema.rs                # .schema — column details
      stream.rs                # .stream — read into workspace
      workspace.rs             # .workspace / .clear
    workspace.rs               # ArrowWorkspace struct
    display.rs                 # Table rendering, styled output
    completer.rs               # reedline Completer
    highlighter.rs             # reedline Highlighter

crates/rapidbyte-cli/
  src/commands/dev.rs           # Dev subcommand (delegates to rapidbyte-dev)
```

**Dependencies for rapidbyte-dev:** reedline, datafusion, rapidbyte-engine, rapidbyte-runtime, rapidbyte-types, arrow (prettyprint), console, indicatif.

**Crate graph update:**
```
types (leaf)
  +-- state    -> types
  +-- runtime  -> types, state
  +-- sdk      -> types
  +-- engine   -> types, runtime, state
  +-- dev      -> engine, runtime, types   <-- NEW
      +-- cli  -> engine, runtime, types, dev
```

## 3. ArrowWorkspace

Single implicit workspace per session. All streamed tables go into it.

```rust
pub struct ArrowWorkspace {
    /// table_name -> collected batches
    tables: HashMap<String, Vec<RecordBatch>>,
    /// DataFusion session for SQL queries
    session: SessionContext,
}

impl ArrowWorkspace {
    pub fn new() -> Self { ... }

    /// Register batches as a DataFusion MemTable.
    pub fn insert(&mut self, name: &str, schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<()>;

    /// Execute SQL, return result batches.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>>;

    /// List tables with row count and memory size.
    pub fn summary(&self) -> Vec<TableSummary>;

    /// Drop one or all tables.
    pub fn clear(&mut self, table: Option<&str>);
}
```

When `.stream public.users` runs, batches accumulate in `tables["users"]` and get registered as a MemTable. Re-streaming the same table replaces the previous data.

## 4. Source Connection & Connector Lifecycle

`.source postgres --host localhost --port 5432 --user app --database mydb`

1. Parse connector name + remaining args into `serde_json::Value` config
2. Resolve connector Wasm path via `resolve_connector_path("source-postgres")`
3. Load Wasm module via `WasmRuntime` — **keep loaded for session**
4. Call `discover()` through existing runtime → get `Catalog`
5. Store in REPL state: `ConnectedSource { loaded_module, config, catalog }`
6. Print: `Connected — 3 streams discovered`

Subsequent `.stream` calls reuse the loaded module (no re-load overhead). A new `.source` call replaces the previous connection.

**Streaming flow:**
1. `.stream public.users --limit 1000`
2. Spawn blocking task: create new Wasm Store from loaded module, call `read()` for that stream with limit
3. Collect Arrow IPC batches from the channel, decode on host side
4. Insert into workspace, register as MemTable
5. Print: `1,000 rows -> workspace:users (12 columns, 847.2 KB)`

## 5. REPL UX

**Library:** reedline (Nushell's REPL engine). Provides history, completions, syntax highlighting, vi/emacs modes.

**Prompt:** `rb> ` — green when source connected, dim gray when no source.

**Startup banner:**
```
Rapidbyte Dev Shell v0.1.0
Type .help for commands, SQL to query workspace tables, .quit to exit
```

**Table display:** Arrow's `pretty_format_batches` (Unicode box-drawing) with row count footer:
```
+----+-------------------+---------------------+
| id | email             | created_at          |
+----+-------------------+---------------------+
| 42 | alice@example.com | 2026-01-15 10:30:00 |
| 87 | bob@example.com   | 2026-02-01 14:22:00 |
+----+-------------------+---------------------+
2 rows
```

**Styled output (consistent with existing CLI):**
- Green checkmark for success: `✓ Connected — 3 streams discovered`
- Red cross for errors: `✗ Connection failed: timeout`
- `format_bytes`/`format_count` for stats
- `indicatif` spinner during `.stream` operations

**Autocomplete (reedline Completer):**
- Dot-commands: `.source`, `.tables`, `.schema`, `.stream`, `.workspace`, `.clear`, `.help`, `.quit`
- Table names after `.schema`, `.stream`, `.clear`, `FROM`, `JOIN`
- Connector names after `.source`

**Syntax highlighting (reedline Highlighter):**
- SQL keywords: `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT` — bold
- Dot-commands: green
- String literals: yellow
- Numbers: cyan

## 6. Error Handling

All errors print to stderr with `✗` prefix and return to the prompt (never crash the REPL).

| Scenario | Behavior |
|----------|----------|
| `.source` connection failure | Print connector error, no source set |
| `.tables`/`.stream` without source | Print "No source connected. Use `.source` first." |
| `.stream` read failure | Print error, workspace unchanged |
| SQL parse/execution error | Print DataFusion error message |
| `.schema` unknown table | Print "Stream not found in catalog" |
| Ctrl+C during `.stream` | Cancel stream, keep partial data? No — discard, print "Cancelled" |
| Ctrl+C at prompt | Ignore (reedline handles) |
| Ctrl+D at prompt | Exit (same as `.quit`) |

## 7. Future Extensions (Not in MVP)

- `.dest` — connect destination, `.diff`, `.push`
- `.transform` — apply SQL transforms to workspace tables
- `.profile <table>.<column>` — column statistics
- `.check` — data quality rules
- `.export <file.yaml>` — serialize session as pipeline YAML
- Step-through debugger for existing pipeline YAMLs
- Named workspaces for comparing datasets
- `.stream --cursor-field` for incremental sync testing
