# Connector Coding Style Conformance Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring all 4 connectors (`source-postgres`, `dest-postgres`, `transform-sql`, `transform-validate`) into full conformance with `CODING_STYLE.md`.

**Architecture:** Documentation and annotation additions only — no behavioral changes. Each connector is updated independently with its own build verification and commit. Source-postgres is the reference connector with the fewest gaps.

**Tech Stack:** Rust, wasm32-wasip2 target, `cargo clippy`, `cargo build`

---

## Gap Analysis Summary

| Connector | Module Docstrings | `# Errors` Docs | `#[must_use]` | Import Ordering | Config `validate()` |
|-----------|:-:|:-:|:-:|:-:|:-:|
| source-postgres | all present | 1 of ~3 `pub` fns | good | good | present |
| dest-postgres | all present | 0 of ~5 `pub` fns | **none** | 2 files mixed | **missing** (validates via client) |
| transform-sql | all present | 0 of ~3 `pub` fns | **none** | good | uses `normalized_query()` |
| transform-validate | **none (0/3)** | 0 of ~2 `pub` fns | **none** | good | uses `compile()` |

Priority order: transform-validate first (MUST violations), then dest-postgres, transform-sql, source-postgres.

---

### Task 1: Add module docstrings to transform-validate

**Files:**
- Modify: `connectors/transform-validate/src/main.rs:1` (insert before `mod config;`)
- Modify: `connectors/transform-validate/src/config.rs:1` (insert before `use std::collections::BTreeMap;`)
- Modify: `connectors/transform-validate/src/transform.rs:1` (insert before `use std::collections::{BTreeMap, HashMap};`)

**Step 1: Add docstring to `main.rs`**

Insert at line 1, before `mod config;`:

```rust
//! Validation transform connector for Rapidbyte.
//!
//! Applies data quality assertion rules (not-null, regex, range,
//! uniqueness) to Arrow batches, enforcing data contracts in-flight.

mod config;
```

**Step 2: Add docstring to `config.rs`**

Insert at line 1, before `use std::collections::BTreeMap;`:

```rust
//! Validation transform configuration and rule compilation.

use std::collections::BTreeMap;
```

**Step 3: Add docstring to `transform.rs`**

Insert at line 1, before `use std::collections::{BTreeMap, HashMap};`:

```rust
//! Rule evaluation engine for the validation transform.
//!
//! Evaluates compiled validation rules against Arrow `RecordBatch`es
//! and applies the configured error policy (`fail`/`skip`/`dlq`).

use std::collections::{BTreeMap, HashMap};
```

**Step 4: Build to verify**

Run: `cd connectors/transform-validate && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/main.rs connectors/transform-validate/src/config.rs connectors/transform-validate/src/transform.rs
git commit -m "docs(transform-validate): add module-level docstrings per coding style §4.1"
```

---

### Task 2: Add `# Errors` docs to transform-validate public functions

**Files:**
- Modify: `connectors/transform-validate/src/config.rs:206` (`compile()`)
- Modify: `connectors/transform-validate/src/transform.rs:26` (`run()`)

**Step 1: Add `# Errors` to `Config::compile()`**

At `config.rs:206`, add doc comment above `pub fn compile`:

```rust
    /// Compile raw rule specs into an optimized `CompiledConfig`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if any rule references an invalid field selector,
    /// contains an invalid regex pattern, or has an empty rule list.
    pub fn compile(&self) -> Result<CompiledConfig, String> {
```

**Step 2: Add `# Errors` to `transform::run()`**

At `transform.rs:26`, add doc comment above `pub async fn run`:

```rust
/// Run validation rules against all incoming batches for a single stream.
///
/// # Errors
///
/// Returns `Err` if a rule evaluation encounters a failing row and the
/// stream's error policy is [`DataErrorPolicy::Fail`].
pub async fn run(
```

**Step 3: Build to verify**

Run: `cd connectors/transform-validate && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 4: Commit**

```bash
git add connectors/transform-validate/src/config.rs connectors/transform-validate/src/transform.rs
git commit -m "docs(transform-validate): add # Errors sections per coding style §14"
```

---

### Task 3: Add `#[must_use]` and `# Errors` to dest-postgres pure functions

**Files:**
- Modify: `connectors/dest-postgres/src/decode.rs` (lines 28, 35, 45, 85, 126)
- Modify: `connectors/dest-postgres/src/type_map.rs` (lines 6, 53)

**Step 1: Add `#[must_use]` to `decode.rs` pure functions**

Add `#[must_use]` annotation above each of these functions:

At line 28 (`qualified_name`):
```rust
#[must_use]
pub(crate) fn qualified_name(schema: &str, table: &str) -> String {
```

At line 35 (`active_column_indices`):
```rust
#[must_use]
pub(crate) fn active_column_indices(
```

At line 45 (`build_upsert_clause`):
```rust
#[must_use]
pub(crate) fn build_upsert_clause(
```

At line 85 (`type_null_flags`):
```rust
#[must_use]
pub(crate) fn type_null_flags(
```

**Step 2: Add `# Errors` to `downcast_columns` in `decode.rs`**

At line 126, above `pub(crate) fn downcast_columns`:
```rust
/// Pre-downcast active columns from a `RecordBatch` into `TypedCol` references.
///
/// # Errors
///
/// Returns `Err` if a column's Arrow data type is unsupported by dest-postgres.
pub(crate) fn downcast_columns<'a>(
```

**Step 3: Add `#[must_use]` to `type_map.rs` pure functions**

At line 6 (`arrow_to_pg_type`):
```rust
#[must_use]
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
```

At line 53 (`pg_types_compatible`):
```rust
#[must_use]
pub(crate) fn pg_types_compatible(info_schema_type: &str, ddl_type: &str) -> bool {
```

**Step 4: Build to verify**

Run: `cd connectors/dest-postgres && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/decode.rs connectors/dest-postgres/src/type_map.rs
git commit -m "style(dest-postgres): add #[must_use] and # Errors per coding style §6.2/§14"
```

---

### Task 4: Add `# Errors` to dest-postgres client functions

**Files:**
- Modify: `connectors/dest-postgres/src/client.rs` (lines 8, 35)

**Step 1: Add `# Errors` to `connect()`**

At `client.rs:8`, above `pub(crate) async fn connect`:
```rust
/// Connect to `PostgreSQL` using the provided config.
///
/// # Errors
///
/// Returns `Err` if the TCP connection via `HostTcpStream` fails or
/// `PostgreSQL` authentication is rejected.
pub(crate) async fn connect(config: &crate::config::Config) -> Result<Client, String> {
```

**Step 2: Add `# Errors` to `validate()`**

At `client.rs:35`, above `pub(crate) async fn validate`:
```rust
/// Validate `PostgreSQL` connectivity and target schema existence.
///
/// # Errors
///
/// Returns `Err` if the connection cannot be established or the test
/// query `SELECT 1` fails.
pub(crate) async fn validate(
```

**Step 3: Build to verify**

Run: `cd connectors/dest-postgres && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 4: Commit**

```bash
git add connectors/dest-postgres/src/client.rs
git commit -m "docs(dest-postgres): add # Errors to client functions per coding style §14"
```

---

### Task 5: Fix import ordering in dest-postgres writer.rs

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs:6-18`

**Step 1: Reorder imports**

Replace lines 6-18:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::catalog::SchemaHint;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::ddl::{prepare_staging, swap_staging_table};
use crate::decode;
```

With:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use tokio_postgres::Client;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::catalog::SchemaHint;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::ddl::{prepare_staging, swap_staging_table};
use crate::decode;
```

**Step 2: Build to verify**

Run: `cd connectors/dest-postgres && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 3: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs
git commit -m "style(dest-postgres): fix import ordering in writer.rs per coding style §5"
```

---

### Task 6: Add `# Errors` and `#[must_use]` to transform-sql

**Files:**
- Modify: `connectors/transform-sql/src/config.rs:15` (`normalized_query()`)
- Modify: `connectors/transform-sql/src/transform.rs:16` (`run()`)

**Step 1: Add docs to `Config::normalized_query()`**

At `config.rs:15`, above `pub fn normalized_query`:
```rust
    /// Normalize the SQL query by trimming whitespace.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the query is empty or contains only whitespace.
    #[must_use = "returns normalized query, does not modify self"]
    pub fn normalized_query(&self) -> Result<String, String> {
```

Note: `#[must_use]` on a Result-returning function is redundant (compiler warns on unused Results), but we use the message form to document intent.

Actually, skip the `#[must_use]` here — Rust already warns on unused `Result`. Just add the `# Errors` doc.

At `config.rs:15`, above `pub fn normalized_query`:
```rust
    /// Normalize the SQL query by trimming whitespace.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the query is empty or contains only whitespace.
    pub fn normalized_query(&self) -> Result<String, String> {
```

**Step 2: Add `# Errors` to `transform::run()`**

At `transform.rs:15-16`, the function already has a doc comment `/// Run the SQL transform for a single stream.`. Add `# Errors`:

```rust
/// Run the SQL transform for a single stream.
///
/// # Errors
///
/// Returns `Err` if the SQL query fails to execute against a batch or
/// downstream batch emission fails.
pub async fn run(
```

**Step 3: Build to verify**

Run: `cd connectors/transform-sql && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 4: Commit**

```bash
git add connectors/transform-sql/src/config.rs connectors/transform-sql/src/transform.rs
git commit -m "docs(transform-sql): add # Errors sections per coding style §14"
```

---

### Task 7: Add missing `# Errors` to source-postgres

**Files:**
- Modify: `connectors/source-postgres/src/discovery.rs:11-12`

**Step 1: Add `# Errors` to `discover_catalog()`**

At `discovery.rs:11`, the function already has `/// Discover all base tables and columns in the public schema.`. Add `# Errors`:

```rust
/// Discover all base tables and columns in the `public` schema.
///
/// # Errors
///
/// Returns `Err` if the `information_schema` query fails or result
/// parsing encounters an unexpected column type.
pub async fn discover_catalog(client: &Client) -> Result<Vec<Stream>, String> {
```

**Step 2: Build to verify**

Run: `cd connectors/source-postgres && cargo clippy --target wasm32-wasip2 -- -D warnings 2>&1 | tail -5`
Expected: `Finished` with no warnings

**Step 3: Commit**

```bash
git add connectors/source-postgres/src/discovery.rs
git commit -m "docs(source-postgres): add # Errors to discover_catalog per coding style §14"
```

---

### Task 8: Full cross-connector verification

**Step 1: Build all connectors**

Run: `just build-connectors 2>&1 | tail -10`
Expected: All connectors build successfully

**Step 2: Run host tests**

Run: `just test 2>&1 | tail -10`
Expected: All tests pass

**Step 3: Verify no regressions**

Run: `just fmt && just lint 2>&1 | tail -5`
Expected: Clean output

---

## Out-of-Scope Items (Noted for Future)

These items were identified during analysis but are behavioral changes, not documentation/annotation fixes:

1. **dest-postgres `Config::validate()`** — The config struct has no `validate()` method (§9.2 SHOULD). Validation is done entirely via `client::validate()`. Adding a config-level `validate()` would require deciding which fields to check (e.g., schema name format, port range). This is a behavioral addition.

2. **transform-validate manual `ConfigSchema`** — The config uses a hand-written `ConfigSchema` implementation instead of `#[derive(ConfigSchema)]` (§9.2 MUST). This is intentional — the JSON Schema is complex with `oneOf` discriminators that the derive macro doesn't support. No change needed.

3. **transform-sql `datafusion::prelude::*`** — This glob import is consistent with the established `prelude::*` exception pattern (§5 / §8.3). No change needed.

4. **cdc/mod.rs selective test imports** — `source-postgres/src/cdc/mod.rs` test module uses selective imports instead of `use super::*`. This is a minor deviation from §13.1 but acceptable.
