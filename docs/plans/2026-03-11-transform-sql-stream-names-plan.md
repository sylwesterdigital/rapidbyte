# Transform SQL Stream Names Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the `transform-sql` plugin’s legacy `input` table contract with stream-name-based SQL so queries must reference the current pipeline stream name, such as `users`.

**Architecture:** Split SQL validation into syntax validation and stream-name validation, using the runtime `Context` to enforce that queries reference the current stream table. Update transform execution to register batches under the stream name instead of `input`, then migrate tests, examples, and docs to the new contract with no backward-compatibility alias.

**Tech Stack:** Rust, Apache DataFusion, sqlparser, Tokio, e2e harness tests, Markdown docs.

---

### Task 1: Add failing plugin tests for stream-name validation

**Files:**
- Modify: `plugins/transforms/sql/src/main.rs`
- Modify: `plugins/transforms/sql/src/config.rs`
- Test: `plugins/transforms/sql/src/main.rs`
- Test: `plugins/transforms/sql/src/config.rs`

**Step 1: Write the failing tests**

Add unit tests that assert:

- init succeeds for syntactically valid `SELECT * FROM users`
- validate succeeds when `Context::new(..., "users")` is paired with
  `SELECT * FROM users`
- validate fails when the query references a different table name
- validate fails for legacy `FROM input`

Example test shape:

```rust
#[tokio::test]
async fn validate_succeeds_when_query_references_current_stream_name() {
    let ctx = Context::new("transform-sql", "users");
    let validation = TransformSql::validate(
        &config::Config {
            query: "SELECT * FROM users".to_string(),
        },
        &ctx,
    )
    .await
    .expect("validate should not return plugin error");

    assert_eq!(validation.status, ValidationStatus::Success);
}

#[tokio::test]
async fn validate_fails_for_legacy_input_table_name() {
    let ctx = Context::new("transform-sql", "users");
    let validation = TransformSql::validate(
        &config::Config {
            query: "SELECT * FROM input".to_string(),
        },
        &ctx,
    )
    .await
    .expect("validate should not return plugin error");

    assert_eq!(validation.status, ValidationStatus::Failed);
    assert!(validation.message.contains("users"));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_succeeds_when_query_references_current_stream_name
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_fails_for_legacy_input_table_name
```

Expected:
- FAIL because validation still hardcodes `input`

**Step 3: Write minimal implementation**

Update `plugins/transforms/sql/src/main.rs` and `plugins/transforms/sql/src/config.rs` so:

- init only normalizes and parses SQL; it no longer hard-requires `input`
- validation checks whether the parsed query references `ctx.stream_name()`
- messages reference the current stream table instead of `input`

Keep single-statement and parse validation intact.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_succeeds_when_query_references_current_stream_name
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_fails_for_legacy_input_table_name
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/transforms/sql/src/main.rs plugins/transforms/sql/src/config.rs
git commit -m "feat(transform-sql): validate queries against stream names"
```

### Task 2: Register DataFusion tables under the stream name

**Files:**
- Modify: `plugins/transforms/sql/src/transform.rs`
- Modify: `plugins/transforms/sql/src/main.rs`
- Test: `plugins/transforms/sql/src/main.rs`

**Step 1: Write the failing test**

Add a focused runtime-oriented test that proves old `FROM input` no longer
plans at runtime for a `users` stream, while `FROM users` does.

If a direct runtime unit test is awkward, add a test around the validation and
planning helper seam that uses `users` as the only registered table name.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml init_rejects_legacy_input_table_at_runtime_contract
```

Expected:
- FAIL because execution still registers a table named `input`

**Step 3: Write minimal implementation**

Update `plugins/transforms/sql/src/transform.rs` to:

- derive `let stream_name = ctx.stream_name();`
- deregister the previous table using that stream name
- register the MemTable under that stream name
- plan against that stream-named table only

Update any helper comments and internal messages accordingly.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml init_rejects_legacy_input_table_at_runtime_contract
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/transforms/sql/src/transform.rs plugins/transforms/sql/src/main.rs
git commit -m "feat(transform-sql): register batch tables by stream name"
```

### Task 3: Migrate e2e tests to the new SQL contract

**Files:**
- Modify: `tests/e2e/tests/transform.rs`
- Modify: `tests/e2e/src/harness/mod.rs`
- Test: `tests/e2e/tests/transform.rs`

**Step 1: Write the failing test**

Update or add e2e coverage so that:

- the happy-path transform query uses the actual stream name, for example
  `FROM users`
- the invalid-query test explicitly uses `FROM input` and expects failure

Example expectations:

```rust
context
    .run_transform_pipeline(
        &schemas,
        r#"SELECT id, UPPER(name) AS name_upper FROM users WHERE id % 2 = 1"#,
        &state_path,
    )
    .await
    .expect("transform pipeline should succeed");

let run = context
    .run_transform_pipeline(&schemas, r#"SELECT * FROM input"#, &state_path)
    .await;
assert!(run.is_err(), "legacy input table should fail");
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --manifest-path tests/e2e/Cargo.toml --test transform sql_transform_filters_and_projects_expected_rows
cargo test --manifest-path tests/e2e/Cargo.toml --test transform sql_transform_rejects_legacy_input_table_name
```

Expected:
- FAIL until runtime and harness expectations align with stream-name SQL

**Step 3: Write minimal implementation**

Update the e2e harness or helpers only if needed so tests can refer to the
stream name predictably. Do not add backward compatibility.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path tests/e2e/Cargo.toml --test transform sql_transform_filters_and_projects_expected_rows
cargo test --manifest-path tests/e2e/Cargo.toml --test transform sql_transform_rejects_legacy_input_table_name
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add tests/e2e/tests/transform.rs tests/e2e/src/harness/mod.rs
git commit -m "test(transform-sql): switch e2e queries to stream names"
```

### Task 4: Update first-party examples and benchmark fixtures

**Files:**
- Modify: `benchmarks/src/pipeline.rs`
- Modify: `docs/PROTOCOL.md`
- Modify: `docs/PLUGIN_DEV.md`
- Modify: `docs/DRAFT.md`

**Step 1: Write the failing tests**

Add or update unit tests in `benchmarks/src/pipeline.rs` so any rendered SQL
transform example uses a real stream name instead of `input`.

Example:

```rust
assert_eq!(parsed.transforms[0].config["query"], "SELECT * FROM bench_events");
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks pipeline::tests::renders_transform_sections_for_pipeline_benchmarks
```

Expected:
- FAIL because benchmark examples still render `SELECT * FROM input`

**Step 3: Write minimal implementation**

Replace first-party examples and docs so they consistently use stream names
such as `users` or `bench_events`.

Remove all normative wording that says the table is always named `input`.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks pipeline::tests::renders_transform_sections_for_pipeline_benchmarks
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/pipeline.rs docs/PROTOCOL.md docs/PLUGIN_DEV.md docs/DRAFT.md
git commit -m "docs(transform-sql): replace input table examples with stream names"
```

### Task 5: Full verification

**Files:**
- Modify: none
- Test: `plugins/transforms/sql/src/main.rs`
- Test: `tests/e2e/tests/transform.rs`
- Test: `benchmarks/src/pipeline.rs`

**Step 1: Run plugin tests**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml
```

Expected:
- PASS

**Step 2: Run benchmark crate tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected:
- PASS

**Step 3: Run transform e2e tests**

Run:

```bash
cargo test --manifest-path tests/e2e/Cargo.toml --test transform
```

Expected:
- PASS

**Step 4: Smoke-check a real transform pipeline if practical**

Run a local transform pipeline or existing harness command using `FROM users`.

Expected:
- succeeds with the stream-name SQL contract

**Step 5: Final status check**

Run:

```bash
git status --short
```

Expected:
- only intended tracked changes remain before review or merge
