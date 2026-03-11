# Dev Shell UX Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the dev shell practical and robust by adding real table/stream completion, tightening `.stream` parsing, and aligning the `.source` coercion comment with actual behavior.

**Architecture:** Extend the dev-shell completer to accept dynamic context from the REPL, sourced from connected catalog streams and workspace table names. Keep completion prefix-based and command-specific, tighten the `.stream` grammar in the parser, and limit the REPL code change to feeding completion context plus correcting the stale comment.

**Tech Stack:** Rust workspace crates, reedline completer API, cargo test

---

### Task 1: Tighten `.stream` Parsing

**Files:**
- Modify: `crates/rapidbyte-dev/src/commands/mod.rs`
- Test: `crates/rapidbyte-dev/src/commands/mod.rs`

**Step 1: Write the failing test**

Add parser tests like:

```rust
#[test]
fn test_stream_rejects_unknown_flag() {
    let err = parse(".stream users --foo 1").unwrap().unwrap_err();
    assert!(err.contains("Unknown flag"));
}

#[test]
fn test_stream_rejects_extra_positional_argument() {
    let err = parse(".stream users extra").unwrap().unwrap_err();
    assert!(err.contains("Usage"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-dev test_stream_rejects_unknown_flag`

Expected: FAIL because the current parser ignores the unknown flag.

**Step 3: Write minimal implementation**

Update `parse_stream` so it:
- accepts only `--limit N`
- errors on any unknown flag
- errors on any extra positional token after the table name

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-dev test_stream_rejects_unknown_flag`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-dev/src/commands/mod.rs
git commit -m "fix: tighten dev shell stream parsing"
```

### Task 2: Add Workspace Table Accessor

**Files:**
- Modify: `crates/rapidbyte-dev/src/workspace.rs`
- Test: `crates/rapidbyte-dev/src/workspace.rs`

**Step 1: Write the failing test**

Add a unit test for a sorted table-name accessor:

```rust
#[test]
fn test_table_names_are_sorted() {
    let mut ws = ArrowWorkspace::new();
    // insert "orders" and "users"
    assert_eq!(ws.table_names(), vec!["orders".to_string(), "users".to_string()]);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-dev test_table_names_are_sorted`

Expected: FAIL because `table_names` does not exist.

**Step 3: Write minimal implementation**

Add:

```rust
#[must_use]
pub fn table_names(&self) -> Vec<String> {
    let mut names = self.tables.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-dev test_table_names_are_sorted`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-dev/src/workspace.rs
git commit -m "feat: expose sorted workspace table names"
```

### Task 3: Add Context-Aware Completion

**Files:**
- Modify: `crates/rapidbyte-dev/src/completer.rs`
- Modify: `crates/rapidbyte-dev/src/repl.rs`
- Test: `crates/rapidbyte-dev/src/completer.rs`

**Step 1: Write the failing test**

Add completer tests like:

```rust
#[test]
fn completes_stream_names_for_stream_command() {
    let mut completer = DevCompleter::new();
    completer.set_source_streams(vec!["public.users".to_string(), "public.orders".to_string()]);
    let suggestions = completer.complete(".stream public.u", ".stream public.u".len());
    assert!(suggestions.iter().any(|s| s.value == "public.users"));
}

#[test]
fn completes_workspace_tables_for_clear_command() {
    let mut completer = DevCompleter::new();
    completer.set_workspace_tables(vec!["users".to_string()]);
    let suggestions = completer.complete(".clear us", ".clear us".len());
    assert!(suggestions.iter().any(|s| s.value == "users"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-dev completes_stream_names_for_stream_command`

Expected: FAIL because the completer currently knows only dot-commands.

**Step 3: Write minimal implementation**

Extend `DevCompleter` with dynamic state and helpers such as:

```rust
pub fn refresh(&mut self, source_streams: Vec<String>, workspace_tables: Vec<String>) {
    self.source_streams = source_streams;
    self.workspace_tables = workspace_tables;
}
```

Update completion logic to:
- complete dot-commands in command position
- complete second-token names for `.schema`, `.stream`, and `.clear`

Then update `repl.rs` to refresh completer context from `ReplState` before each `read_line`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-dev completes_stream_names_for_stream_command`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-dev/src/completer.rs crates/rapidbyte-dev/src/repl.rs
git commit -m "feat: add dev shell table and stream completion"
```

### Task 4: Fix `.source` Coercion Comment

**Files:**
- Modify: `crates/rapidbyte-dev/src/repl.rs`

**Step 1: Write the failing test**

No new behavior test is needed because this task is comment-only and covered by code review.

**Step 2: Verify current mismatch**

Inspect the comment and implementation in `handle_source`.

Expected: comment says `f64`, implementation parses `bool`.

**Step 3: Write minimal implementation**

Replace the stale comment with one that matches the actual coercion order:

```rust
// Try to parse as i64, then bool, then fallback to string.
```

**Step 4: Run targeted tests to verify no regressions**

Run: `cargo test -p rapidbyte-dev`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-dev/src/repl.rs
git commit -m "docs: align dev shell source coercion comment"
```

### Task 5: Full Verification

**Files:**
- Verify only

**Step 1: Run targeted dev-shell tests**

Run:

```bash
cargo test -p rapidbyte-dev
```

Expected: PASS

**Step 2: Run workspace lint/tests affected by the tranche**

Run:

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rapidbyte-benchmarks -p rapidbyte-sdk -p rapidbyte-runtime -p rapidbyte-engine -p rapidbyte-cli -p rapidbyte-dev
```

Expected: PASS

**Step 3: Commit verification-only changes if needed**

If no files changed, no commit.
