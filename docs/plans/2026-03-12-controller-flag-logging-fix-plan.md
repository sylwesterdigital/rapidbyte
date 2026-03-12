# Controller Flag Rename and Logging Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename the insecure signing override flag to `--allow-insecure-signing-key` and make `-v` / `-vv` drive default tracing output for long-running CLI commands like `controller` and `agent`.

**Architecture:** Keep the change at the CLI layer. Rename the clap/user-facing flag and update its validation paths, then change the shared logging initializer so it derives a default `tracing` filter from CLI verbosity unless `RUST_LOG` is explicitly set.

**Tech Stack:** Rust, clap, tracing-subscriber, Markdown docs

---

### Task 1: Rename the insecure signing override flag

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: docs/examples that reference the old flag name

**Step 1: Write/adjust failing tests**

- Update or add command/config tests so they expect:
  - `--allow-insecure-signing-key`
  - error/help strings that use the new name
- Add a negative assertion if practical that the old string is no longer
  present in help/error output.

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli controller_execute_allows_insecure_default_signing_key
cargo test -p rapidbyte-cli agent_execute_allows_insecure_default_signing_key
```

Expected: FAIL or require test updates because the old spelling is still wired.

**Step 3: Implement the rename**

- Rename the clap flag in `main.rs`
- Update controller/agent validation strings and plumbing
- Update tests/docs/help expectations to the new name

**Step 4: Re-run targeted tests**

Run:

```bash
cargo test -p rapidbyte-cli controller_execute
cargo test -p rapidbyte-cli agent_execute
```

Expected: PASS for the updated flag/config tests.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/controller.rs crates/rapidbyte-cli/src/commands/agent.rs
git commit -m "fix: rename insecure signing override flag"
```

### Task 2: Make verbosity drive default tracing behavior

**Files:**
- Modify: `crates/rapidbyte-cli/src/logging.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Test: `crates/rapidbyte-cli/src/logging.rs` or existing CLI tests

**Step 1: Write the failing tests**

- Add tests for logging filter resolution covering:
  - no `RUST_LOG` + default verbosity
  - no `RUST_LOG` + `-v`
  - no `RUST_LOG` + `-vv`
  - explicit `RUST_LOG` overriding the derived default

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli logging
```

Expected: FAIL before the logging initializer is updated.

**Step 3: Implement minimal logging change**

- Refactor the logging helper to derive a default `EnvFilter` from verbosity
- Keep `RUST_LOG` precedence intact
- Ensure initialization still happens once near CLI startup

**Step 4: Re-run targeted tests**

Run:

```bash
cargo test -p rapidbyte-cli logging
```

Expected: PASS for filter-resolution coverage.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/logging.rs crates/rapidbyte-cli/src/main.rs
git commit -m "fix: derive tracing defaults from verbosity"
```

### Task 3: Update docs and verify end-to-end behavior

**Files:**
- Modify: `README.md`
- Modify: other user-facing docs/examples if they mention the old flag

**Step 1: Update docs**

- Replace the old flag spelling in README/docs/examples
- Ensure wording around verbosity/logging is not misleading for controller/agent

**Step 2: Run verification**

Run:

```bash
cargo test -p rapidbyte-cli
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS.

**Step 3: Optional manual smoke check**

Run:

```bash
./target/release/rapidbyte controller --allow-unauthenticated --allow-insecure-signing-key -vv
```

Expected: startup logs visible without requiring `RUST_LOG`.

**Step 4: Commit**

```bash
git add README.md crates/rapidbyte-cli/src/logging.rs crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/controller.rs crates/rapidbyte-cli/src/commands/agent.rs
git commit -m "docs: align controller flag and logging behavior"
```
