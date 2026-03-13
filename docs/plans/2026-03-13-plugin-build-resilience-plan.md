# Plugin Build Resilience Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `just build-all` recover automatically from stale plugin target
state and verify that every checked-in plugin builds successfully.

**Architecture:** Add a plugin build wrapper script that fingerprints the
effective Rust build environment and invalidates stale plugin build-script state
when that environment changes. Route plugin builds through the wrapper in
`Justfile`, then verify the regression path and the normal all-plugin build
path.

**Tech Stack:** Bash, Cargo, Justfile, repo-local Rust wrapper configuration.

---

### Task 1: Add a failing regression harness for stale plugin build state

**Files:**
- Create: `tests/test_plugin_build_resilience.sh`
- Reference: `scripts/rustc-wrapper.sh`
- Reference: `plugins/sources/postgres/Cargo.toml`

**Step 1: Write the regression script**

Implement a Bash test that:
- creates a temporary `CARGO_TARGET_DIR`
- builds `plugins/sources/postgres` once with `RUSTC_BOOTSTRAP=1`
- rebuilds the same plugin on stable without `RUSTC_BOOTSTRAP`
- initially demonstrates the stale-state failure when using plain Cargo from the
  plugin directory

**Step 2: Run the regression script to verify it fails**

Run: `bash tests/test_plugin_build_resilience.sh`
Expected: non-zero exit with the stable `thiserror`/`E0554` failure reproduced

**Step 3: Commit**

```bash
git add tests/test_plugin_build_resilience.sh
git commit -m "test: reproduce stale plugin build-state failure"
```

### Task 2: Add the resilient plugin build wrapper

**Files:**
- Create: `scripts/build-plugin.sh`
- Modify: `Justfile`
- Reference: `docs/BUILD.md`

**Step 1: Write the wrapper script**

Implement a Bash script that:
- accepts plugin path and build mode/profile inputs
- computes a stable build-environment fingerprint
- stores it in plugin-local target metadata
- removes stale `target/*/build` state when the fingerprint changes
- invokes `cargo build` for the plugin

**Step 2: Route `Justfile` plugin builds through the wrapper**

Replace the four inline `cd plugins/... && cargo build ...` lines with wrapper
invocations so all plugin builds use the same invalidation behavior.

**Step 3: Run the regression script to verify it now passes**

Run: `bash tests/test_plugin_build_resilience.sh`
Expected: exit code `0`

**Step 4: Commit**

```bash
git add scripts/build-plugin.sh Justfile tests/test_plugin_build_resilience.sh
git commit -m "fix: harden plugin builds against stale target state"
```

### Task 3: Document and verify all plugin builds

**Files:**
- Modify: `docs/BUILD.md`

**Step 1: Update build documentation**

Document:
- that plugin builds now self-heal stale target state
- that the cleanup is triggered only when the build environment changes
- that `just build-all` remains the supported entrypoint

**Step 2: Verify all plugins build from `Justfile`**

Run: `just _build-plugins`
Expected: success for source, destination, and both transform plugins

**Step 3: Verify warm-cache rebuild also succeeds**

Run: `just _build-plugins`
Expected: success again without manual cleanup

**Step 4: Run repo verification**

Run:
- `cargo test`
- `bash tests/test_plugin_build_resilience.sh`
- `just _build-plugins`

Expected: all exit code `0`

**Step 5: Commit**

If documentation or verification caused follow-up edits:

```bash
git add docs/BUILD.md scripts/build-plugin.sh Justfile tests/test_plugin_build_resilience.sh
git commit -m "docs: document resilient plugin build flow"
```
