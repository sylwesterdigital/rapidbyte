# Signing Key and CLI Routing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Require an explicit opt-in for the insecure default signing key and restore local-by-default `run` semantics.

**Architecture:** Add explicit insecure-signing-key escape hatches to controller and agent startup/config builders, and split controller URL resolution so only explicitly distributed commands use config-file fallback.

**Tech Stack:** Rust, Clap, rapidbyte-cli, rapidbyte-controller, rapidbyte-agent

---

### Task 1: Add the failing signing-key validation tests

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`

**Step 1: Write the failing tests**

Add tests showing:
- controller config rejects the default signing key unless `allow_insecure_default_signing_key` is true
- agent config rejects the default signing key unless `allow_insecure_default_signing_key` is true

**Step 2: Run tests to verify they fail**

Run:
- `cargo test -p rapidbyte-cli controller_execute_rejects_default_signing_key`
- `cargo test -p rapidbyte-cli agent_execute_rejects_default_signing_key`

Expected: FAIL

**Step 3: Implement the minimal validation**

Add the explicit CLI flag and config-builder validation for both subcommands.

**Step 4: Re-run tests**

Expected: PASS

### Task 2: Add the failing controller-routing tests

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Write the failing tests**

Add tests for controller resolution:
- `run` ignores config-file fallback
- control-plane commands still use config-file fallback

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-cli controller_url_resolution`

Expected: FAIL

**Step 3: Implement the minimal routing change**

Split controller URL resolution so `run` only uses explicit CLI/env controller, while distributed control-plane commands keep config fallback.

**Step 4: Re-run tests**

Expected: PASS

### Task 3: Verify integration and lint

**Files:**
- Modify integration tests if they relied on the insecure default signing key

**Step 1: Run package tests**

Run:
- `cargo test -p rapidbyte-cli`
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-agent`

**Step 2: Run lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS
