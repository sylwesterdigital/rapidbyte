# Transform Validate Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor `connectors/transform-validate` to align with Rapidbyte coding-style correctness and performance rules while preserving intended connector behavior.

**Architecture:** Keep connector interface stable and refactor internals in `config.rs` and `transform.rs` into explicit compile/evaluation helpers. Preserve deterministic row-level semantics with full failure aggregation and policy-driven handling. Replace lossy decimal range comparisons with scale-aware decimal comparisons and keep non-finite float rejection explicit.

**Tech Stack:** Rust, Arrow, Arrow JSON, regex, serde, rapidbyte-sdk.

---

### Task 1: Reduce Config Compilation Allocation and Duplication

**Files:**
- Modify: `connectors/transform-validate/src/config.rs`
- Test: `connectors/transform-validate/src/config.rs`

**Step 1: Write the failing test**

Add compile tests for empty/whitespace field selectors in `FieldSelector::Many` and map-form regex selectors with empty keys.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml compile_`
Expected: new selector-validation test fails before refactor.

**Step 3: Write minimal implementation**

Refactor selector expansion helpers to iterator-style APIs and centralize non-empty field validation so compile logic avoids clone-heavy temporary vectors and validates all selector forms consistently.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml compile_`
Expected: selector-validation and existing compile tests pass.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/config.rs
git commit -m "refactor(transform-validate): simplify config rule expansion"
```

### Task 2: Refactor Batch Evaluation Structure

**Files:**
- Modify: `connectors/transform-validate/src/transform.rs`
- Test: `connectors/transform-validate/src/transform.rs`

**Step 1: Write the failing test**

Add a row-level test ensuring missing columns across multiple rules report all failure reasons in one message.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml evaluate_`
Expected: new aggregate-failure test fails before helper refactor.

**Step 3: Write minimal implementation**

Extract shared rule-evaluation helpers and centralize failure construction to remove duplicated branch logic while preserving deterministic failure aggregation.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml evaluate_`
Expected: new and existing evaluation tests pass.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/transform.rs
git commit -m "refactor(transform-validate): centralize rule evaluation flow"
```

### Task 3: Make Decimal Range Validation Scale-Aware

**Files:**
- Modify: `connectors/transform-validate/src/transform.rs`
- Test: `connectors/transform-validate/src/transform.rs`

**Step 1: Write the failing test**

Add decimal range tests covering exact boundary values and large-scale values that could be misclassified by `f64` conversion.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml decimal`
Expected: new decimal precision test fails before scale-aware comparison.

**Step 3: Write minimal implementation**

Replace decimal-to-f64 conversion path with typed decimal comparisons for `Decimal128` and `Decimal256`, parsing configured bounds into comparable decimal representations.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml decimal`
Expected: decimal tests pass with exact-bound and high-scale behavior.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/transform.rs
git commit -m "fix(transform-validate): use scale-aware decimal range checks"
```

### Task 4: End-to-End Connector Verification

**Files:**
- Modify: `connectors/transform-validate/src/config.rs`
- Modify: `connectors/transform-validate/src/transform.rs`

**Step 1: Run format checks**

Run: `just fmt`
Expected: formatting checks pass.

**Step 2: Run lint checks**

Run: `just lint`
Expected: lint checks pass for affected code.

**Step 3: Run connector tests**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml`
Expected: all connector unit tests pass.

**Step 4: Record verification evidence**

Capture command list and status for PR notes.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/config.rs connectors/transform-validate/src/transform.rs
git commit -m "refactor(transform-validate): align validation internals with coding style blueprint"
```
