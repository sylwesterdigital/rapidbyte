# Repo-Wide Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Consolidate repo-wide formatting helpers, remove dead/awkward progress and dry-run placeholder shapes, and harden scaffolded plugins with CI enforcement against committed stub markers.

**Architecture:** Move only the duplicated low-level formatting helpers into `rapidbyte-types`, keeping higher-level CLI presentation logic in place. Simplify the progress API to the events the engine actually emits, make dry-run results valid at construction time, and add scaffold hardening in both generated output and CI.

**Tech Stack:** Rust workspace crates, GitHub Actions CI, small shell script guard, cargo test, cargo clippy

---

### Task 1: Shared Formatting Helpers

**Files:**
- Create: `crates/rapidbyte-types/src/format.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`
- Modify: `crates/rapidbyte-cli/src/output/format.rs`
- Modify: `crates/rapidbyte-dev/src/display.rs`
- Test: `crates/rapidbyte-types/src/format.rs`
- Test: `crates/rapidbyte-cli/src/output/format.rs`

**Step 1: Write the failing test**

Add tests in `crates/rapidbyte-types/src/format.rs` for:

```rust
#[test]
fn format_bytes_binary_uses_binary_unit_labels() {
    assert_eq!(format_bytes_binary(1_048_576), "1.0 MiB");
    assert_eq!(format_bytes_binary(1_073_741_824), "1.0 GiB");
}

#[test]
fn format_count_inserts_grouping_separators() {
    assert_eq!(format_count(1_200_000), "1,200,000");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-types format_bytes_binary_uses_binary_unit_labels -- --exact`

Expected: FAIL because `crates/rapidbyte-types/src/format.rs` and the tested functions do not exist yet.

**Step 3: Write minimal implementation**

Create `crates/rapidbyte-types/src/format.rs`:

```rust
#![allow(clippy::cast_precision_loss)]

pub fn format_bytes_binary(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_count(n: u64) -> String {
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
```

Re-export it from `crates/rapidbyte-types/src/lib.rs`, then update CLI/dev-shell call sites to use the shared helpers instead of local copies. Keep `format_rate`, `format_byte_rate`, and `format_duration` in the CLI crate.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-types format_bytes_binary_uses_binary_unit_labels -- --exact`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/format.rs crates/rapidbyte-types/src/lib.rs crates/rapidbyte-cli/src/output/format.rs crates/rapidbyte-dev/src/display.rs
git commit -m "refactor: share binary formatting helpers"
```

### Task 2: Progress Event Cleanup

**Files:**
- Modify: `crates/rapidbyte-engine/src/progress.rs`
- Modify: `crates/rapidbyte-cli/src/output/progress.rs`
- Test: `crates/rapidbyte-cli/src/output/progress.rs`

**Step 1: Write the failing test**

Add or update a unit test that pattern-matches only on the supported progress events and fails compilation if `ProgressEvent::Error` is still expected by the spinner logic.

Use a compile-targeted behavior test like:

```rust
#[test]
fn spinner_handles_running_events_without_error_variant_branch() {
    let _event = ProgressEvent::Retry {
        attempt: 1,
        max_retries: 3,
        message: "retry".to_string(),
        delay_secs: 1.0,
    };
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-cli spinner_handles_running_events_without_error_variant_branch -- --exact`

Expected: FAIL to compile or FAIL because the current progress code still matches the removed/dead branch.

**Step 3: Write minimal implementation**

Remove `ProgressEvent::Error` from the engine progress model and delete the dead match arm in `crates/rapidbyte-cli/src/output/progress.rs`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-cli spinner_handles_running_events_without_error_variant_branch -- --exact`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/progress.rs crates/rapidbyte-cli/src/output/progress.rs
git commit -m "refactor: remove dead progress error event"
```

### Task 3: Dry-Run Result Construction

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/execution.rs`
- Test: `crates/rapidbyte-engine/src/orchestrator.rs`

**Step 1: Write the failing test**

Add a unit test near `collect_dry_run_frames` that asserts the returned `DryRunStreamResult` already carries the stream name:

```rust
#[test]
fn collect_dry_run_frames_preserves_stream_name() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    drop(tx);
    let result = collect_dry_run_frames("public.users", &rx, None, None).expect("dry run");
    assert_eq!(result.stream_name, "public.users");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine collect_dry_run_frames_preserves_stream_name -- --exact`

Expected: FAIL because `collect_dry_run_frames` does not currently accept a stream name and returns an empty placeholder string.

**Step 3: Write minimal implementation**

Change the function signature to:

```rust
fn collect_dry_run_frames(
    stream_name: &str,
    receiver: &sync_mpsc::Receiver<Frame>,
    limit: Option<u64>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
) -> Result<DryRunStreamResult, PipelineError>
```

Return:

```rust
Ok(DryRunStreamResult {
    stream_name: stream_name.to_string(),
    batches,
    total_rows,
    total_bytes,
})
```

Update the caller to pass the stream name directly and remove any later patch-up mutation.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine collect_dry_run_frames_preserves_stream_name -- --exact`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/execution.rs
git commit -m "refactor: construct valid dry run stream results"
```

### Task 4: Scaffold Warning Text And Generated README

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`
- Test: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing test**

Add tests asserting scaffold output includes a generated `README.md` and strong non-production language, for example:

```rust
#[test]
fn scaffold_source_marks_generated_plugin_as_not_production_ready() {
    // run scaffold into temp dir
    // assert README.md exists
    // assert README and stub files contain "not production-ready"
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-cli scaffold_source_marks_generated_plugin_as_not_production_ready -- --exact`

Expected: FAIL because no README is generated and the stronger warning text is absent.

**Step 3: Write minimal implementation**

Update scaffold generation to:
- create `README.md`
- prepend comments in generated stub files such as:

```rust
// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.
```

- print a stronger CLI summary warning that scaffold output is incomplete starter code only

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-cli scaffold_source_marks_generated_plugin_as_not_production_ready -- --exact`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "feat: harden scaffold output warnings"
```

### Task 5: CI Guard For Committed Scaffold Stubs

**Files:**
- Create: `scripts/check-no-scaffold-stubs.sh`
- Modify: `.github/workflows/ci.yml`
- Test: `scripts/check-no-scaffold-stubs.sh`

**Step 1: Write the failing test**

Create a simple shell-script self-test mode or a focused invocation that fails on a temp plugin file containing `UNIMPLEMENTED`.

Example script behavior target:

```sh
tmpdir="$(mktemp -d)"
mkdir -p "$tmpdir/plugins/sources/example/src"
printf '%s\n' 'const _: &str = "UNIMPLEMENTED";' > "$tmpdir/plugins/sources/example/src/main.rs"
scripts/check-no-scaffold-stubs.sh "$tmpdir"
```

Expected exit code: non-zero

**Step 2: Run test to verify it fails**

Run: `sh scripts/check-no-scaffold-stubs.sh /tmp/path-containing-unimplemented`

Expected: FAIL because the guard is not implemented yet.

**Step 3: Write minimal implementation**

Create `scripts/check-no-scaffold-stubs.sh`:

```sh
#!/usr/bin/env sh
set -eu

root="${1:-.}"

if rg -n 'UNIMPLEMENTED' "$root/plugins"; then
  echo "Committed plugin scaffold stubs detected under plugins/"
  exit 1
fi
```

Then tighten it as needed to avoid false positives, and wire it into `.github/workflows/ci.yml` as a dedicated step in the main CI job set.

**Step 4: Run test to verify it passes**

Run: `scripts/check-no-scaffold-stubs.sh .`

Expected: PASS in the current repo state

Run: `tmpdir=$(mktemp -d) && mkdir -p "$tmpdir/plugins/sources/example/src" && printf '%s\n' 'const _: &str = "UNIMPLEMENTED";' > "$tmpdir/plugins/sources/example/src/main.rs" && scripts/check-no-scaffold-stubs.sh "$tmpdir"`

Expected: FAIL with a clear message

**Step 5: Commit**

```bash
git add scripts/check-no-scaffold-stubs.sh .github/workflows/ci.yml
git commit -m "ci: block committed scaffold stubs"
```

### Task 6: Full Verification

**Files:**
- Verify only

**Step 1: Run focused tests**

Run:

```bash
cargo test -p rapidbyte-types
cargo test -p rapidbyte-cli scaffold_source_marks_generated_plugin_as_not_production_ready -- --exact
cargo test -p rapidbyte-engine collect_dry_run_frames_preserves_stream_name -- --exact
```

Expected: PASS

**Step 2: Run lint and broader tests**

Run:

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rapidbyte-benchmarks -p rapidbyte-sdk -p rapidbyte-runtime -p rapidbyte-engine -p rapidbyte-cli
```

Expected: PASS

**Step 3: Run the new CI guard locally**

Run:

```bash
scripts/check-no-scaffold-stubs.sh .
```

Expected: PASS

**Step 4: Commit verification-only changes if needed**

If no files changed, no commit.
