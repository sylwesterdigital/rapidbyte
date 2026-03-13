# Plugin Build Resilience Design

## Summary

Harden plugin builds so `just build-all` remains reliable on macOS and Ubuntu
even when a plugin target directory contains stale build-script state from a
different Rust/compiler environment. The immediate failure to prevent is the
`thiserror`/stable-channel crash caused by stale probe output under
`plugins/*/target`.

## Goals

- Make `just _build-plugins` and `just build-all` recover automatically from
  stale plugin build-script state.
- Preserve incremental plugin builds when the effective build environment has
  not changed.
- Keep the solution shell-based and portable across macOS and Ubuntu.
- Verify that all checked-in plugins build successfully after the change.

## Non-Goals

- Moving plugins into the root Cargo workspace.
- Pinning `thiserror` or other dependencies purely to avoid this single failure.
- Running a full `cargo clean` for every plugin build.
- Replacing Cargo, `just`, or the existing repo-local rustc wrapper.

## Root Cause

The failure is not a deterministic source error in `plugins/sources/postgres`.
Fresh target directories build correctly. The break only appears when an
existing plugin target directory contains stale build-script output from a
different environment, especially crates that probe compiler capabilities such
as `thiserror`.

That stale state can leave Cargo reusing an old `OUT_DIR`/build-script result
that was produced under a different `RUSTC_BOOTSTRAP` or flag context. On the
next stable build, Cargo can compile `thiserror` with
`error_generic_member_access` enabled and fail with `E0554`.

## Alternatives Considered

### 1. Recommended: environment-aware plugin build wrapper

Build each plugin through a repo script that fingerprints the effective build
environment and selectively invalidates stale plugin target state when the
fingerprint changes.

Pros:
- Fixes the actual stale-state failure mode.
- Preserves fast incremental builds for steady-state development.
- Works for `thiserror` and other probe-driven crates.
- Portable across macOS and Ubuntu.

Cons:
- Adds a small amount of build orchestration logic.

### 2. Dependency workaround

Pin direct `thiserror` usage to an older major version or force a narrower
resolution.

Pros:
- Small diff.

Cons:
- Symptom-level only.
- Does not protect other probe-based crates.
- Likely to regress when dependencies move again.

### 3. Clean plugin targets on every build

Run `cargo clean` or delete each plugin target directory before every build.

Pros:
- Extremely reliable.

Cons:
- Too slow for normal development.
- Throws away useful incremental state even when nothing relevant changed.

## Recommended Design

Add a repo-managed plugin build script, for example
`scripts/build-plugin.sh`, and route `Justfile` plugin builds through it.

For each plugin build, the script should:

1. Resolve the plugin root and its target directory.
2. Compute a build fingerprint from the effective environment:
   - `rustc --version --verbose`
   - resolved `RUSTC` path
   - `RUSTC_WRAPPER`
   - `RUSTC_BOOTSTRAP`
   - `RUSTFLAGS`
   - `CARGO_ENCODED_RUSTFLAGS`
   - target triple
   - profile/debug vs release mode
3. Store that fingerprint in plugin-local target metadata such as
   `target/.rapidbyte-build-env`.
4. If the fingerprint changed, remove only stale plugin build-script state:
   - `target/<profile>/build`
   - `target/wasm32-wasip2/<profile>/build`
   - optionally matching `deps/lib*thiserror*` style artifacts only if needed
5. Re-run the plugin build with the requested profile.

This design keeps the cleanup narrow: invalidate Cargo build-script outputs and
their dependent artifacts when the environment changed, but keep the rest of the
target tree intact when possible.

## Change Scope

### New files

- `scripts/build-plugin.sh`
- `tests/test_plugin_build_resilience.sh`

### Modified files

- `Justfile`
- `docs/BUILD.md`

## Error Handling

- If the plugin path is invalid, the build script exits non-zero with a clear
  usage message.
- If the fingerprint file cannot be written, the script exits non-zero rather
  than silently pretending the environment was recorded.
- If cleanup is needed, the script prints a short reason before removing stale
  build-state directories.
- If Cargo build fails after cleanup, the script surfaces Cargo's exit code and
  output unchanged.

## Verification Strategy

- Baseline host verification:
  - `cargo test`
- Fresh plugin build verification:
  - `just _build-plugins`
- Warm-cache verification:
  - `just _build-plugins`
  - run twice and confirm both succeed
- Regression verification:
  - create a temporary plugin target
  - build once with `RUSTC_BOOTSTRAP=1`
  - build again without `RUSTC_BOOTSTRAP`
  - confirm the wrapper invalidates stale state and the second build succeeds
- Cross-platform portability:
  - use POSIX/Bash features already available on macOS and Ubuntu
  - avoid GNU-only `sed`, `readlink`, or `mktemp` assumptions in the script
