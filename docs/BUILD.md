# Build Configuration

Rapidbyte uses a repo-local rustc wrapper, plugin-local Cargo configuration,
and target-specific flags to keep builds fast and reliable.

## Defaults

- `sccache` is used automatically when available.
- If `sccache` fails (for example permission/server errors), the wrapper falls back to direct `rustc` so builds still complete.
- Native CPU tuning is enabled for non-wasm targets.
- `wasm32-wasip2` builds explicitly avoid `target-cpu=native`.
- `just build-all` and `just _build-plugins` route plugin builds through
  `scripts/build-plugin.sh`, which invalidates stale plugin build-script state
  when the effective Rust build environment changes.

## Common Commands

```bash
# Normal build/check (uses sccache when healthy)
cargo build
cargo check --all-targets

# Force bypass sccache
NO_SCCACHE=1 cargo build
NO_SCCACHE=1 cargo check --all-targets

# Just shortcuts
just build-no-sccache
just check-no-sccache
just sccache-stats
just _build-plugins
```

## Notes

- Cache directory is set repo-locally via Cargo config (`target/sccache`).
- The wrapper prints the fallback warning once, then auto-disables sccache for the session to avoid repeated failure overhead.
- Set `FORCE_SCCACHE=1` to retry sccache after fixing permissions.
- The plugin build wrapper fingerprints the effective Rust build environment
  (`rustc`, wrapper, bootstrap, and rustflags) in each plugin target
  directory.
- If that fingerprint changes, the wrapper clears only the affected plugin
  profile's `build/` and `.fingerprint/` state before rebuilding. This heals
  stale `thiserror`/build-script probe output without forcing a full plugin
  clean on every run.
