# Controller Flag Rename and Logging Fix Design

## Summary

Two CLI ergonomics issues need to be corrected:

1. The insecure preview signing override flag name is too verbose and awkward.
   It should be renamed from `--allow-insecure-default-signing-key` to
   `--allow-insecure-signing-key`.
2. Global verbosity flags (`-v`, `-vv`) currently do not enable `tracing`
   output for long-running commands like `rapidbyte controller` and
   `rapidbyte agent`, which makes them feel broken.

The fix should stay at the CLI layer. It should rename the flag cleanly,
update help/docs/tests, and make verbosity derive the default tracing filter
unless `RUST_LOG` is explicitly set.

## Problem

### Flag Naming

The current flag name is unnecessarily specific and user-hostile:

- `--allow-insecure-default-signing-key`

Users expect a shorter, clearer opt-out flag. The longer name adds friction in
interactive use and clutters examples and help output.

### Logging Behavior

The current logging initializer suppresses tracing entirely unless `RUST_LOG` is
set. That means `-v` and `-vv` only affect human-facing command output, not
server logs. For long-running server commands, this breaks user expectations:

```bash
rapidbyte controller -vv ...
```

should emit debug logs, but currently appears silent unless `RUST_LOG` is also
set.

## Goals

- Hard-rename the insecure signing override flag to
  `--allow-insecure-signing-key`
- Remove the old spelling instead of keeping a deprecated alias
- Make `-v` map to `info` tracing by default
- Make `-vv` map to `debug` tracing by default
- Keep `RUST_LOG` as the override when explicitly set
- Keep the fix small and contained to CLI-layer behavior

## Non-Goals

- Adding a new `--log-level` flag
- Redesigning controller or agent logging internals
- Changing controller/agent crate behavior outside what the CLI initializes

## Design

### Flag Rename

Change the clap-exposed flag name and all user-facing references to:

- `--allow-insecure-signing-key`

Update:

- CLI option definitions
- validation error messages
- help text
- tests
- README/docs/examples

The old spelling should no longer be accepted.

### Logging Behavior

Current behavior in `crates/rapidbyte-cli/src/logging.rs` is:

- if `RUST_LOG` is set: use it
- otherwise: force tracing off

New behavior should be:

- if `RUST_LOG` is set: use it exactly
- otherwise derive default tracing from CLI verbosity

Recommended default mapping:

- quiet/default: no tracing output unless the command already emits human output
- `-v`: `info`
- `-vv`: `debug`

This preserves advanced control via `RUST_LOG` while making `-v` / `-vv`
behave the way users expect.

## Validation

- `rapidbyte controller --help` should show `--allow-insecure-signing-key`
- old flag spelling should be rejected
- verbosity-to-filter unit tests should cover default, `-v`, `-vv`, and
  `RUST_LOG` override behavior
- command/config tests should assert the new error/help strings
