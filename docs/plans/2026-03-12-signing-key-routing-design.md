# Signing Key and CLI Routing Design

## Problem

Two default behaviors are too permissive:

- Controller and agent both accept the hard-coded development signing key by default, which makes preview ticket signatures guessable if operators forget to override it.
- The CLI now loads `controller.url` from `~/.rapidbyte/config.yaml` for every command, which silently changes `rapidbyte run` from local execution to distributed execution when a config file exists.

## Decision

### Signing key

Keep the shared development signing key for local development only, but require an explicit opt-in to use it.

- Controller startup rejects the default signing key unless an explicit dev escape hatch is enabled.
- Agent startup does the same.
- CLI controller/agent subcommands expose `--allow-insecure-default-signing-key` for local/dev use.

### CLI routing

Restore local-by-default semantics for `run`.

- `run` uses distributed mode only when `--controller` or `RAPIDBYTE_CONTROLLER` is set.
- `status`, `watch`, and `list-runs` continue to accept config-file fallback from `~/.rapidbyte/config.yaml`.

## Testing

- Add controller and agent config tests that reject the default signing key without the explicit override.
- Add CLI resolution tests that prove `run` ignores config fallback while control-plane commands still use it.

## Scope

This is a defaults-and-validation fix. It does not change preview ticket format, signing implementation, or explicit distributed CLI behavior.
