# Dev Shell UX Cleanup Design

**Date:** 2026-03-11

## Goal

Finish the remaining dev-shell cleanup by fixing the visible UX mismatches that make the shell feel unfinished:
- completion should support relevant table/stream names, not just dot-commands
- `.stream` should reject unknown flags instead of silently ignoring them
- the `.source` coercion comment should match the real implementation

## Scope

In scope:
- `rapidbyte-dev` command parsing
- `rapidbyte-dev` completer state model
- REPL integration required to feed completion context
- comment cleanup in `.source` config parsing

Out of scope:
- SQL completion
- plugin-name completion for `.source`
- broader REPL redesign

## Current Problems

### 1. Completer comment and behavior drift

[`crates/rapidbyte-dev/src/completer.rs`](/home/netf/rapidbyte/crates/rapidbyte-dev/src/completer.rs) says it completes “dot-commands and table names”, but it only stores and returns dot-commands. That mismatch is directly user-visible.

### 2. `.stream` parser is too permissive

[`crates/rapidbyte-dev/src/commands/mod.rs`](/home/netf/rapidbyte/crates/rapidbyte-dev/src/commands/mod.rs) only understands `.stream <table> [--limit N]`, but any extra tokens or unknown flags are effectively ignored unless they happen to trip a `--limit` parse path. This makes malformed commands look accepted.

### 3. `.source` comment is stale

[`crates/rapidbyte-dev/src/repl.rs`](/home/netf/rapidbyte/crates/rapidbyte-dev/src/repl.rs) says the config coercion tries `i64`, then `f64`, then string, but the actual code does `i64`, then `bool`, then string.

## Design

### Stateful, context-aware completion

Make `DevCompleter` hold lightweight completion state:
- static dot-command list
- source stream names from the connected catalog
- workspace table names from the in-memory workspace

The completer should stay simple and prefix-based. It only needs enough context to improve the shell commands that already exist:
- first token beginning with `.`: complete dot-commands
- second token for `.schema`, `.stream`, `.clear`: complete known table/stream names

Name sources:
- `.schema` and `.stream`: prefer connected source catalog streams
- `.clear`: prefer workspace table names

This keeps completion behavior aligned with what the shell can actually operate on.

### REPL integration

The REPL currently constructs the completer once at startup. To support dynamic completion, the REPL should refresh the completer context each loop iteration before reading input, based on current `ReplState`.

The refresh path should be cheap:
- collect source stream names from `state.source.as_ref().map(|s| &s.catalog.streams)`
- collect workspace table names from a small accessor on `ArrowWorkspace`

### Strict `.stream` parsing

Tighten `.stream` parsing so only these shapes are accepted:
- `.stream <table>`
- `.stream <table> --limit N`

Reject:
- unknown flags
- extra positional arguments
- `--limit` without a value
- repeated unexpected trailing tokens

This is a correctness fix more than a UX nicety, because it stops malformed commands from being silently accepted.

### Comment correction

Update the `.source` config coercion comment to match actual behavior:
- parse `i64`
- else parse `bool`
- else use string

There is no need to add `f64` parsing in this tranche because the request is to make the shell practical, correct, and robust, not to widen implicit coercion rules without a clear requirement.

## Testing Strategy

- parser unit tests for `.stream` unknown-flag rejection and malformed input
- completer unit tests for:
  - dot-command completion
  - `.stream` completion from source stream names
  - `.clear` completion from workspace tables
  - no irrelevant suggestions in unsupported positions
- targeted `rapidbyte-dev` tests for the changed modules

## Risks And Mitigations

### Completion state drift

If the completer owns stale data, suggestions become misleading. Refreshing completion context directly from `ReplState` before each `read_line` avoids this.

### Over-completion

Adding suggestions in too many contexts can become noisy. Restricting table-name completion to the second token of `.schema`, `.stream`, and `.clear` keeps the behavior predictable.

### Parser strictness regression

Making `.stream` stricter could break currently tolerated malformed inputs. That is the intended fix; the tests should lock in the supported grammar explicitly.

## Acceptance Criteria

- the dev-shell completer really supports relevant table/stream names
- `.stream` rejects unknown flags and malformed extra tokens
- the `.source` coercion comment matches the implementation
- targeted `rapidbyte-dev` tests pass
