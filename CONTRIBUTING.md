# Contributing to Rapidbyte

We welcome contributions of all kinds — bug fixes, new plugins, documentation improvements, and test coverage. Whether you are fixing a typo or building a full plugin, thank you for helping make Rapidbyte better.

## Getting Started

### Prerequisites

- **Rust 1.75+** with the `wasm32-wasip2` target:
  ```bash
  rustup target add wasm32-wasip2
  ```
- **[just](https://github.com/casey/just)** command runner
- **Docker** (for PostgreSQL in dev/test)

### Setup

```bash
git clone https://github.com/netf/rapidbyte.git
cd rapidbyte
just install-hooks
just dev-up
```

This starts PostgreSQL, builds everything (release mode), and seeds 1M rows. Verify your setup with:

```bash
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml -v
```

## Contribution Paths

### Bug Fixes & Features

1. Fork the repo and create a branch (`fix/short-description` or `feat/short-description`).
2. Make your changes and run `just ci` plus any extra checks listed in [Testing Expectations](#testing-expectations).
3. Open a pull request against `main`.

The local Git hooks installed by `just install-hooks` are intentionally light:
`pre-commit` auto-runs formatting before commit and stops if it changed staged
files so you can review and recommit, while `pre-push` auto-runs formatting on
the checked-out repo state, stages newly formatted files, and stops so you can
review, commit, and push again. They are convenience layers, not a substitute
for `just ci`.

### New Plugins

Plugins are independent Wasm components that compile to `wasm32-wasip2` and depend only on `rapidbyte-sdk`. They do not require any engine changes. See [docs/PLUGIN_DEV.md](docs/PLUGIN_DEV.md) for a complete walkthrough covering the SDK traits, build setup, configuration, and testing.

### Documentation & Tests

Documentation fixes and new tests are always welcome and follow the same PR process as code changes.

## Code Standards

Refer to [docs/CODING_STYLE.md](docs/CODING_STYLE.md) for the full style guide. Key rules:

- **Linting** — All host crates use `#![warn(clippy::pedantic)]`.
- **Import ordering** — std, external, workspace, crate-local, each group separated by a blank line.
- **Error handling** — Use `PluginError` factories at plugin crate boundaries; use `thiserror` for internal error types.
- **Visibility** — Default to `pub(crate)` for internal types; only make items fully `pub` when they are part of the crate's external API.
- **Tests** — Colocate unit tests in `#[cfg(test)] mod tests` at the bottom of each module.

## Testing Expectations

| Command | When to Run |
|---|---|
| `just ci` | Always before opening or updating a PR |
| `just install-hooks` | Once after clone |
| `just fix` | Any time you want the same fast formatting path the hook uses |
| `just fmt` | Always |
| `just lint` | Always |
| `just test` | Always |
| `just bench-pr` | Local perf-regression checks when connector or hot-path changes need investigation |
| `just e2e` | Engine, runtime, or plugin changes |
| `cargo bench` | Hot-path code changes |

## Benchmarks

The next-generation benchmark platform lives under `benchmarks/`.

- `just bench --suite pr --output target/benchmarks/pr/results.jsonl --env-profile local-dev-postgres` runs the PR smoke suite directly
- `just bench-pr` is the recommended local perf-regression command; it provisions the local benchmark environment, runs the PR smoke suite, and compares against the checked-in baseline artifact set
- `just bench-lab pg_dest_insert` runs the native Postgres INSERT benchmark using the committed `local-dev-postgres` environment profile
- `just bench-lab pg_dest_copy` runs the native Postgres COPY benchmark using the committed `local-dev-postgres` environment profile
- the checked-in baseline is a local/manual smoke mechanism; the GitHub benchmark workflow is `workflow_dispatch` only until dedicated benchmark runners exist

For direct runner use, provide an environment profile explicitly:

```bash
just bench --suite lab --scenario pg_dest_insert --env-profile local-dev-postgres
```

Override the committed local profile with:

- `RB_BENCH_PG_HOST`
- `RB_BENCH_PG_PORT`
- `RB_BENCH_PG_USER`
- `RB_BENCH_PG_PASSWORD`
- `RB_BENCH_PG_DATABASE`
- `RB_BENCH_PG_SOURCE_SCHEMA`
- `RB_BENCH_PG_DEST_SCHEMA`

The core runner does not auto-provision Docker/Testcontainers; `just bench-lab`
is the local orchestration wrapper for the repo-supported dev environment.

## Local Hooks

- `just install-hooks` sets `git config core.hooksPath .githooks`
- the repo-managed `pre-commit` and `pre-push` hooks run the fast auto-fix path only
- today that auto-fix path is `cargo fmt --all`
- if the hook changes files, it re-stages the originally staged paths it
  touched and aborts the commit so you can inspect the diff and re-run `git commit`
- if `pre-push` changes files, it stages newly formatted files and aborts the
  push so you can inspect the diff, create a follow-up commit, and push again
- no Python `pre-commit` package is required

## PR Process

- Fill out the PR template (auto-populated checklist).
- Keep PRs focused — one logical change per pull request.
- Include tests for new behavior or bug fixes.
- Update documentation if your change affects configuration or the protocol.
- Same-repo non-draft pull requests also run the advisory `Codex Review`
  workflow in GitHub Actions.
- Maintainers must configure the `OPENAI_API_KEY` GitHub Actions secret before
  that workflow can run successfully.
- The Codex review is supplemental feedback only and is not a required merge
  gate in the current rollout.

## Architecture Quick Reference

```
crates/
  rapidbyte-types/    # Shared protocol types (leaf crate)
  rapidbyte-state/    # State backend (SQLite, Postgres)
  rapidbyte-runtime/  # Wasmtime component runtime
  rapidbyte-sdk/      # Plugin SDK (traits, macros, host FFI)
  rapidbyte-engine/   # Pipeline orchestrator
  rapidbyte-cli/      # CLI binary
plugins/
  sources/postgres/    # wasm32-wasip2
  destinations/postgres/  # wasm32-wasip2
  transforms/sql/      # wasm32-wasip2
  transforms/validate/ # wasm32-wasip2
```

**Dependency chain:** types &larr; state &larr; runtime &larr; engine &larr; cli. Plugins depend only on `rapidbyte-sdk`.

For deeper context, see [docs/PROTOCOL.md](docs/PROTOCOL.md) and [docs/CODING_STYLE.md](docs/CODING_STYLE.md).
