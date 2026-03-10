# Benchmarking

Rapidbyte now uses the connector-agnostic benchmark platform under `benchmarks/`.

## Commands

Run the benchmark runner:

```bash
just bench --suite pr --output target/benchmarks/pr/results.jsonl
```

Run the PR smoke suite and compare it against the checked-in baseline artifact
set:

```bash
just bench-pr
```

Run the explicit native PostgreSQL destination benchmarks:

```bash
just bench-lab pg_dest_insert
just bench-lab pg_dest_copy
```

Print a readable summary for a single artifact set:

```bash
just bench-summary target/benchmarks/lab/pg_dest_copy.jsonl
```

Compare the two generated artifact sets directly:

```bash
just bench-compare target/benchmarks/lab/pg-copy.jsonl target/benchmarks/lab/pg-insert.jsonl --min-samples 1
```

Compare two artifact sets directly:

```bash
just bench-compare benchmarks/baselines/main/pr.jsonl target/benchmarks/pr/candidate.jsonl --min-samples 1
```

## Layout

- `benchmarks/scenarios/` contains declarative benchmark scenarios
- `benchmarks/environments/` contains benchmark environment profiles
- `benchmarks/analysis/` contains comparison and reporting logic
- `benchmarks/baselines/` contains checked-in smoke baselines

## Notes

- The checked-in baseline is a local and CI smoke mechanism.
- The long-term comparison model is rolling artifacts from `main`.
- Native lab scenarios currently include `pg_dest_insert` and `pg_dest_copy`.
- Use `just bench-summary <artifact>` when you only have one JSONL artifact set
  and want to inspect latency and throughput without a saved baseline.
- Native lab scenarios should be run with `--env-profile <id>` or the `just bench-lab` wrapper.
- The repo-supported local profile is `local-dev-postgres`.
- `just bench-lab` reuses the shared repo Docker Compose project, so it works
  from linked git worktrees without creating a second Postgres container.
- `just bench-lab` also normalizes the local `postgres` role password to the
  repo default before running lab benchmarks, so stale local container state
  does not break auth after config changes.
- Override local profile settings with:
  `RB_BENCH_PG_HOST`, `RB_BENCH_PG_PORT`, `RB_BENCH_PG_USER`,
  `RB_BENCH_PG_PASSWORD`, `RB_BENCH_PG_DATABASE`,
  `RB_BENCH_PG_SOURCE_SCHEMA`, and `RB_BENCH_PG_DEST_SCHEMA`.
- The core benchmark runner executes against an existing environment; it does not
  auto-provision Docker/Testcontainers itself.
- The retired benchmark harness should not be reintroduced.
