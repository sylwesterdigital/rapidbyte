# Benchmark Scenarios

This directory holds declarative benchmark scenarios for the new connector-agnostic
benchmark runner.

- `pr/` will contain the small, stable regression-gating suite.
- `lab/` will contain broader exploratory and release-oriented scenarios.

Scenario files are YAML manifests consumed by `rapidbyte-benchmarks`.

Current notable lab scenarios:

- `lab/pg_dest_insert.yaml` benchmarks PostgreSQL destination writes with `load_method: insert`.
- `lab/pg_dest_copy.yaml` benchmarks the same workload with `load_method: copy`.
- `../environments/local-dev-postgres.yaml` defines the repo-supported local benchmark environment.

Run a specific scenario with:

```bash
just bench --suite lab --scenario pg_dest_insert --env-profile local-dev-postgres --output target/benchmarks/lab/insert.jsonl
just bench --suite lab --scenario pg_dest_copy --env-profile local-dev-postgres --output target/benchmarks/lab/copy.jsonl
```
