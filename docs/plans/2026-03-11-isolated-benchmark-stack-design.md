# Isolated Benchmark Stack Design

## Summary

Move benchmark-owned Docker environments off the shared repo-root development
Compose stack and onto a dedicated benchmark-local Compose project. Keep
benchmark teardown destructive for owned environments, but only after the
runner is operating against an isolated benchmark stack that it truly owns.

## Goals

- Preserve benchmark-owned lifecycle semantics, including automatic destructive
  teardown after runs.
- Eliminate the risk that benchmark teardown stops or deletes data from the
  shared repo-root development stack.
- Make the benchmark environment contract explicit in code, profiles, wrapper
  commands, and documentation.
- Create a clean base for future benchmark-owned providers and multi-service
  benchmark environments.

## Non-Goals

- Making the shared repo-root `docker-compose.yml` benchmark-owned.
- Adding conditional ownership detection on top of the shared dev stack.
- Reworking benchmark fixture seeding or measurement semantics.
- Adding Terraform or cloud-backed benchmark providers in this slice.

## Current Problems

- `benchmarks/environments/local-dev-postgres.yaml` points at `provider.project_dir: .`,
  which resolves to the repo root and therefore the shared `docker-compose.yml`.
- The current benchmark lifecycle implementation provisions with
  `docker compose up -d` and tears down with `docker compose down -v`.
- `down -v` is correct for benchmark-owned infrastructure but unsafe against
  the shared repo-root development stack because it can remove pre-existing
  local volumes and data.
- Repo docs still describe `just bench-lab` as reusing the shared repo Docker
  Compose project, which conflicts with the benchmark-owned teardown policy.

## Alternatives Considered

### 1. Recommended: dedicated benchmark Compose stack under `benchmarks/`

Add a benchmark-specific Compose file and profile, for example:

- `benchmarks/docker-compose.yml`
- `benchmarks/environments/local-bench-postgres.yaml`

Use a stable benchmark-specific Compose project name so container names and
volumes are clearly namespaced away from the dev stack.

Pros:
- Explicit ownership boundary.
- Clean separation from dev workflow.
- Easy to extend with benchmark-only services later.
- Keeps destructive teardown valid because the runner truly owns the stack.

Cons:
- One more Compose definition to maintain.

### 2. Reuse repo-root Compose with a unique project name

Keep using the repo-root service definition but override the Compose project
name for benchmark runs.

Pros:
- Less short-term duplication.

Cons:
- Shared service definition still couples benchmarks to dev workflow.
- Ownership remains conceptually muddy.
- Harder to document and evolve cleanly.

### 3. Keep shared dev profile and make teardown non-destructive

Retain `local-dev-postgres` for benchmark runs but replace `down -v` with a
less destructive cleanup path.

Pros:
- Smallest code change.

Cons:
- Wrong long-term model.
- Benchmark-owned semantics become conditional and ambiguous.
- Leaves ongoing tech debt around ownership boundaries.

## Recommended Design

### Environment Taxonomy

Split local PostgreSQL benchmark environments into two distinct concepts:

- `local-dev-postgres`
  - shared/manual environment
  - may remain for manual execution against an existing stack
  - must not use benchmark-owned destructive teardown

- `local-bench-postgres`
  - benchmark-owned environment
  - provisioned and destroyed automatically by the benchmark runner
  - backed by a dedicated Compose file under `benchmarks/`

This removes the current ambiguity where one profile is expected to behave as
both shared development state and benchmark-owned infrastructure.

### Dedicated Benchmark Compose Stack

Create a benchmark-local Compose definition under `benchmarks/`, separate from
the repo-root `docker-compose.yml`.

Recommended properties:

- benchmark-specific Postgres service only
- benchmark-specific port mapping
- benchmark-specific volume namespace
- healthcheck compatible with `docker compose up -d --wait`

The benchmark-local profile should point at this Compose directory rather than
the repo root.

### Provider Contract

Benchmark-owned Docker profiles should include provider metadata sufficient to
identify the owned stack precisely:

- `kind: docker_compose`
- `project_dir: benchmarks`
- a stable benchmark-specific Compose project name

Provisioning behavior:

- run `docker compose` in the benchmark-local project directory
- pass the benchmark-specific project name
- use `up -d --wait`

Teardown behavior:

- run `docker compose down -v` with the same project name

Because the stack is isolated and benchmark-owned, destructive teardown remains
appropriate and requires no ownership heuristics.

### Wrapper Commands and Docs

Update local benchmark entrypoints to default to the benchmark-owned profile:

- `just bench-lab`
- `just bench-pr`

Documentation should describe:

- `local-bench-postgres` as the benchmark-owned, auto-managed environment
- `local-dev-postgres` as a shared/manual profile if retained
- the fact that benchmark-owned Docker lifecycle no longer reuses the repo-root
  development Compose project

`scripts/bench-env-up.sh` should also align with the new benchmark-local stack
or be retired if the benchmark runner fully owns provisioning for the default
benchmark workflow.

## Testing Strategy

Add focused coverage for:

- parsing the new benchmark-owned environment profile
- provider command construction including the benchmark-specific Compose project
  name
- destructive teardown remaining limited to the isolated benchmark profile
- wrapper commands and docs defaulting to the owned profile instead of the
  shared dev profile

Keep unit tests at the command-construction level where possible. Real Docker
smoke checks can remain supplemental verification.

## Change Scope

### New files

- `benchmarks/docker-compose.yml`
- `benchmarks/environments/local-bench-postgres.yaml`

### Modified files

- `benchmarks/environments/local-dev-postgres.yaml`
- `benchmarks/src/environment.rs`
- `benchmarks/scenarios/README.md`
- `docs/BENCHMARKING.md`
- `Justfile`
- `scripts/bench-env-up.sh`

## Outcome

After this change, benchmark-owned teardown remains strict and destructive, but
only against infrastructure the benchmark runner truly owns. The shared dev
stack and the benchmark stack become separate concepts, which removes the data
loss risk and gives the benchmark environment model a clean long-term shape.
