# Benchmark Environment Lifecycle Design

## Summary

Make benchmark environment profiles own infrastructure lifecycle instead of
only providing connection metadata. `bench run` should provision the selected
environment before any warmups or measured iterations, execute all selected
scenarios inside that environment, and always tear it down afterward on both
success and failure.

## Goals

- Treat benchmark environments as benchmark-owned infrastructure.
- Guarantee teardown after benchmark execution, including failed runs.
- Add a provider lifecycle abstraction that supports `docker_compose` now and
  can extend to Terraform-backed infrastructure later.
- Keep existing scenario-level fixture preparation inside the provisioned
  environment without changing benchmark measurement semantics.

## Non-Goals

- Adding an escape hatch to preserve failed benchmark environments.
- Reworking scenario-level Postgres fixture seeding beyond what is required to
  run inside a provisioned environment.
- Generalizing the first iteration beyond provider kinds already modeled by
  benchmark environment profiles.

## Current Problems

- `benchmarks/src/environment.rs` resolves profile YAML into service bindings
  but does not model provider lifecycle.
- `benchmarks/src/runner.rs` prepares schemas and tables inside the configured
  database, but it never provisions or tears down provider-owned resources.
- The committed `local-dev-postgres` environment profile declares
  `provider.kind: docker_compose`, but the runner currently ignores that
  information entirely.
- This leaves Docker resources running after benchmark completion and prevents
  the environment profile abstraction from scaling cleanly to future providers
  such as Terraform.

## Alternatives Considered

### 1. Recommended: Typed provider lifecycle handlers

Introduce a benchmark environment session abstraction that dispatches on
`EnvironmentProvider.kind` and owns both provision and teardown.

Pros:
- Explicit provider contract.
- Testable path resolution and command construction.
- Scales to multiple provider kinds without pushing shell semantics into YAML.

Cons:
- Requires new session and provider plumbing.

### 2. Raw setup and teardown shell commands in profiles

Add free-form command strings to environment profile YAML.

Pros:
- Fastest to wire up initially.

Cons:
- Weak validation.
- Hard to compose with provider-specific state and future extensions.
- Pushes too much behavior into unchecked shell snippets.

### 3. Only clean database objects

Limit cleanup to schemas and tables touched by benchmark fixture preparation.

Pros:
- Smallest code change.

Cons:
- Does not satisfy the benchmark-owned infrastructure requirement.
- Leaves Docker and future cloud resources behind.

## Recommended Design

### Environment Session

Add an `EnvironmentSession` abstraction that is created once per `bench run`
execution after the CLI has resolved the selected environment profile. The
session owns:

- the resolved `EnvironmentProfile`
- provider-specific lifecycle state
- resolved service bindings already needed by benchmark scenarios

The runner will use the session for the entire command, not per scenario, so a
single provisioned environment can serve warmups, measured iterations, and
multiple selected scenarios before a single teardown at the end.

### Provider Lifecycle Contract

Interpret `EnvironmentProvider.kind` as a dispatch key for typed lifecycle
handlers.

Initial provider behavior:

- `docker_compose`
  - requires `provider.project_dir`
  - resolves `project_dir` relative to the repository root
  - provisions via `docker compose up -d`
  - tears down via `docker compose down -v`

Future providers can implement the same contract without changing runner flow,
for example a Terraform-backed provider using `terraform apply` and
`terraform destroy`.

### Runner Flow

`bench run` should execute in this order:

1. Parse CLI arguments.
2. Load and validate the selected environment profile.
3. Create an `EnvironmentSession`.
4. Provision provider-owned resources.
5. Resolve benchmark environment bindings from the session.
6. Run warmups and measured iterations for all selected scenarios.
7. Attempt teardown unconditionally.
8. Return either the benchmark result or a combined error that preserves the
   benchmark failure as primary and includes teardown failure as context.

This keeps environment ownership outside scenario execution while allowing
existing Postgres fixture preparation to continue inside the provisioned
database.

### Error Handling

Teardown is mandatory and should run on both success and failure.

- If benchmark execution succeeds and teardown fails, return the teardown
  failure.
- If benchmark execution fails and teardown succeeds, return the benchmark
  failure.
- If both fail, return the benchmark failure with teardown failure attached as
  additional context so operators do not lose the original failure cause.

Provision failures should stop execution before any benchmark scenario runs.

### Testing Strategy

Add focused tests for:

- provider dispatch and repository-root-relative path resolution for
  `docker_compose`
- teardown running after a successful benchmark session
- teardown still running after benchmark execution fails
- combined failure reporting when both benchmark execution and teardown fail

Keep provider lifecycle tests isolated from real Docker usage by verifying
constructed commands and injected executor behavior.

## Change Scope

### Modified files

- `benchmarks/src/environment.rs`
- `benchmarks/src/runner.rs`
- `benchmarks/src/cli.rs`
- `benchmarks/scenarios/README.md`

### New files

- none required beyond documentation in this design slice

## Outcome

After this change, benchmark environment profiles will define owned
infrastructure lifecycles rather than passive connection settings. Local Docker
benchmark environments will come up before a run and be torn down
automatically afterward, and the same runner contract will be ready for future
Terraform-backed benchmark environments.
