# README Architecture v1.1 Refresh Design

## Summary

The root `README.md` should remain a high-level product and developer entry
point, but it needs to reflect the distributed controller/agent runtime that is
now implemented and documented in `docs/ARCHITECTUREv2.md`.

The README should not become an operator manual. It should explain that
Rapidbyte supports both standalone and distributed execution, expose the new
high-level CLI surface, and point readers to deeper docs for architecture,
benchmarking, plugin development, and protocol details.

## Problem

The current README still reads primarily like a standalone local pipeline
runner. It describes the local engine well, but it underspecifies the new
distributed controller/agent runtime, new CLI subcommands, benchmark shape, and
security/runtime defaults introduced by the v1.1 architecture work.

This creates a documentation mismatch:

- the codebase now supports standalone and distributed modes
- the architecture doc explains that distributed model in detail
- the README does not yet present that model clearly at a high level

## Goals

- Keep the README concise and high-level.
- Update the project description so it no longer implies standalone-only usage.
- Add a compact distributed-runtime section.
- Expand the CLI section to include the new distributed commands.
- Keep standalone local quickstart intact.
- Point readers to the correct deeper docs instead of duplicating detailed spec
  content.

## Non-Goals

- Rewriting the README into a deployment or operator guide.
- Copying detailed lease, ticket, or Flight protocol semantics out of
  `docs/ARCHITECTUREv2.md`.
- Updating every documentation file in the repo; this pass is centered on the
  root README with light consistency checks.

## Design

### README Positioning

The README should describe Rapidbyte as a single executable that supports two
execution modes:

- standalone/local execution
- distributed controller/agent execution

That keeps the messaging consistent with the actual shipped UX: `rapidbyte`
still ships as one binary, but it now includes `controller` and `agent`
subcommands and distributed client commands.

### Content Changes

- Refresh the intro/value proposition to mention distributed execution.
- Add one or two feature bullets for distributed runtime and distributed
  previews.
- Keep the current local quickstart for the fastest first experience.
- Expand the CLI table with `status`, `watch`, `list-runs`, `controller`, and
  `agent`.
- Add a short "Distributed Runtime" section that explains controller/agent mode,
  high-level security/runtime requirements, and the shared-state constraint.
- Update the architecture section so it acknowledges the local engine core plus
  the distributed control/data planes.
- Mention the distributed benchmark alongside the local benchmark flow.

### Documentation Links

The README should explicitly point to:

- `docs/ARCHITECTUREv2.md`
- `docs/BENCHMARKING.md`
- `CONTRIBUTING.md`
- `docs/PLUGIN_DEV.md`
- `docs/PROTOCOL.md`

## Validation

The refreshed README should let a new reader answer these questions quickly:

- What is Rapidbyte?
- Can it run locally and in distributed mode?
- What are the main CLI entrypoints?
- Where do I go for deeper architecture or benchmark details?
