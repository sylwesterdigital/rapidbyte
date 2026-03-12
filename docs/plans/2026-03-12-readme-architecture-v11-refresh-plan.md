# README Architecture v1.1 Refresh Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refresh the root README so it accurately reflects the implemented distributed controller/agent runtime while remaining a concise high-level entrypoint.

**Architecture:** Keep the README high-level. Update the intro, features, CLI surface, architecture summary, and development/benchmark sections to acknowledge both standalone and distributed execution, then link out to the architecture, benchmarking, protocol, and plugin docs for details.

**Tech Stack:** Markdown docs, repo-local documentation references

---

### Task 1: Refresh README positioning and CLI surface

**Files:**
- Modify: `README.md`

**Step 1: Identify stale standalone-only messaging**

Review:

```bash
rg -n "Single-binary|CLI|Architecture|benchmark|controller|agent|watch|list-runs|status" README.md
```

Expected: existing README sections that need v1.1 alignment.

**Step 2: Update the README content**

- Refresh the intro to mention standalone + distributed execution modes.
- Add feature bullets for distributed runtime / distributed previews.
- Expand the CLI command table with:
  - `status`
  - `watch`
  - `list-runs`
  - `controller`
  - `agent`
- Add a short distributed-runtime section with:
  - standalone vs distributed mode
  - controller/agent role summary
  - shared Postgres state requirement for distributed mode
  - auth/signing/TLS mentioned at a high level
- Update the architecture section to mention controller control plane + agent
  Flight preview plane.
- Mention the distributed benchmark in the development section.

**Step 3: Review the rendered flow**

Read:

```bash
sed -n '1,260p' README.md
```

Expected: README remains concise and does not turn into an operator manual.

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: refresh readme for distributed runtime"
```

### Task 2: Verify README consistency against architecture/docs links

**Files:**
- Modify: `README.md` (only if follow-up tweaks are needed)

**Step 1: Run doc consistency grep**

Run:

```bash
rg -n "controller|agent|distributed|watch|list-runs|status|auth|signing|TLS|benchmark" README.md docs/ARCHITECTUREv2.md docs/BENCHMARKING.md CONTRIBUTING.md
```

Expected: README terminology is consistent with the higher-detail docs.

**Step 2: Final proofread**

- Ensure README stays high-level.
- Ensure links point to the right docs.
- Ensure no outdated claim remains about standalone-only behavior.

**Step 3: Commit follow-up if needed**

```bash
git add README.md
git commit -m "docs: align readme with architecture v1.1"
```
