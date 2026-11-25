# Malachite PR 3 Follow-up Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the follow-up safety review of PR #3 and produce a new immutable Malachite runtime candidate for
Emerald.

**Architecture:** Make WAL opening validate without mutating persisted bytes, and match Circle v0.8.2's split between
restartable startup liveness failures and supervisor-visible replay/runtime safety failures. Strengthen real actor
tests around the exact flush and shutdown boundaries, then record every remaining review item and phase-3 retirement
condition.

**Tech Stack:** Rust, Tokio, ractor, Malachite WAL/engine/app-channel crates, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`

## Global Constraints

- Keep the branch based on Circle v0.7.0; do not import v0.8.x persisted representations or migrations.
- Preserve WAL framing, tags, sequence values, signed bytes, identities, genesis, and configuration compatibility.
- Never delete, reset, truncate, replace, or restore a stale WAL as corruption recovery.
- Runtime append/flush uncertainty and corrupt replay safety-hang; reset/fetch startup I/O fails visibly for restart.
- Keep generic fixes isolated for contribution to Circle and record their phase-3 retirement conditions.
- Run all Cargo commands from `code/`; keep Markdown within 120 columns.
- Before push, run `cargo clippy --tests -- -D warnings` and `cargo +nightly fmt --all --check`.

## Review Focus

- A corrupt middle-frame length that remains below `MAX_ENTRY_SIZE` must not be mistaken for a torn tail.
- A physically incomplete final frame must fail without changing any byte, including on a second open.
- Replay corruption must set the safety gauge before stopping safety-relevant children.
- Failing the flush immediately before publish or decide must prevent the corresponding effect.
- Normal shutdown must stop consensus and sync before WAL and must continue cleanup after one child-stop error.

---

### Task 1: Non-mutating WAL open

**Files:**
- Modify: `code/crates/wal/src/log.rs`
- Modify: `code/crates/wal/tests/it/corruption.rs`
- Modify: `code/crates/wal/tests/it/crashes.rs`

**Interfaces:**
- Produces: `Log::open` returns `InvalidData` or `UnexpectedEof` for malformed/incomplete frames without mutation.
- Consumes: existing V1 frame header and `MAX_ENTRY_SIZE` limit; no format changes.

- [ ] Add regressions for oversized, plausible-but-past-EOF, partial-header, and incomplete-payload tails; preserve exact
      bytes across two failed opens.
- [ ] Run the focused tests and confirm they fail because `open_with` truncates and returns success.
- [ ] Replace scan-time `break` plus `truncate_to` with explicit validation errors; retain valid entry counting.
- [ ] Run WAL corruption/crash tests in default, explicit compression, and forced-compression modes.
- [ ] Commit as `fix: reject malformed WAL tails without mutation`.

### Task 2: Visible startup and replay failure policy

**Files:**
- Modify: `code/crates/engine/src/util/failure.rs`
- Modify: `code/crates/engine/src/consensus.rs`
- Modify: `code/crates/engine/src/util/events.rs`
- Modify: `code/crates/app-channel/tests/wal_safety_hang.rs`

**Interfaces:**
- Consumes: Task 1's non-mutating startup error and existing `NodeMsg::SafetyFailure` supervisor path.
- Produces: reset/fetch failures return `ActorProcessingErr`; replay corruption casts `SafetyFailure` and remains pending.

- [ ] Add real-engine tests proving fetch/reset failure terminates through the liveness path and replay corruption sets
      the gauge, stops safety children, keeps the connector open, and never reaches normal progress.
- [ ] Run the focused tests and confirm the current local `hang_on_failure` leaves them pending without the expected
      supervisor outcome.
- [ ] Port the narrow `stop_on_failure` helper and use it for reset/fetch; use `hang_on_safety_failure` for replay.
- [ ] Emit the existing WAL reset/replay diagnostics on every failure path.
- [ ] Run engine, app-channel, and WAL compatibility tests.
- [ ] Commit as `fix: expose WAL startup and replay failures`.

### Task 3: Exact runtime flush and shutdown boundaries

**Files:**
- Modify: `code/crates/app-channel/tests/wal_safety_hang.rs`
- Modify: `code/crates/engine/src/node.rs`
- Modify: `code/crates/app-channel/src/builder.rs`
- Modify: `code/crates/engine/src/network.rs`
- Test: existing engine node/network unit tests and app-channel integration tests.

**Interfaces:**
- Consumes: Task 2's safety supervisor behavior.
- Produces: Nth-flush injection before publish/decide and ordered, best-effort shutdown with WAL last.

- [ ] Generalize the custom WAL fault to fail a selected flush and add publish-boundary and decide-boundary tests.
- [ ] Run both tests and confirm the current first-flush-only harness cannot reach the asserted boundaries.
- [ ] Track consensus/sync as shutdown-first children; stop them before WAL and continue cleanup after an error.
- [ ] Add or adapt node tests that observe shutdown order and cleanup continuation.
- [ ] Downgrade the two harmless network notification races from actor errors to contextual logs, matching v0.8.2.
- [ ] Run app-channel, engine, and network suites; mutation-check by changing each protected flush call.
- [ ] Commit as `fix: harden WAL failure and shutdown boundaries`.

### Task 4: Review documentation and CI hygiene

**Files:**
- Modify: `.github/workflows/docs.yml`
- Modify: `.github/workflows/quint.yml`
- Modify: `.github/workflows/semver.yml`
- Modify: any other workflow using the same `dorny/paths-filter` tag.
- Modify: `BREAKING_CHANGES.md` and/or `RELEASE_NOTES.md`
- Modify: `docs/architecture/adr-007-write-ahead-log.md`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: Tasks 1-3 final behavior.
- Produces: operator contract, public API notes, SHA-pinned actions, and phase-3 upstream/retirement inventory.

- [ ] Pin every `dorny/paths-filter` use to the verified v3 commit SHA.
- [ ] Document strict tail rejection, the startup/replay failure split, shutdown backstop, bad-final-frame recovery, and
      the only easy downgrade proof: a stopped 12-byte header-only WAL; otherwise require roll-forward.
- [ ] Record public API breaks and mark generic engine fixes for Circle contribution and later retirement.
- [ ] Give remaining pre-existing review items explicit linked follow-up dispositions.
- [ ] Run `actionlint`, `typos`, Markdown width checks, and `git diff --check`.
- [ ] Commit as `docs: record final v0.7 safety boundaries`.

### Task 5: Produce the immutable Malachite candidate

**Files:**
- Modify: PR #3 description after verification.

**Interfaces:**
- Produces: one exact Malachite runtime SHA consumed by the Emerald follow-up plan.

- [ ] Run focused regression suites, `cargo all-tests`, Clippy, nightly formatting, and the applicable Quint/MBT gates.
- [ ] Run a fresh whole-branch review and fix any Critical or Important findings with RED-GREEN evidence.
- [ ] Push the branch, wait for final CI, and update PR #3 with exact commands, results, limitations, and candidate SHA.

