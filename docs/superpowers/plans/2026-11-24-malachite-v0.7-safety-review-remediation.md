# Malachite v0.7 Safety Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the safety-critical review findings on Malachite PR #3, pin the corrected candidate in Emerald PR
#68, and preserve Phase 2's no-data-migration boundary.

**Architecture:** Make WAL corruption non-mutating, introduce an in-memory node safety-hang state, and route runtime
WAL durability uncertainty into that state before any later consensus effect can run. Preserve the existing
transactional builder contract: default actors started by the builder are rolled back on build failure, while
caller-owned custom actors are linked for supervision only after a successful build. Restore the removed value-sync
fault tests against the reconciled v0.7 behavior, then pin and qualify the resulting Malachite revision in Emerald.

**Tech Stack:** Rust 1.88 for Malachite, Rust 1.91.1 for Emerald, Cargo workspaces, Tokio, ractor supervision,
prometheus-client, LZ4 WAL framing, cargo-nextest, Emerald model-based tests, GitHub pull requests.

**Spec:** `docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`

**Existing plans:** `docs/superpowers/plans/2026-11-12-integrate-v0.7.0-malachite.md` and
`docs/superpowers/plans/2026-11-12-integrate-v0.7.0-emerald.md`

## Global Constraints

- Work from `1Money-Co/malachite@1d9010a7da7ced32568f7dea2c3dcbcbf112574a` on
  `seb/integrate-v0.7.0` and `1Money-Co/om-emerald@1d78818ac6b3c62a38e2ecdbc799e665bc07bde6` on
  `seb/integrate-malachite-v0.7.0`.
- Do not import a v0.8.x persisted type, codec, replay index, certificate format, or database change.
- Do not rewrite, delete, truncate, replace, or restore a stale WAL as part of recovery.
- Preserve WAL tags, framing, sequence values, validator identities, signed bytes, genesis, redb contents, and
  persisted configuration. This remediation requires no data migration.
- Runtime WAL append, flush, or unknown transport outcomes are safety failures. Startup/replay corruption remains a
  visible startup failure, and ordinary child failure remains a restartable liveness failure.
- A safety failure must not produce an Emerald process error or allow `Restart=on-failure` to restart the validator.
- Build rollback owns only actors created by the builder. A caller-supplied `WalBuilder::Custom` actor must survive a
  failed build, but must be supervised after a successful build.
- Use tests through real actor wiring for safety contracts. Helper-only tests may supplement but cannot prove them.
- Preserve raw fixture provenance and never regenerate a fixture to make a test pass.
- Never format `Cargo.lock`; review dependency-source changes separately from formatting changes.
- Keep Markdown at or below 120 columns.
- Use test-driven development: write each regression, observe the intended failure, implement the smallest fix, and
  rerun the focused test before broadening validation.
- Before pushing either repository, run `cargo clippy --tests -- -D warnings` and
  `cargo +nightly fmt --all --check`; Emerald also runs `taplo fmt --check`.
- Do not describe remote CI as green until GitHub reports it green at the pushed revisions.

## Review Focus

- A runtime append or flush failure cannot publish or decide another consensus effect and cannot exit the engine.
- Child termination caused by entering safety hang cannot terminate the node supervisor or clear the hang metric.
- Custom actors survive failed build rollback but become supervised children after a successful build.
- A corrupt middle WAL frame leaves every file byte unchanged after the first failed replay and after restart.
- Decode-failure and eager-mode sync recovery still retry without penalizing the serving peer for a local failure.

---

## Task 1: Lock the Approved Contract and Baseline

**Files:**
- Modify: `docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`
- Create: `docs/superpowers/plans/2026-11-24-malachite-v0.7-safety-review-remediation.md`
- Inspect: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: the approved runtime-only safety-hang decision and review findings 1–5.
- Produces: one implementation checklist and explicit no-migration, ownership, recovery, and restart invariants.

- [ ] **Step 1: Confirm branch and revision boundaries**

Run:

```bash
git branch --show-current
git rev-parse HEAD
git status --short
git -C ../om-emerald branch --show-current
git -C ../om-emerald rev-parse HEAD
git -C ../om-emerald status --short
```

Expected: the branches and starting revisions match Global Constraints; only approved documentation is dirty.

- [ ] **Step 2: Record the review remediation in the inventory**

Add or update rows for non-mutating WAL replay, runtime safety hang, real actor-wiring tests, restored sync fault
coverage, downgrade behavior, and the Emerald restart boundary. Link each row to its task in this plan.

- [ ] **Step 3: Check documentation shape**

Run:

```bash
awk 'length($0) > 120 { print FNR ":" length($0); bad=1 } END { exit bad }' \
  docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md \
  docs/superpowers/plans/2026-11-24-malachite-v0.7-safety-review-remediation.md
git diff --check
```

Expected: both commands emit no violations.

Do not make a documentation-only commit yet. Commit the approved contract with Task 2 so the branch does not gain a
review-only checkpoint unrelated to a behavioral fix.

## Task 2: Preserve a Corrupt WAL Byte-for-Byte

**Files:**
- Modify: `code/crates/test/tests/unit/wal_compatibility.rs`
- Modify: `code/crates/engine/src/wal/thread.rs`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: a WAL containing valid frames before and after a deliberately corrupted middle frame.
- Produces: replay errors without replay-time truncation or mutation; physical incomplete-tail handling in
  `malachitebft_wal::Log::open` remains unchanged.

- [ ] **Step 1: Write the actor-level corruption regression**

Add `corrupt_actor_replay_preserves_wal_across_restart`. Build a WAL with at least three frames, corrupt a byte in
the middle frame without shortening the file, capture the exact file bytes, and start the real WAL actor through
`Wal::spawn`. Send `WalMsg::StartedHeight` and assert replay reports the corrupt entry rather than returning a safe
prefix as success. Stop the actor, compare the file bytes with the captured bytes, spawn it again, repeat replay,
and compare the bytes again.

Keep the existing fixture helper tests, but rename any test whose name overstates production-path coverage.

- [ ] **Step 2: Observe the regression fail for the intended reason**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-test --test unit \
  -E 'test(corrupt_actor_replay_preserves_wal_across_restart)' --retries 0
```

Expected: RED because the first actor start truncates the log at the corrupt frame and changes the file bytes.

- [ ] **Step 3: Remove replay-time destructive repair**

In `fetch_entries`, retain the read error in the returned replay vector and stop iteration, but remove
`log.truncate(idx)`. Do not classify CRC, decompression, or frame-read corruption as an incomplete physical tail.
Do not change `Log::open` handling of a physically incomplete trailing frame.

- [ ] **Step 4: Prove raw and forced-compression behavior**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-test --test unit \
  -E 'test(corrupt_actor_replay_preserves_wal_across_restart) or test(wal_compatibility)' --retries 0
cargo nextest run -p arc-malachitebft-test --features wal-force-compression --test unit \
  -E 'test(corrupt_actor_replay_preserves_wal_across_restart) or test(wal_compatibility)' --retries 0
```

Expected: both modes pass and exact bytes remain stable across both actor starts.

- [ ] **Step 5: Commit the corruption fix and approved contract**

Run:

```bash
git diff --check
git add code/crates/engine/src/wal/thread.rs code/crates/test/tests/unit/wal_compatibility.rs \
  DOWNSTREAM_CHANGES.md docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md \
  docs/superpowers/plans/2026-11-24-malachite-v0.7-safety-review-remediation.md
git commit -m "fix: preserve corrupted WAL evidence"
```

Expected: the commit contains the regression, minimal non-mutating replay fix, inventory, spec, and plan.

## Task 3: Add the In-Memory Safety Supervisor Without Changing Builder Ownership

**Files:**
- Modify: `code/crates/metrics/src/metrics.rs`
- Modify: `code/crates/engine/src/node.rs`
- Create: `code/crates/engine/src/util/failure.rs`
- Modify: `code/crates/engine/src/util/mod.rs`
- Modify: `code/crates/app/src/spawn.rs`
- Modify: `code/crates/app-channel/src/builder.rs`
- Modify: the test module in `code/crates/app-channel/src/builder.rs`

**Interfaces:**
- Produces: `NodeMsg::SafetyFailure`, `NodeState { safety_hang, shutting_down }`, a safety-failure gauge, and
  `hang_on_safety_failure(node, future, formatter)`.
- Preserves: liveness failures terminate the supervisor; build rollback stops only builder-owned actors.

- [ ] **Step 1: Replace helper-only node tests with real supervision tests**

Use a small real child actor and a spawned `Node` to add these tests:

- `safety_failure_stops_children_but_keeps_node_alive`;
- `repeated_safety_failure_is_idempotent`;
- `ordinary_child_failure_still_terminates_node`; and
- `safety_failure_sets_the_node_gauge`.

The first test must cast a safety failure, observe child termination, assert the node join handle remains pending,
then explicitly stop the node so the test does not leak a task. The liveness test must fail a child without first
entering safety hang and observe the node handle complete.

- [ ] **Step 2: Observe the node tests fail to compile or fail behaviorally**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-engine node::tests --retries 0
```

Expected: RED because the current node accepts `()` messages, links children during `pre_start`, and has no safety
state or gauge.

- [ ] **Step 3: Implement the minimal supervisor state machine**

Introduce the message and state described above. On the first safety failure, set the gauge, set `safety_hang`, and
stop the safety-relevant runtime children while leaving the application connector alive. This keeps the embedding
application's work channel open so the safety hang cannot become a process failure. Ignore expected
termination/failure events while safety-hung. Outside safety hang, keep the current liveness-failure disposition.
Keep the node and connector alive until an explicit stop.

Add a failure utility that reports `NodeMsg::SafetyFailure` and then awaits `pending()` for uncertain runtime WAL
operations. Do not add persisted state or a recovery command.

- [ ] **Step 4: Move node creation before dependent actors without stealing custom ownership**

Change both app spawn paths so consensus can receive a `NodeRef`. In `EngineBuilder::build`, spawn the node first,
then spawn dependencies. Keep the existing rollback ledger for defaults and add the new node to rollback. Do not
link a caller-owned custom WAL, network, or sync actor during partial construction. After every component has
started successfully, commit all child roles and links in one supervisor message, then verify every child is still
active before returning success. Reject an already-dead custom WAL before spawning dependent actors. This closes the
registration/link race while retaining caller ownership on earlier build failure.

If default actors need early supervision during construction, only link actors that the rollback ledger owns and
ensure rollback remains bounded and ordered. Do not use a `Node::pre_start` implementation that unconditionally
links caller-owned references.

- [ ] **Step 5: Add the custom-ownership regression**

Extend the existing build-failure test with a live custom WAL actor. Inject a later build failure and assert default
actors and the node stop, while the caller-owned WAL remains alive. Then build successfully with a custom WAL in a
separate case and assert it is linked and stopped when the runtime enters safety hang. Explicitly stop the custom
actor during test cleanup.

- [ ] **Step 6: Verify focused lifecycle behavior**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-engine node::tests --retries 0
cargo nextest run -p arc-malachitebft-app-channel --lib \
  -E 'test(build_failure) or test(safety_failure)' --retries 0
```

Expected: safety and liveness paths are distinct, rollback ownership is unchanged, and cleanup leaves no listeners.

## Task 4: Route Real WAL Durability Failures Into Safety Hang

**Files:**
- Modify: `code/crates/engine/src/consensus.rs`
- Modify: `code/crates/engine/src/wal.rs` if worker termination needs a safety notification
- Modify: `code/crates/app/src/spawn.rs`
- Modify: `code/crates/app-channel/src/builder.rs`
- Create: `code/crates/app-channel/tests/wal_safety_hang.rs`
- Modify: `code/crates/app-channel/Cargo.toml` only if the integration test needs an existing test dependency
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: append result, flush result, or actor-call failure whose durability outcome is unknown.
- Produces: a node safety notification followed by a permanently pending consensus operation.
- Forbids: `Event::WalWriteError` causing engine completion, later Publish, or later Decide.

- [ ] **Step 1: Write a real custom-WAL integration test**

Implement a test WAL actor used through `WalBuilder::Custom`. It records calls and fails a selected Nth append or
flush. Drive the ordinary application/consensus handshake until the injected operation occurs. Record published and
decided events, child liveness, the node safety gauge, and the engine join handle.

Add append and flush cases. After injection, assert:

- the failing operation was reached through actor wiring;
- no later publish or decide is observed;
- runtime children terminate;
- the safety gauge is one;
- the engine handle remains pending; and
- explicit test cleanup can stop the supervisor and join the runtime.

- [ ] **Step 2: Observe the integration test fail under current fatal propagation**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-app-channel --test wal_safety_hang --retries 0
```

Expected: RED because current code emits `WalWriteError`, fails consensus, and allows the node/engine handle to
complete rather than remain safety-hung.

- [ ] **Step 3: Apply safety hang at every live WAL write path**

Pass `NodeRef` into consensus. Wrap append, flush, and call/transport failures with `hang_on_safety_failure` so the
node is notified before the operation becomes pending. Remove `publish_fatal_wal_error` and the helper-only fatal
tests it made possible. Ensure the normal effect path and buffered-message path both call the same real WAL methods;
do not leave a bypass that can continue processing after an uncertain result.

If the WAL worker can disappear without replying, route that unknown outcome through the same safety path. Preserve
startup `StartedHeight`, reset, replay, and decode failures as visible startup/recovery failures rather than runtime
safety hangs.

- [ ] **Step 4: Run focused engine and actor tests**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-app-channel --test wal_safety_hang --retries 0
cargo nextest run -p arc-malachitebft-engine \
  -E 'test(consensus::tests) or test(node::tests)' --retries 0
cargo nextest run -p arc-malachitebft-test --test unit \
  -E 'test(finalization_evidence) or test(wal_height) or test(wal_compatibility)' --retries 0
```

Expected: real append and flush faults hang safely; replay and finalization ordering remain green.

- [ ] **Step 5: Commit the runtime safety behavior**

Run:

```bash
git diff --check
git add code/crates/metrics/src/metrics.rs code/crates/engine/src code/crates/app/src/spawn.rs \
  code/crates/app-channel DOWNSTREAM_CHANGES.md
git commit -m "fix: hang safely on WAL durability loss"
```

Expected: one reviewable commit contains the supervisor, ownership-preserving builder wiring, real fault tests, and
consensus routing. No persisted format changes are present.

## Task 5: Restore Removed Sync Fault and Mode Coverage

**Files:**
- Modify: `code/crates/test/tests/it/value_sync.rs`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Restores: `status_update_on_decision`, `skipped_early_commit_does_not_break_sync`, and
  `sync_recovers_from_decode_failure`, including eager and interval status-update variants.
- Preserves: local `ValueProcessingError` retries without peer penalty.

- [ ] **Step 1: Port the missing scenarios as failing or coverage-restoring tests**

Use Circle v0.7.0's removed tests as behavioral scaffolding, but adapt assertions to the fork's intended local-fault
classification. Parameterize eager and interval modes where the harness supports it. Assert decode failure causes a
later request and successful catch-up, while peer score/penalty state is unchanged.

- [ ] **Step 2: Run each restored scenario independently**

Run from `code/`:

```bash
cargo nextest run -p arc-malachitebft-test --test it \
  -E 'test(status_update_on_decision) or test(skipped_early_commit_does_not_break_sync) or \
  test(sync_recovers_from_decode_failure)' --retries 0
```

Expected: tests either expose a wiring regression or pass against the intended reconciled behavior. A passing
restored test is still valuable because the finding is a coverage gap, not a proven implementation defect.

- [ ] **Step 3: Make only behavior required by the restored tests change**

If a test fails, trace it through the current sync request and response paths. Preserve exact ranges, wrong-peer
ownership, retry timers, decide pruning, and no-penalty classification. Do not restore the older sync architecture
or introduce v0.8 sync outcome types.

- [ ] **Step 4: Run the full value-sync integration target and commit**

Run from `code/` outside a listener-restricted sandbox if necessary:

```bash
cargo nextest run -p arc-malachitebft-test --test it -E 'test(value_sync)' --retries 0
```

Then:

```bash
git diff --check
git add code/crates/test/tests/it/value_sync.rs DOWNSTREAM_CHANGES.md
git commit -m "test: restore value sync fault coverage"
```

Expected: all current and restored value-sync tests pass; the commit is test-only unless a demonstrated regression
required a minimal sync fix.

## Task 6: Correct Downgrade and Recovery Documentation

**Files:**
- Modify: `docs/architecture/adr-007-*` using the existing ADR filename
- Modify: `docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`
- Modify: `DOWNSTREAM_CHANGES.md`
- Modify: `README.md` or the existing operator runbook only if it currently states the unsafe recovery rule

**Interfaces:**
- Documents: Phase 1 drops an unknown `0x08` record, reports replay failure, and continues with an empty replay
  result; it does not leave the WAL untouched and halt visibly.
- Establishes: downgrade is allowed only after proving the current WAL contains no `0x08`; advancing to a later
  height alone is insufficient because the later height can write another `0x08`.

- [ ] **Step 1: Replace the incorrect downgrade claims**

Correct every claim that older binaries reject `0x08` and safely halt. Describe the actual old filter/decode/error
path and its safety consequence without promising a preflight guard that does not exist.

- [ ] **Step 2: Add the runtime safety-hang runbook rule**

Document operator-visible symptoms, required WAL/storage diagnosis, and explicit restart. State that deleting,
truncating, replacing, or restoring a stale WAL is forbidden. Distinguish runtime durability uncertainty from
startup corruption and ordinary liveness failure.

- [ ] **Step 3: Validate and commit**

Run:

```bash
awk 'length($0) > 120 { print FILENAME ":" FNR ":" length($0); bad=1 } END { exit bad }' \
  docs/architecture/adr-007-* docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md \
  DOWNSTREAM_CHANGES.md
git diff --check
git add docs/architecture docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md \
  DOWNSTREAM_CHANGES.md README.md
git commit -m "docs: correct Phase 2 recovery boundaries"
```

Expected: documentation matches code and makes no claim that Phase 1 fails closed on `0x08`.

## Task 7: Pin and Prove the Safety-Hang Candidate in Emerald

**Files:**
- Modify: `app/src/node.rs`
- Modify: the focused runtime lifecycle test near `app/src/node.rs`
- Modify: the 12 Malachite dependency declarations already pinned by PR #68
- Modify: `Cargo.lock` only for the 20 expected Malachite source revisions
- Modify: the existing Phase 2 qualification and upgrade documentation
- Modify: the existing approved changelog fragment only after renewed human approval if wording changes

**Interfaces:**
- Consumes: final reviewed Malachite candidate SHA from Tasks 2–6.
- Produces: Emerald remains alive during a Malachite runtime WAL safety hang and still exits on ordinary engine
  completion or startup failure.

- [ ] **Step 1: Replace the process-error regression before changing Emerald behavior**

Replace `wal_write_event_is_a_process_error` with a test proving a defensive `WalWriteError` event does not complete
the runtime with an error. Keep or add a companion assertion that ordinary engine completion still returns a
process error. The normal Malachite safety path should no longer emit the event, but Emerald must not turn an
accidental event into an unsafe service-manager restart.

- [ ] **Step 2: Observe the Emerald test fail under the current event policy**

Run the narrow test from the Emerald repository:

```bash
cargo nextest run -p emerald -E 'test(wal_write)' --retries 0
```

Expected: RED because the current runtime maps `Event::WalWriteError` to a process error.

- [ ] **Step 3: Make the event non-fatal and pin the final Malachite SHA**

Log the defensive event at error level without returning from the Emerald runtime. Update every direct Malachite
dependency to the same final SHA, then update the lockfile mechanically. Review that lockfile changes are limited to
the expected Malachite Git source revisions; do not format the lockfile.

- [ ] **Step 4: Run focused Emerald lifecycle and dependency checks**

Run:

```bash
rg -n "malachite.*rev|source = .*malachite" --glob 'Cargo.toml' --glob 'Cargo.lock'
cargo nextest run -p emerald \
  -E 'test(wal_write) or test(runtime_event_reports_unexpected_engine_completion)' --retries 0
cargo nextest run -p emerald --test malachite_v07_recovery --retries 0
git diff -- Cargo.lock
git diff --check
```

Expected: one candidate SHA is used everywhere; defensive WAL events are non-fatal; ordinary failure semantics stay
intact; only expected Git source lines change in `Cargo.lock`.

- [ ] **Step 5: Obtain changelog approval if the fragment changes**

Show the exact proposed fragment to the user before editing or committing it. If no fragment change is needed,
record that decision in the PR evidence and leave the approved fragment untouched.

- [ ] **Step 6: Commit the Emerald integration**

Run:

```bash
git add app Cargo.toml Cargo.lock crates docs
git commit -m "fix: pin safety-hang Malachite candidate"
```

Adjust the path list to the files actually changed. Expected: the commit contains only the dependency pin, defensive
runtime policy, focused test, and approved documentation/changelog edits.

## Task 8: Run Full Qualification and Update Both Pull Requests

**Files:**
- Modify: Malachite and Emerald qualification records
- Modify: PR #3 and PR #68 descriptions after local evidence is complete

**Interfaces:**
- Produces: exact local results, final SHAs, remaining external gates, and linked PR evidence.
- Does not produce: a production rollout claim or a green-CI claim before remote checks finish.

- [ ] **Step 1: Run Malachite focused and workspace validation**

Run from `malachite/code/`:

```bash
cargo nextest run -p arc-malachitebft-test --test unit -E 'test(wal_compatibility)' --retries 0
cargo nextest run -p arc-malachitebft-test --features wal-force-compression \
  --test unit -E 'test(wal_compatibility)' --retries 0
cargo nextest run -p arc-malachitebft-app-channel --test wal_safety_hang --retries 0
cargo nextest run -p arc-malachitebft-test --test it -E 'test(value_sync)' --retries 0
cargo all-tests
cargo clippy --tests -- -D warnings
cargo +nightly fmt --all --check
```

Run networked integration tests outside the restricted sandbox if loopback binding is denied. Record every command,
exit code, test count, and any environment limitation.

- [ ] **Step 2: Run Emerald validation**

Run from `om-emerald/`:

```bash
cargo nextest run --workspace --retries 0
cargo clippy --tests -- -D warnings
cargo +nightly fmt --all --check
taplo fmt --check
```

Then rerun the existing Phase 2 model-based/Docker qualification using the repository's documented command. Record
the exact Reth revision, Malachite SHA, Emerald SHA, scenario count, failures, and environment. Do not substitute a
unit suite for unavailable model-based coverage.

- [ ] **Step 3: Review the final data and migration boundary**

Inspect both diffs and confirm there is no persisted-format or schema change beyond the already reviewed v0.7
`0x08` record. Confirm no test or runbook deletes a WAL, and confirm all dependency pins use one SHA.

Run:

```bash
git diff 1d9010a7da7ced32568f7dea2c3dcbcbf112574a...HEAD --stat
git diff --check
git -C ../om-emerald diff 1d78818ac6b3c62a38e2ecdbc799e665bc07bde6...HEAD --stat
git -C ../om-emerald diff --check
```

- [ ] **Step 4: Request code review before push**

Use `superpowers:requesting-code-review` on both final diffs. Resolve safety-critical findings and rerun affected
tests. Defer unrelated cleanup explicitly rather than mixing it into these branches.

- [ ] **Step 5: Push, update the PRs, and watch CI**

Push both existing branches. Update PR #3 first with the final Malachite SHA and evidence, then update PR #68 with
that exact dependency pin. Summarize each review finding, fix, test, and remaining gate. Use `gh pr checks --watch`
or bounded polling and distinguish pending, skipped, failed, and passed jobs.

- [ ] **Step 6: Final handoff**

Report:

- final Malachite and Emerald SHAs;
- local commands and exact outcomes;
- remote CI status at those SHAs;
- model-based/Docker qualification status;
- the no-data-migration conclusion and `0x08` downgrade boundary;
- the operator action required after a safety hang; and
- any remaining merge or production-qualification gates.

Do not merge either PR without explicit user direction.
