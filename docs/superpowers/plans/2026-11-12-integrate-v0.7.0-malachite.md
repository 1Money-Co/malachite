# Malachite v0.7.0 Fork Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a reviewed 1Money Malachite candidate based on Circle v0.7.0 while retaining every qualified
downstream invariant and removing delayed WAL replay.

**Architecture:** Characterize the fork before merging, merge Circle implementation commit `5cd137fb` as an explicit
ancestry checkpoint, and reconcile behavior in focused follow-up commits. Merge release commit `32abf3cf` only after
the implementation candidate is stable, then hand its exact SHA to the linked Emerald plan.

**Tech Stack:** Rust 1.88, Cargo workspace, Tokio, ractor, libp2p, prost, ECDSA, LZ4 WAL framing, cargo-nextest,
Git merge ancestry.

**Spec:** `docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`

**Dependent plan:** `docs/superpowers/plans/2026-11-12-integrate-v0.7.0-emerald.md`

## Global Constraints

- Start from `1Money-Co/malachite@6f544d7efca221ee8e7d2da9af37795ed6745f99`.
- Merge implementation commit `5cd137fb158fc52b55044eee9b090fd0d34996b0` before release commit
  `32abf3cf952bb5fe03de914435e902a782632b59`; preserve both in ancestry.
- The implementation checkpoint already contains the `arc-malachitebft-*` rename from `8d8c224c`.
- Preserve the phase-1 behavior inventory until replacement evidence satisfies each recorded retirement gate.
- Preserve raw fixture provenance; never regenerate old fixtures from the candidate to resolve a mismatch.
- Preserve old WAL tags `0x01`, `0x02`, and `0x04`; add polka-certificate tag `0x08`.
- Remove delayed WAL replay, `WaitingForSync`, its timer, and its runtime configuration effect.
- Do not import the broader v0.8.1 WAL replay index or unrelated v0.8.1 changes.
- Keep Malachite generic; Emerald codecs, storage, key providers, and Engine API behavior stay in Emerald.
- Retain fork-owned QA and explicit managed-action reverts; do not accept supply-chain policy changes implicitly.
- Review required `Cargo.lock` changes explicitly and never format the lockfile.
- Keep Markdown at or below 120 columns.
- Before push, run `cargo clippy --tests -- -D warnings` and `cargo +nightly fmt --all --check` from `code/`.

## Review Focus

- Workspace-wide `--all-features` must not compare a compressed rewrite with a raw fixture hash; Task 2 tests raw,
  explicit forced-compression, and workspace-all-feature invocations.
- A valid proof from a key outside the current validator set must remain stored but unclassified; Task 4 tests later
  promotion and demotion from the stored proof.
- Delayed, stale, partial, unsolicited, and wrong-peer sync replies must not leak or free the wrong capacity; Task 5
  ports the phase-1 lifecycle matrix to the v0.7 queue.
- A sync certificate must never skip crash-state WAL replay; Task 6 asserts `WalReplayBegin` precedes sync recovery.
- A `0x08` record must decode and replay on the candidate while failing explicitly on the old decoder; Task 6 tests
  both sides of that downgrade boundary.

---

## File Structure

### Provenance and policy

- `DOWNSTREAM_CHANGES.md` — v0.7 source map, retained/replaced behavior, evidence, and retirement gates.
- `RELEASE_NOTES.md`, `BREAKING_CHANGES.md` — upstream release history plus the bounded 1Money integration note.

### WAL compatibility and recovery

- `code/crates/test/Cargo.toml` — exposes a focused test feature for forced WAL compression.
- `code/crates/test/tests/unit/wal_compatibility.rs` — immutable raw fixtures and framing-aware rewrite assertions.
- `code/crates/test/tests/unit/wal_codec.rs` — all WAL entry tags, including polka certificate `0x08`.
- `code/crates/test/tests/it/wal.rs` — crash, restart, immediate replay, corruption, and sync interaction.
- `code/crates/engine/src/{consensus.rs,wal.rs,wal/entry.rs}` — immediate replay and WAL entry encoding.
- `code/crates/config/src/lib.rs` — no active delayed-replay configuration.

### Lifecycle, signing, and network identity

- `code/crates/app-channel/src/{builder.rs,connector.rs,run.rs,spawn.rs}` — builder assembly and request lifecycle.
- `code/crates/app/src/{config.rs,spawn.rs}` — generic node configuration and actor ownership.
- `code/crates/signing/src/{lib.rs,ext.rs}` — separate `Signer` and `Verifier` contracts.
- `code/crates/signing-ecdsa/` — retained ECDSA encodings and known-answer fixtures under the renamed package.
- `code/crates/core-types/src/{context.rs,timeouts.rs,height_params.rs,validator_proof.rs}` — v0.7 context surface.
- `code/crates/network/src/{state.rs,validator_proof/}` — proof transport, validation, storage, and classification.

### Synchronization and downstream invariants

- `code/crates/sync/src/{effect.rs,handle.rs,state.rs,scoring.rs}` — v0.7 queue and scoring implementation.
- `code/crates/engine/src/{consensus.rs,network.rs,sync.rs}` — actor ordering and response forwarding.
- `code/crates/test/tests/unit/sync_lifecycle.rs` — phase-1 lifecycle characterization on the new queue.
- `code/crates/test/tests/it/value_sync.rs` — peer restart, response bounds, partial replies, and catch-up.
- `code/crates/app-channel/tests/consensus_request.rs` — closed/full/dropped reply distinctions.

## Task 1: Extend the Inventory to the v0.7 Checkpoint

**Files:**
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: phase-1 inventory rows and the exact source commits in Global Constraints.
- Produces: v0.7 decision rows that Tasks 2–8 update with commands, results, and final disposition.

- [ ] **Step 1: Verify the starting point and source objects**

Run:

```bash
git rev-parse HEAD
git merge-base --is-ancestor 6f544d7efca221ee8e7d2da9af37795ed6745f99 HEAD
git cat-file -e 5cd137fb158fc52b55044eee9b090fd0d34996b0^{commit}
git cat-file -e 32abf3cf952bb5fe03de914435e902a782632b59^{commit}
```

Expected: `HEAD` contains the approved design; all remaining commands exit 0.

- [ ] **Step 2: Add the v0.7 provenance and behavior table**

Add rows for builder lifecycle, crate rename, signer/verifier split, validator proofs, height parameters, sync queue,
proposal-part forwarding, WAL `0x08`, delayed-replay removal, and WAL feature-matrix coverage. Each row names its
upstream commit, fork behavior, decision, evidence task, and retirement gate; unresolved evidence remains pending.

- [ ] **Step 3: Record upstream ports without retiring downstream evidence**

Record `628d8cff`, `8ee5d998`, and `5cd137fb` as replacement inputs. Map each phase-1 row to exact, partial, or no
replacement and state which phase-1 test must still pass before the old implementation can be removed.

- [ ] **Step 4: Validate and commit**

Run:

```bash
awk 'length($0) > 120 { print FNR ":" length($0); bad=1 } END { exit bad }' DOWNSTREAM_CHANGES.md
git diff --check
git add DOWNSTREAM_CHANGES.md
git commit -m "docs: map v0.7 downstream behavior"
```

Expected: checks emit no output; the commit contains only the inventory.

## Task 2: Make WAL Compatibility Tests Framing-Aware

**Files:**
- Modify: `code/crates/test/Cargo.toml`
- Modify: `code/crates/test/tests/unit/wal_compatibility.rs`
- Modify: `.github/workflows/rust.yml`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: immutable raw fixture hashes and `malachitebft-wal/force-compression`.
- Produces: test feature `wal-force-compression` and framing-aware `assert_fixture(...)` behavior.

- [ ] **Step 1: Reproduce both modes before changing assertions**

Run from `code/`:

```bash
cargo nextest run -p informalsystems-malachitebft-test --test unit \
  production_wal_fixture_decodes_and_rewrites_canonically --retries 0
cargo nextest run --workspace --all-features \
  production_wal_fixture_decodes_and_rewrites_canonically --retries 0
```

Expected: package-scoped mode passes; workspace all-features fails because the rewrite is compressed.

- [ ] **Step 2: Expose the focused compression feature**

Add direct workspace dependency `malachitebft-wal` and feature
`wal-force-compression = ["malachitebft-wal/force-compression"]` to the test crate.

- [ ] **Step 3: Write framing-aware assertions**

Keep source length, checksum, decoded entries, and historical-vote assertions unconditional. In raw mode require the
rewrite checksum to equal the fixture. With `cfg!(feature = "wal-force-compression")`, require the first entry flag
at byte offset 12 to be `1`, require the raw hash to differ, reopen the rewrite, and apply `assert_entries` to it.

- [ ] **Step 4: Add CI coverage for both modes**

In the Rust workflow run `wal_compatibility` once with default features and once with
`--features wal-force-compression`. Keep the workspace all-features invocation covered by the ordinary suite.

- [ ] **Step 5: Verify and commit**

Run from `code/`:

```bash
cargo nextest run -p informalsystems-malachitebft-test --test unit wal_compatibility
cargo nextest run -p informalsystems-malachitebft-test --features wal-force-compression \
  --test unit wal_compatibility
cargo nextest run --workspace --all-features wal_compatibility --retries 0
```

Expected: all three commands pass and the checked-in fixture hashes remain unchanged.

```bash
git add .github/workflows/rust.yml code/crates/test/Cargo.toml \
  code/crates/test/tests/unit/wal_compatibility.rs DOWNSTREAM_CHANGES.md
git commit -m "test: cover raw and compressed WAL rewrites"
```

## Task 3: Merge the v0.7 Implementation Checkpoint

**Files:**
- Modify: the implementation conflict set reported by `git merge-tree --write-tree HEAD 5cd137fb`
- Delete: `code/crates/starknet/` and `code/examples/channel/` as removed upstream
- Preserve: `qa/`, `.github/workflows/qa.yml`, downstream fixtures, specs, and plans

**Interfaces:**
- Consumes: Circle implementation tree at `5cd137fb` and Tasks 1–2 characterization evidence.
- Produces: an explicit merge commit with `5cd137fb` as parent and no unresolved index entries.

- [ ] **Step 1: Preview and save the exact conflict list**

Run:

```bash
git merge-tree --write-tree HEAD 5cd137fb158fc52b55044eee9b090fd0d34996b0
git status --short
```

Expected: the preview reports workflow, workspace, app-channel, consensus, engine, ECDSA, sync, and test conflicts;
the working tree remains clean.

- [ ] **Step 2: Start the implementation merge**

```bash
git merge --no-ff --no-commit 5cd137fb158fc52b55044eee9b090fd0d34996b0
```

Expected: Git stops on the previewed conflicts.

- [ ] **Step 3: Resolve repository and workspace conflicts**

Accept the upstream Rust 1.88 floor, `arc-malachitebft-*` package names, removal of Starknet/example-channel code, and
new generic test app. Retain fork QA, downstream fixtures/docs, and explicit managed-action reverts. Regenerate the
lockfile only through Cargo after manifests are resolved; inspect rather than format it.

- [ ] **Step 4: Resolve runtime conflicts by target architecture**

Use v0.7 builder/context/signing/network/sync structures as the target. Preserve phase-1 request errors, ECDSA bytes,
sync tests, WAL fixtures, same-height WAL reply, diagnostics, startup ordering, and dedicated sync-input isolation.
Do not retain obsolete Starknet-only adapters merely to make a conflict disappear.

- [ ] **Step 5: Verify the merge checkpoint and commit it**

Run:

```bash
git diff --name-only --diff-filter=U
git diff --check
cargo metadata --manifest-path code/Cargo.toml --no-deps --format-version 1 >/dev/null
git commit
git merge-base --is-ancestor 5cd137fb158fc52b55044eee9b090fd0d34996b0 HEAD
```

Expected: no unresolved paths; metadata parses; the merge commit retains Circle's commit as an ancestor. Compilation
failures caused by application API reconciliation are allowed only if listed explicitly for Task 4.

## Task 4: Reconcile Builder, Signing, and Validator-Proof Contracts

**Files:**
- Modify: `code/crates/app-channel/src/{builder.rs,connector.rs,run.rs,spawn.rs}`
- Modify: `code/crates/app/src/{config.rs,spawn.rs}`
- Modify: `code/crates/signing-ecdsa/src/lib.rs`
- Modify: `code/crates/signing-ecdsa/tests/known_answers.rs`
- Modify: `code/crates/test/app/src/node.rs`
- Modify: `code/crates/test/tests/unit/validator_proof.rs`
- Modify: `code/crates/network/src/state.rs`
- Modify: `code/crates/app-channel/tests/consensus_request.rs`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: `EngineBuilder`, `ConsensusContext::{new_validator,new_full_node}`, `Signer`, `Verifier`, and canonical
  `ValidatorProof::signing_bytes(public_key, peer_id)` from v0.7.
- Produces: compiling generic validator/full-node construction and preserved request/proof behavior.

- [ ] **Step 1: Port the failing lifecycle and request tests first**

Retain tests for `ConsensusRequestError::{Closed,Full,Recv}` and add builder tests named
`validator_context_requires_signer` and `full_node_context_uses_verifier_only`. Run them and record the expected
compile/test failures before adapting the implementation.

- [ ] **Step 2: Reconcile the builder lifecycle**

Keep actor creation in dependency order, install consensus before sync subscription, preserve asynchronous `Decided`
replies, and retain operator-visible errors for dropped application replies. Remove the old `Node`-trait-dependent
assembly from the runtime path.

- [ ] **Step 3: Preserve ECDSA known answers under the renamed crate**

Adapt imports and manifests to `arc-malachitebft-signing-ecdsa` without changing any fixture bytes, JSON encodings,
key derivation, or verification outcomes.

- [ ] **Step 4: Pin the exact validator-proof protocol**

Retain the upstream `PoV || u32_be(pk_len) || pk || u32_be(peer_len) || peer` tests. Add/retain state tests proving a
valid unknown key stays unclassified and is promoted/demoted on validator-set updates. Retain duplicate, peer-ID
mismatch, invalid-signature, reconnect-reset, and malformed-decode-without-promotion behavior.

- [ ] **Step 5: Verify and commit**

Run from `code/`:

```bash
cargo test -p arc-malachitebft-app-channel --test consensus_request
cargo test -p arc-malachitebft-signing-ecdsa --all-features --test known_answers
cargo test -p arc-malachitebft-test --test unit validator_proof
cargo test -p arc-malachitebft-network
cargo check --workspace --all-targets --all-features
```

Expected: all commands pass.

```bash
git add code/crates/app-channel code/crates/app code/crates/signing-ecdsa \
  code/crates/test/app code/crates/test/tests/unit/validator_proof.rs code/crates/network/src/state.rs \
  DOWNSTREAM_CHANGES.md
git commit -m "fix: reconcile v0.7 lifecycle and signing"
```

## Task 5: Reconcile v0.7 Sync with Downstream Invariants

**Files:**
- Modify: `code/crates/sync/src/{effect.rs,handle.rs,state.rs,scoring.rs}`
- Modify: `code/crates/engine/src/{consensus.rs,network.rs,sync.rs}`
- Modify: `code/crates/metrics/src/metrics.rs`
- Modify: `code/crates/test/tests/unit/sync_lifecycle.rs`
- Modify: `code/crates/test/tests/it/value_sync.rs`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: v0.7 `sync_height`, pending request IDs, partial-success scoring, and consensus response queue.
- Produces: one bounded request lifecycle that preserves phase-1 startup, validation, retry, and diagnostics.

- [ ] **Step 1: Port the phase-1 lifecycle matrix as failing tests**

Keep the stable test names for restored-height startup, obsolete replies, partial suffixes, original-range validation,
capacity saturation, wrong-peer replies, unknown replies, and processing-error retry. Update only construction APIs so
the assertions still describe the phase-1 behavior.

- [ ] **Step 2: Add the concurrent rewind regression**

Assert a failed/retried response cannot advance a concurrently rewound `sync_height`; the next request must begin at
the earliest missing height and may not duplicate a consensus-pending height.

- [ ] **Step 3: Reconcile queue and scoring behavior**

Use upstream request IDs and partial-success scoring. Validate sender, requested range, contiguity, certificate/value
identity, and combined capacity before forwarding. Penalize peer-caused invalid data only; local codec, host, actor,
storage, and application failures remain local failures and release the affected height for retry.

- [ ] **Step 4: Preserve actor ordering and diagnostics**

Install consensus before subscribing to sync events, retain bounded sync-input isolation and sync-first draining, and
report inflight/pending counts, lowest starts, and local tip from one snapshot when saturated.

- [ ] **Step 5: Verify and commit**

Run from `code/`:

```bash
cargo test -p arc-malachitebft-test --test unit sync_lifecycle
cargo nextest run -p arc-malachitebft-test --test it value_sync --retries 0 --test-threads 1
cargo test -p arc-malachitebft-sync
```

Expected: the unit matrix and all value-sync scenarios pass without retry.

```bash
git add code/crates/sync code/crates/engine/src code/crates/metrics/src/metrics.rs \
  code/crates/test/tests/unit/sync_lifecycle.rs code/crates/test/tests/it/value_sync.rs \
  DOWNSTREAM_CHANGES.md
git commit -m "fix: preserve sync recovery on v0.7"
```

## Task 6: Remove Delayed Replay and Complete WAL v0.7 Compatibility

**Files:**
- Modify: `code/crates/config/src/lib.rs`
- Modify: `code/crates/engine/src/consensus.rs`
- Modify: `code/crates/engine/src/wal/entry.rs`
- Modify: `code/crates/test/tests/unit/{wal_codec.rs,wal_compatibility.rs}`
- Modify: `code/crates/test/tests/it/wal.rs`
- Modify: `docs/architecture/adr-007-write-ahead-log.md`
- Modify: `DOWNSTREAM_CHANGES.md`

**Interfaces:**
- Consumes: v0.7 `WalEntry::PolkaCertificate`, tag `0x08`, and immediate-replay subset of the v0.8.1 disposition.
- Produces: deterministic replay before sync recovery and documented one-way downgrade after a `0x08` write.

- [ ] **Step 1: Write immediate-replay regressions**

Add tests named `non_empty_wal_replays_before_sync_recovery`, `sync_certificate_cannot_skip_recorded_wal`,
`corrupt_replay_fails_closed`, and `empty_wal_and_full_node_start_without_replay`. Assert event ordering and exact
recorded signed bytes, not only final height.

- [ ] **Step 2: Remove the delayed path**

Remove `WalReplayDelayElapsed`, `WaitingForSync`, pending WAL entries, the timer handle, sync-message bypass, and all
branches that reset or skip WAL because sync won the delay. Replay non-empty validator WAL immediately. Do not copy
the v0.8.1 replay index.

- [ ] **Step 3: Remove runtime configuration effect**

Remove `ConsensusConfig::wal_replay_delay`. Add a deserialization regression showing a legacy TOML key is ignored and
does not alter startup; document that it has no runtime effect.

- [ ] **Step 4: Pin WAL tag and downgrade behavior**

Round-trip every entry variant and assert the polka certificate uses tag byte `0x08`. Reopen the phase-1 fixtures,
then create a `0x08` fixture. Copy that fixture into a detached worktree at `6f544d7` and run its old decoder; require
an unknown-tag error and unchanged fixture hash rather than truncation, skip, or deletion.

- [ ] **Step 5: Verify and commit**

Run from `code/`:

```bash
cargo test -p arc-malachitebft-test --test unit wal_codec
cargo test -p arc-malachitebft-test --test unit wal_compatibility
cargo nextest run -p arc-malachitebft-test --test it wal --retries 0 --test-threads 1
cargo test -p arc-malachitebft-config
```

Expected: old fixtures decode, new entries round-trip, replay is immediate, and corruption/downgrade fail explicitly.

```bash
git add code/crates/config code/crates/engine/src/consensus.rs code/crates/engine/src/wal \
  code/crates/test/tests docs/architecture/adr-007-write-ahead-log.md DOWNSTREAM_CHANGES.md
git commit -m "fix: replay WAL immediately on recovery"
```

## Task 7: Merge the v0.7.0 Release Commit

**Files:**
- Modify: `BREAKING_CHANGES.md`
- Modify: `RELEASE_NOTES.md`
- Modify: `code/Cargo.toml`
- Modify: `code/Cargo.lock`

**Interfaces:**
- Consumes: stable implementation candidate with `5cd137fb` in ancestry.
- Produces: explicit `32abf3cf` ancestry and version `0.7.0` for every `arc-malachitebft-*` package.

- [ ] **Step 1: Prove the release delta is bounded**

```bash
git diff --name-only 5cd137fb158fc52b55044eee9b090fd0d34996b0..32abf3cf952bb5fe03de914435e902a782632b59
git merge-base --is-ancestor 5cd137fb158fc52b55044eee9b090fd0d34996b0 HEAD
```

Expected: only `BREAKING_CHANGES.md`, `RELEASE_NOTES.md`, `code/Cargo.toml`, and `code/Cargo.lock`; ancestry exits 0.

- [ ] **Step 2: Merge the release commit separately**

```bash
git merge --no-ff 32abf3cf952bb5fe03de914435e902a782632b59
```

Resolve only fork release-note and lockfile conflicts. Do not re-resolve implementation behavior in this merge.

- [ ] **Step 3: Verify package identity and ancestry**

Run from `code/`:

```bash
cargo metadata --no-deps --format-version 1
cargo check --workspace --all-targets --all-features
git merge-base --is-ancestor 32abf3cf952bb5fe03de914435e902a782632b59 HEAD
```

Expected: workspace packages use version `0.7.0` and `arc-malachitebft-*`; all commands succeed.

## Task 8: Close the Malachite Development Gate

**Files:**
- Modify: `DOWNSTREAM_CHANGES.md`
- Modify: `RELEASE_NOTES.md`

**Interfaces:**
- Consumes: Tasks 1–7 commands and their actual results.
- Produces: exact candidate SHA and evidence record consumed by the Emerald plan.

- [ ] **Step 1: Run focused compatibility suites**

Run from `code/`:

```bash
cargo test -p arc-malachitebft-signing-ecdsa --all-features --test known_answers
cargo test -p arc-malachitebft-app-channel --test consensus_request
cargo test -p arc-malachitebft-test --test unit
cargo nextest run -p arc-malachitebft-test --test it wal --retries 0 --test-threads 1
cargo nextest run -p arc-malachitebft-test --test it value_sync --retries 0 --test-threads 1
```

Expected: every suite passes without retry.

- [ ] **Step 2: Run workspace validation**

Run from `code/` outside a network-restricted sandbox:

```bash
cargo build --workspace --all-features
make tests
cargo clippy --tests -- -D warnings
cargo +nightly fmt --all --check
```

Expected: all commands pass. Report unavailable Quint, network, or host requirements rather than treating them as
success.

- [ ] **Step 3: Audit the complete branch**

Run:

```bash
git diff --check origin/main...HEAD
git diff --stat origin/main...HEAD
git log --oneline --graph --decorate origin/main..HEAD
git rev-parse HEAD
```

Confirm both Circle commits appear in ancestry, no phase-1 fixture changed, no stale
`informalsystems-malachitebft` package name remains, and the lockfile change follows manifests.

- [ ] **Step 4: Record evidence and commit**

Update every v0.7 inventory row with its exact command/result, record limitations, and name the final candidate SHA
for Emerald. State explicitly that development completion is not phase-1 qualification or production readiness.

```bash
git add DOWNSTREAM_CHANGES.md RELEASE_NOTES.md
git commit -m "docs: record v0.7 integration evidence"
```
