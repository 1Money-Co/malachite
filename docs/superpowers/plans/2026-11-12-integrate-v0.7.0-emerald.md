# Emerald Malachite v0.7.0 Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate the reviewed 1Money Malachite v0.7.0 candidate into Emerald while preserving key identity,
authenticated proposal forwarding, persisted state, node roles, and failure behavior.

**Architecture:** Capture immutable Emerald phase-1 compatibility fixtures before changing the dependency, then pin
the exact Malachite candidate produced by the linked plan. Adapt configuration, context, signing, codecs, and node
assembly in focused commits; validate old-state restart and the new `0x08` WAL boundary before opening the linked PR.

**Tech Stack:** Rust 1.91.1, Cargo, Tokio, ractor, libp2p, prost, k256 ECDSA, redb, Reth Engine API, mdBook,
cargo-nextest, Taplo.

**Spec:** `../malachite/docs/superpowers/specs/2026-11-12-integrate-v0.7.0-design.md`

**Prerequisite plan:** `../malachite/docs/superpowers/plans/2026-11-12-integrate-v0.7.0-malachite.md`

## Global Constraints

- Start from refreshed `1Money-Co/om-emerald@d681a119fee2e4b27351c1553bfea016bb2fe158` or the then-current
  `origin/main`; record the exact base SHA.
- Create `seb/integrate-malachite-v0.7.0`; never commit implementation work on `main`.
- Pin the full Malachite candidate SHA produced by the prerequisite plan, not a branch, tag, or abbreviated SHA.
- Keep local dependency aliases `malachitebft-*`, but set every package name to `arc-malachitebft-*`.
- Preserve fixed k256 address, public-key, signature, JSON, protobuf, network-peer, and cloud-key-provider contracts.
- Preserve authenticated proposal forwarding at the Emerald application layer after trait-level part signing is gone.
- Preserve old Emerald application WAL and redb/history reads; add new polka-certificate WAL tag `0x08` support.
- Do not delete, reset, or auto-rewrite persisted state to recover from a compatibility failure.
- Treat the old decoder error plus recovery documentation as the downgrade boundary; add no marker or CLI.
- `wal_replay_delay` has no runtime effect; retained legacy input may parse only as ignored configuration.
- Keep Emerald generic and free of 1Money bridge-specific behavior.
- Review `Cargo.lock` changes explicitly and never format the lockfile.
- Phase-1 qualification may run in parallel, but phase-2 must not merge without linked evidence and human approval.
- Keep Markdown at or below 120 columns.
- Before push run `cargo clippy --tests -- -D warnings`, `cargo +nightly fmt --all --check`, and
  `taplo fmt --check` from the Emerald root.

## Review Focus

- A cloud-loaded key must produce the same consensus address, peer ID, and proof as the file-loaded key; Task 4 uses
  the existing envelope vector for both provider paths.
- An unknown but cryptographically valid validator proof must not grant validator status until the set changes;
  Task 5 tests validator/full-node role construction and later classification.
- Proposal parts delivered by a forwarder must still authenticate the declared proposer and content; Task 6 retains
  wrong-publisher, altered-content, hidden-lock, and restream regressions.
- Existing WAL and redb/history files must restart without mutation; Tasks 1 and 6 use fixtures generated before the
  dependency bump and compare state after reopening.
- Once tag `0x08` is written, an old decoder must fail explicitly rather than skip or truncate the entry; Task 6 pins
  both the candidate restart and the phase-1 rejection.

---

## File Structure

### Phase-1 compatibility fixtures

- `types/examples/phase1_consensus_fixture.rs` — deterministic k256 and protobuf fixture generator.
- `types/tests/fixtures/phase1_consensus.json` — exact phase-1 address, peer ID, sign bytes, signatures, and codecs.
- `types/tests/phase1_compatibility.rs` — immutable fixture assertions across the dependency update.
- `app/examples/phase1_state_fixture.rs` — deterministic Emerald WAL plus redb/history fixture generator.
- `app/tests/fixtures/phase1-state/` — source manifest, application WAL, and minimal store database.
- `app/tests/phase1_restart.rs` — old-state open, replay, and restart assertions.

### Configuration and consensus context

- `cli/src/config.rs` — Emerald-owned timeout settings flattened under `[consensus]` and `NodeConfig` projection.
- `types/src/context.rs` — `Context::Timeouts = LinearTimeouts`.
- `app/src/state.rs` — current height's timeout policy and validator set.
- `app/src/app.rs` — `HeightParams` replies for start, next height, and restart.
- `cli/src/{new.rs,cmd/testnet/add_node.rs,cmd/testnet/generate.rs}` — explicit validator/full-node configuration.

### Signing, proof, and codec integration

- `types/src/signing/secp256k1.rs` — separate `K256Signer` and `K256Verifier` implementations.
- `types/proto/consensus.proto` — `ValidatorProof` wire message.
- `types/src/codec/proto/mod.rs` — proof and direct polka-certificate codecs.
- `app/src/node.rs` — one key resolution, proof creation, network identity, and `EngineBuilder` assembly.

### Application behavior and documentation

- `app/src/{app.rs,state.rs,streaming.rs,proposal_forwarding_tests.rs}` — authenticated proposal transport.
- `app/src/{store.rs,store/proposal_metadata.rs}` — unchanged redb/history behavior across restart.
- `docs/operational-docs/src/{architecture/consensus.md,architecture/syncing.md,config-examples.md}` — v0.7 API,
  replay, configuration, and downgrade position.
- `Cargo.toml`, `Cargo.lock` — exact package aliases and candidate pin.

## Task 1: Create the Emerald Branch and Freeze Phase-1 Compatibility

**Files:**
- Create: `types/examples/phase1_consensus_fixture.rs`
- Create: `types/tests/fixtures/phase1_consensus.json`
- Create: `types/tests/fixtures/README.md`
- Create: `types/tests/phase1_compatibility.rs`
- Create: `app/examples/phase1_state_fixture.rs`
- Create: `app/tests/fixtures/phase1-state/{manifest.json,consensus.wal,store.db}`
- Create: `app/tests/phase1_restart.rs`

**Interfaces:**
- Consumes: current Emerald main, Malachite pin `eb2405ba`, key `[0x01; 32]`, and current store/WAL codecs.
- Produces: immutable fixture manifests naming both full source SHAs and tests reused after the dependency bump.

- [ ] **Step 1: Create the implementation branch from refreshed main**

Run in `om-emerald/`:

```bash
git fetch origin main
git switch -c seb/integrate-malachite-v0.7.0 origin/main
git rev-parse HEAD
git status --short --branch
```

Expected: the new branch tracks the refreshed base and the worktree is clean. Record the full base SHA in the fixture
README and later PR.

- [ ] **Step 2: Write the deterministic consensus fixture generator**

Generate, from private key `[0x01; 32]`, the checksummed address
`0x1a642f0E3c3aF545E7AcBD38b07251B3990914F1`, peer ID
`16Uiu2HAmEWQnHq2jLKJypwVnVoQeFCULuyop6atvq2eWjYSUjzNi`, compressed public key, fixed vote/proposal
sign bytes and signatures, JSON encodings, and protobuf bytes. Include full Emerald and Malachite source SHAs and no
timestamps or host paths.

- [ ] **Step 3: Write the deterministic state fixture generator**

Create the smallest application WAL containing a signed vote, timeout, and proposed value at height 7, plus a store
database containing the matching decided value, certificate, history minimum, and proposal metadata. Record file
lengths and SHA-256 values in `manifest.json`.

- [ ] **Step 4: Generate fixtures on the phase-1 dependency**

```bash
cargo run -p malachitebft-eth-types --example phase1_consensus_fixture
cargo run -p emerald --example phase1_state_fixture
```

Expected: generated manifests name the current Emerald base and exact `eb2405ba` dependency. Consensus and WAL bytes
are repeatable; the store fixture has the exact semantic contents and a recorded immutable hash. Do not regenerate
these files after the dependency bump.

- [ ] **Step 5: Write and run baseline fixture tests**

`phase1_compatibility` asserts every exact field and rejects a mutated message/signature. `phase1_restart` copies the
fixtures to a temporary directory, opens without truncation, restores the recorded vote and store tip, and asserts
the original fixture hashes remain unchanged.

```bash
cargo test -p malachitebft-eth-types --test phase1_compatibility
cargo test -p emerald --test phase1_restart
```

Expected: both pass on the old pin.

- [ ] **Step 6: Commit**

```bash
git add types/examples types/tests app/examples app/tests
git commit -m "test: pin phase1 Emerald compatibility"
```

## Task 2: Pin the Exact v0.7 Candidate and Package Aliases

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: full candidate SHA recorded by Malachite Plan Task 8.
- Produces: one git source/revision for all Malachite aliases, each mapped to its `arc-malachitebft-*` package.

- [ ] **Step 1: Record and verify the candidate SHA**

```bash
MALACHITE_CANDIDATE=$(git -C ../malachite rev-parse HEAD)
git -C ../malachite merge-base --is-ancestor \
  32abf3cf952bb5fe03de914435e902a782632b59 "$MALACHITE_CANDIDATE"
git -C ../malachite status --short
```

Expected: ancestry exits 0 and the Malachite worktree is clean. Copy the printed full SHA into every manifest entry;
do not leave the shell variable or a symbolic reference in committed files.

- [ ] **Step 2: Update dependency package mappings**

Keep alias keys such as `malachitebft-app`, but change `package` values from
`informalsystems-malachitebft-*` to `arc-malachitebft-*` and set every Malachite `rev` to the full candidate SHA.

- [ ] **Step 3: Update and audit the lockfile**

```bash
cargo check --workspace --all-targets --all-features
rg -n 'informalsystems-malachitebft|arc-malachitebft|1Money-Co/malachite' Cargo.toml Cargo.lock
git diff -- Cargo.toml Cargo.lock
```

Expected: compilation now exposes only expected v0.7 API failures; no old package name or old revision remains; the
lockfile source uses exactly the candidate SHA. Do not format the lockfile.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: pin Malachite v0.7 candidate"
```

## Task 3: Preserve Timeout Configuration through `HeightParams`

**Files:**
- Modify: `cli/src/config.rs`
- Modify: `types/src/context.rs`
- Modify: `app/src/state.rs`
- Modify: `app/src/app.rs`
- Modify: `cli/src/new.rs`
- Modify: `cli/src/cmd/testnet/{add_node.rs,generate.rs}`
- Modify: `docs/operational-docs/src/config-examples/malachitebft-config.toml`

**Interfaces:**
- Consumes: v0.7 `LinearTimeouts`, `HeightParams::new(validator_set, timeouts, target_time)`, and existing flattened
  `[consensus] timeout_*` keys.
- Produces: `ConsensusSettings { engine: ConsensusConfig, timeouts: TimeoutSettings }` and exact per-height replies.

- [ ] **Step 1: Write configuration and height-parameter tests**

Add tests named `legacy_consensus_timeouts_parse_unchanged`, `legacy_wal_replay_delay_has_no_effect`,
`consensus_ready_returns_current_height_params`, and `next_height_uses_updated_validator_set_and_same_timeouts`.
Use non-default timeout values so accidental fallback to `LinearTimeouts::default()` fails.

- [ ] **Step 2: Add Emerald-owned timeout settings**

Define serializable `TimeoutSettings` with the seven existing `timeout_*` fields and a conversion to
`LinearTimeouts`. Define `ConsensusSettings` with both `engine: ConsensusConfig` and `timeouts: TimeoutSettings`
flattened under `[consensus]`; implement `NodeConfig` using `engine`.

- [ ] **Step 3: Attach timeouts to application state**

Set `EmeraldContext::Timeouts = LinearTimeouts`. Pass converted timeouts into `State::new` and return
`HeightParams::new(validator_set, timeouts, None)` from `ConsensusReady`, successful finalization, and restart paths.
Keep `min_block_time` in the application; do not map it to `target_time`.

- [ ] **Step 4: Update config constructors and roles**

Update every `Config` literal to construct `ConsensusSettings`. Set `engine.enabled = false` for generated
non-validator/full nodes and `true` for validators; do not infer role from queue capacity.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p malachitebft-eth-cli config
cargo test -p emerald
cargo check --workspace --all-targets --all-features
```

Expected: configuration retains exact timeout values, the ignored replay-delay key cannot affect runtime state, and
all height callbacks carry explicit parameters.

```bash
git add cli/src types/src/context.rs app/src/state.rs app/src/app.rs \
  docs/operational-docs/src/config-examples/malachitebft-config.toml
git commit -m "feat: provide per-height consensus parameters"
```

## Task 4: Split k256 Signing and Add Validator-Proof Codecs

**Files:**
- Modify: `types/src/signing/secp256k1.rs`
- Modify: `types/src/signing/mod.rs`
- Modify: `types/proto/consensus.proto`
- Modify: `types/src/codec/proto/mod.rs`
- Modify: `types/tests/phase1_compatibility.rs`
- Modify: `crates/key-provider/src/{file.rs,aws_sm_kms.rs,gcp_sm_kms.rs}` only if callers require adaptation
- Modify: `crates/key-provider/src/gcp_sm_kms/tests.rs`

**Interfaces:**
- Consumes: `Signer<EmeraldContext>`, `Verifier<EmeraldContext>`, canonical `ValidatorProof`, and phase-1 k256 bytes.
- Produces: `K256Signer::new(PrivateKey)`, zero-state `K256Verifier`, proof codec, and direct polka-certificate codec.

- [ ] **Step 1: Write failing signer/verifier/proof tests**

Add tests for fixed vote/proposal signatures, canonical proof preimage, proof round-trip, tampered peer ID, malformed
public key, and polka-certificate round-trip. Assert proof preimage begins with `PoV` and has big-endian `u32` lengths;
do not include network ID, nonce, timestamp, or challenge.

- [ ] **Step 2: Split the provider**

Move private-key operations to `K256Signer` implementing `Signer<EmeraldContext>`. Implement
`K256Verifier: Verifier<EmeraldContext>` without private state. Retain explicit raw `sign`/`verify` helpers used by
Emerald's `ProposalFin` authentication and preserve phase-1 bytes exactly.

- [ ] **Step 3: Add proof and polka codecs**

Add protobuf `ValidatorProof { consensus_pub_key, peer_id, signature }` and implement
`Codec<ValidatorProof<EmeraldContext>>`. Add direct `Codec<PolkaCertificate<EmeraldContext>>` by reusing existing
encode/decode helpers; do not create a second wire representation.

- [ ] **Step 4: Prove file and cloud key equivalence**

Using the existing `[0x01; 32]` envelope fixture, assert file and cloud material produce the same address, peer ID,
proof public key, proof preimage, and valid proof signature. Keep provider parsing and custody roles unchanged.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p malachitebft-eth-types
cargo test -p key-provider
cargo test -p malachitebft-eth-types --test phase1_compatibility
```

Expected: all tests pass and the immutable phase-1 fixture remains byte-identical.

```bash
git add types crates/key-provider
git commit -m "feat: adapt k256 signing to Malachite v0.7"
```

## Task 5: Replace the `Node` Trait with `EngineBuilder`

**Files:**
- Modify: `app/src/node.rs`
- Modify: `app/src/main.rs`
- Modify: `cli/src/{new.rs,file.rs}` and command modules that require local node helper traits
- Modify: `app/Cargo.toml`

**Interfaces:**
- Consumes: `WalContext`, `NetworkContext`, `SyncContext`, `ConsensusContext`, `RequestContext`, `NetworkIdentity`,
  `K256Signer`, and `K256Verifier`.
- Produces: `App::build_runtime() -> eyre::Result<AppRuntime>` with one key resolution and explicit role assembly.

- [ ] **Step 1: Write role and lifecycle tests**

Add tests named `validator_builds_signing_consensus_context`, `full_node_builds_verifier_only_context`,
`unknown_valid_key_is_not_promoted_until_validator_update`, `startup_failure_returns_error`, and
`shutdown_stops_engine_and_application`. Use injected builder inputs; do not require Reth for construction tests.

- [ ] **Step 2: Build one network identity**

Resolve the configured private key once, derive its consensus public key/address and libp2p peer ID, sign the static
validator proof for that peer ID, encode it, and construct `NetworkIdentity::new_validator`. This proof advertises
key control but does not itself grant validator status.

- [ ] **Step 3: Assemble the engine explicitly**

Use `EngineBuilder::new(ctx, config)` with default WAL, network, sync, and request contexts. Choose
`ConsensusContext::new_validator` when `config.consensus.engine.enabled` is true and
`ConsensusContext::new_full_node` otherwise. Preserve WAL path, request capacity, metrics registry, event channel,
and app task ownership.

- [ ] **Step 4: Replace only the removed generic node helpers**

Move Emerald-specific genesis, key-file, network-key, start, run, and kill helpers onto `App` or narrow local traits.
Keep CLI behavior and generated files stable; do not reintroduce the removed upstream `Node` trait locally.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p emerald node::tests
cargo test -p malachitebft-eth-cli
cargo check --workspace --all-targets --all-features
```

Expected: validator and full-node paths compile and pass, startup failures propagate, and shutdown owns all tasks.

```bash
git add app/src/node.rs app/src/main.rs app/Cargo.toml cli/src
git commit -m "refactor: build Emerald with Malachite contexts"
```

## Task 6: Preserve Authenticated Forwarding and Persisted-State Recovery

**Files:**
- Modify: `app/src/{app.rs,state.rs,streaming.rs,proposal_forwarding_tests.rs}`
- Modify: `app/tests/phase1_restart.rs`
- Modify: `types/src/codec/proto/mod.rs`
- Modify: `docs/operational-docs/src/{architecture/consensus.md,architecture/syncing.md}`

**Interfaces:**
- Consumes: application-level `ProposalFin` signatures, source `PeerId`, v0.7 `WalEntry::PolkaCertificate`, and Task 1
  fixtures.
- Produces: authenticated restream/forwarding plus old-read/new-write restart coverage and explicit downgrade errors.

- [ ] **Step 1: Port proposal-forwarding tests before implementation changes**

Retain local proposal, honest forwarder, altered content, wrong publisher, malformed part, duplicate delivery,
hidden-lock, mixed delivery, and restream tests. Assertions must distinguish delivering peer resource accounting from
the proposal identity authenticated by `ProposalFin`.

- [ ] **Step 2: Adapt forwarding without trait-level part signing**

Continue signing the complete proposal identity into `ProposalFin` with `K256Signer::sign`. Verify it with
`K256Verifier::verify` only after assembly and before accepting or persisting the value. Never replace this check with
transport peer identity alone.

- [ ] **Step 3: Prove phase-1 state restart**

Open copies of Task 1 WAL and redb fixtures, assert no truncation or hash change, restore the exact historical vote,
store tip, history minimum, certificate, and proposal metadata, then start at the next height with explicit
`HeightParams`.

- [ ] **Step 4: Prove new polka WAL and downgrade behavior**

Write a polka certificate, assert the entry tag is `0x08`, restart the candidate, and assert the certificate affects
replay as expected. Feed the same entry to a frozen phase-1 tag decoder accepting only `0x01`, `0x02`, and `0x04` and
assert an unknown-tag error with no truncation, skip, or deletion.

- [ ] **Step 5: Preserve failure propagation**

Add/retain tests showing application, Engine API, actor, codec, WAL, and redb failures return errors or fail closed.
Peer scoring must not change for these local failures.

- [ ] **Step 6: Verify and commit**

```bash
cargo test -p emerald proposal_forwarding_tests
cargo test -p emerald --test phase1_restart
cargo test -p emerald store
cargo test -p malachitebft-eth-types codec
```

Expected: authentication, old-state restart, new `0x08` restart, and explicit downgrade rejection all pass.

```bash
git add app types/src/codec/proto/mod.rs docs/operational-docs/src/architecture
git commit -m "fix: preserve Emerald recovery on Malachite v0.7"
```

## Task 7: Validate, Document, and Record the Merge Gate

**Files:**
- Modify: `docs/operational-docs/src/{config-examples.md,architecture/consensus.md,architecture/syncing.md}`
- Modify: `docs/research/2026-11-02-malachite-staged-upstream-migration.md`
- Modify: pull-request description after opening the PR

**Interfaces:**
- Consumes: exact candidate/base SHAs, Tasks 1–6 results, and parallel phase-1 qualification evidence.
- Produces: a phase-2 PR that may be reviewed but cannot merge until its external gate is satisfied or overridden.

- [ ] **Step 1: Update operator documentation**

Document explicit per-height timeouts, ignored `wal_replay_delay`, immediate replay, validator/full-node roles,
proof-based classification, old-state reads, and the `0x08` downgrade recovery procedure. State that deleting WAL is
not recovery and that phase-2 development is not production qualification.

- [ ] **Step 2: Run focused and workspace checks**

```bash
cargo test -p malachitebft-eth-types
cargo test -p key-provider
cargo test -p emerald
cargo check --locked --workspace --all-targets --all-features
cargo check --locked -p key-provider --no-default-features --all-targets
cargo check --locked -p key-provider --no-default-features --features gcp-sm-kms --all-targets
cargo check --locked -p key-provider --no-default-features --features aws-sm-kms --all-targets
cargo clippy --tests -- -D warnings
cargo +nightly fmt --all --check
taplo fmt --check
```

Expected: all commands pass. Do not claim custom-Reth, Forge, MBT, or external Engine API coverage from these commands.

- [ ] **Step 3: Run integration checks when dependencies are available**

```bash
forge fmt --check
forge build --sizes
forge test
make docs
make mbt-test
```

Expected: each available check passes. Report unavailable Reth, Quint, Docker, cloud, or network dependencies exactly.

- [ ] **Step 4: Audit the branch and lockfile**

```bash
git diff --check origin/main...HEAD
git diff --stat origin/main...HEAD
git diff origin/main...HEAD -- Cargo.toml Cargo.lock
git log --oneline --decorate origin/main..HEAD
```

Confirm a single Malachite git revision, only `arc-malachitebft-*` package mappings, unchanged phase-1 fixtures, and no
unrelated storage, Reth, Solidity, or deployment changes.

- [ ] **Step 5: Record the parallel phase-1 merge gate**

Link an evidence record naming the tested Emerald revision, exact phase-1 Malachite revision, environment, scenarios,
results, and limitations. Record explicit human approval before merging phase 2. If absent, mark the PR blocked from
merge; any override must name its approver, rationale, and accepted risk.

- [ ] **Step 6: Commit documentation**

```bash
git add docs
git commit -m "docs: describe Malachite v0.7 compatibility"
```

After the implementation PR exists, follow Emerald's mandatory changelog-decision workflow from `AGENTS.md`; do not
add a fragment before showing its exact path/content and receiving human approval.
