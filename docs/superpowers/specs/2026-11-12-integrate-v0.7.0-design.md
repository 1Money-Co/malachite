# Integrate Circle Malachite v0.7.0 into the 1Money fork

**Date:** 2026-11-12
**Status:** Draft for approval
**Tracking issue:** [1Money-Co/om-emerald#48](https://github.com/1Money-Co/om-emerald/issues/48)

## Intent

Advance the maintained 1Money Malachite fork from the reconciled Circle v0.6.0-rc.3 baseline to a v0.7.0-based
fork, then integrate that candidate into Emerald without dropping behavior required by existing networks. The phase
must preserve consensus identities, recovery state, authenticated proposal forwarding, validator/full-node roles,
and downstream sync invariants while adopting v0.7.0's builder, context, signing, validator-proof, and WAL surfaces.

This is a development and integration milestone. It does not authorize a production rollout, declare rolling
compatibility, or retire the fork. Phase-1 Emerald qualification will run in parallel with this work. The phase-2
pull requests may be opened before that qualification finishes, but they must not merge until the phase-1 evidence
passes or an explicitly approved disposition replaces that gate.

## Exact starting points

| Role | Repository and commit |
| --- | --- |
| 1Money phase-1 fork | `1Money-Co/malachite@6f544d7efca221ee8e7d2da9af37795ed6745f99` |
| Circle v0.7.0 implementation parent | `circlefin/malachite@5cd137fb158fc52b55044eee9b090fd0d34996b0` |
| Circle v0.7.0 release commit | `circlefin/malachite@32abf3cf952bb5fe03de914435e902a782632b59` |
| Circle v0.8.0 sibling release | `circlefin/malachite@72143f6c99a98452b587e1c392bdb80944eb2232` |
| Current Emerald main | `1Money-Co/om-emerald@d681a11` |
| Current Emerald Malachite pin | `1Money-Co/malachite@eb2405ba78fc2d5c4465652364299f3217428a36` |

The v0.7.0 and v0.8.0 release commits are siblings over `5cd137fb`. The v0.7.0 release commit contains the named
release metadata and package-version transition; the implementation reconciliation is therefore separated from the
release delta. Record full SHAs again at implementation start and before opening each pull request.

## Goals

- Preserve auditable Circle and 1Money ancestry while integrating the exact v0.7.0 implementation and release.
- Keep the phase-1 downstream inventory authoritative until replacement evidence satisfies each retirement gate.
- Adopt the `arc-malachitebft-*` package names while retaining stable local dependency aliases where useful.
- Replace the removed application `Node` trait with builder/context integration without changing lifecycle behavior.
- Split signing and verification, and implement validator-proof signing, verification, and codecs without re-keying.
- Implement `Context::Timeouts`, `HeightParams`, and changed height and validator-set callbacks explicitly.
- Adapt to upstream sync queue, validation, and scoring behavior while preserving downstream recovery invariants.
- Preserve authenticated Emerald proposal forwarding despite removal of proposal-part signing trait methods.
- Add polka-certificate WAL tag `0x08`, old-state reads, new-state restart coverage, and a downgrade boundary.
- Remove delayed WAL replay entirely and retain immediate, deterministic replay with focused safety regressions.
- Repair WAL compatibility tests so raw canonical bytes and forced-compression semantics are both tested correctly.
- Produce linked Malachite and Emerald pull requests with exact evidence, limitations, and phase-3 handoff.

## Non-goals

- Production deployment, rolling rehearsal, instance reconfiguration, release publication, or fleet observation.
- Declaring the v0.7.0-based fork production-qualified merely because development tests pass.
- Retiring the 1Money fork or switching Emerald to an unmodified Circle dependency.
- Adopting v0.8.0 proposal-mode, certificate, failure-supervision, or sync-outcome changes except for an explicitly
  scoped safety backport required by this phase.
- Enabling vote extensions, changing consensus key material, changing address derivation, or changing signed bytes.
- Introducing a bulk WAL or database rewrite when compatible reads and bounded adapters are sufficient.
- Activating another Emerald storage schema or combining unrelated dependency, CI, or repository cleanup.
- Resolving unrelated interoperability issues or changing Reth execution history, genesis, or chain identity.

## Anti-goals

- Treating a clean merge, successful compilation, or tag name as compatibility evidence.
- Rebuilding the fork from a clean v0.7.0 base and relying on memory to restore required downstream behavior.
- Deleting or resetting a WAL, re-signing altered historical messages, or restoring stale signing state to recover.
- Keeping delayed WAL replay available behind a nonzero configuration value after declaring it mitigated.
- Weakening raw WAL byte-compatibility assertions merely to make an all-features test invocation pass.
- Penalizing peers for local application, execution, actor, codec, or storage failures.
- Turning a safety-critical failure into successful exit, silent progress, or an automatic unsafe restart loop.
- Claiming mixed-version readiness without admission, consensus, sync, restart, and recovery evidence for the pair.

## Constraints

- Malachite remains generic; Emerald-specific types, codecs, storage fixtures, and Engine API failures stay in
  `om-emerald`.
- The phase starts from the merged phase-1 fork commit `6f544d7`, not the older production or development pin.
- Merge Circle implementation parent `5cd137fb` first, then merge v0.7.0 release commit `32abf3cf` separately.
- Preserve the phase-1 signing, WAL, redb/history, sync, and application-request fixtures and extend them in place.
- Preserve old WAL tags `0x01`, `0x02`, and `0x04`; new tag `0x08` creates a downgrade-after-write boundary.
- Existing raw fixtures are immutable provenance artifacts and must not be regenerated from the candidate.
- Lockfile changes required by package and dependency changes are allowed, reviewed explicitly, and never formatted.
- Documentation follows the workspace 120-column Markdown limit.
- Before pushing, run `cargo clippy --tests -- -D warnings` and `cargo +nightly fmt --all --check`.
- Emerald work additionally runs `taplo fmt --check` and follows its changelog approval workflow after PR opening.

## Approaches considered

### 1. Two-stage upstream merge — selected

From the phase-1 fork, merge Circle implementation parent `5cd137fb` and reconcile implementation conflicts by
behavior. After that merge is stable, merge its v0.7.0 child `32abf3cf` separately for the release and package-name
delta. Preserve both merge ancestries.

This separates the architectural reconciliation from the release metadata, keeps exact upstream provenance, and
makes the final tree easy to compare with both the common implementation parent and the named v0.7.0 release. It
also keeps omission from becoming the default failure mode.

### 2. Direct merge of the v0.7.0 release commit — rejected

Merge `32abf3cf` into the phase-1 fork in one step. This preserves tag ancestry with one merge, but combines the
large implementation reconciliation with package aliases, versions, release notes, and lockfile changes. The mixed
conflict surface makes behavior review and regression attribution harder without reducing the actual work.

### 3. Fresh v0.7.0 base with selected downstream ports — rejected

Start from Circle v0.7.0 and port only the patches believed to remain necessary. This yields a superficially cleaner
tree but makes omission the main risk, weakens the phase-1 patch inventory, and conflicts with the requirement to
retire downstream behavior only after replacement evidence exists.

## Design

### 1. Revalidate provenance and expand the downstream inventory first

Before merging runtime code, extend `DOWNSTREAM_CHANGES.md` for the v0.7.0 checkpoint. For every phase-1 behavior,
record whether v0.7.0 contains an exact replacement, a partial replacement, or no replacement. Record the upstream
commit, the fork implementation, the chosen resolution, focused regression evidence, and the next retirement gate.

Add new v0.7.0 surfaces to the same inventory:

- builder/context lifecycle and application-channel wiring;
- package aliases and crate provenance;
- signer/verifier separation and validator proofs;
- timeouts, height parameters, validator-set callbacks, and proposer transitions;
- sync queue, validation, scoring, admission, and persistent-peer behavior;
- proposal-part authentication after trait removal;
- polka-certificate WAL encoding and downgrade behavior;
- delayed-WAL-replay removal; and
- default versus forced-compression WAL test behavior.

Circle's upstream ports of downstream commits are evidence inputs, not automatic retirement decisions. A source-level
match still requires the fork and Emerald regressions that represent the behavior consumed in production.

### 2. Merge the implementation parent, then the release commit

Merge `5cd137fb` into `seb/integrate-v0.7.0` with a dedicated merge commit. Resolve conflicts in reviewable groups:

1. workspace topology, crate manifests, and dependencies;
2. core context, timeout, height, validator-set, and signing traits;
3. application builder, channel, node, and process lifecycle;
4. network admission, validator proofs, persistent peers, and reconnection;
5. consensus, WAL, proposal forwarding, and authenticated parts;
6. sync queue, response validation, scoring, capacity, and metrics;
7. test framework, fixtures, examples, documentation, and CI; and
8. lockfile, reviewed only after manifest decisions are stable.

For every runtime conflict, state the competing invariants in the inventory before selecting code. Prefer the
upstream implementation when it satisfies all required properties; retain or adapt fork behavior when evidence is
missing. Do not resolve broad conflicts by choosing one side wholesale.

Once implementation tests are coherent, merge `32abf3cf`. This second merge should be limited to the v0.7.0 release
delta. Verify its diff against `5cd137fb..32abf3cf`, including `arc-malachitebft-*` package names, version metadata,
release notes, and lockfile changes. Do not import v0.8.0 sibling release metadata.

### 3. Replace the application Node trait with builder/context integration

Adopt the v0.7.0 builder and context construction model while preserving the observable lifecycle currently used by
Emerald:

- resolve consensus keys once and share the same resolved identity across application, signing, and network setup;
- construct validators and full nodes with their existing roles and consensus-enabled behavior;
- install application, consensus, sync, network, WAL, metrics, and event-channel dependencies before start;
- preserve event subscription availability and ownership;
- propagate task or actor startup failures instead of returning a partially live node;
- preserve coordinated shutdown, child-task termination, and top-level process exit semantics; and
- keep restart behavior explicit rather than relying on dropped handles or detached tasks.

Malachite tests cover generic builder ownership, role construction, startup failure, and shutdown. Emerald tests cover
its concrete key providers, Engine API application, event consumers, CLI/process lifecycle, and validator/full-node
configuration.

### 4. Split signing and verification without changing identities

Adapt the retained ECDSA implementation to v0.7.0's separate `Signer` and `Verifier` traits. Preserve every supported
curve and the existing private-key, public-key, address, signature, JSON, and protobuf contracts. Keep known-answer
fixtures anchored to phase-1 and production-compatible bytes.

Implement Circle v0.7.0's canonical validator-proof preimage exactly: `PoV`, then the public-key length as a
big-endian `u32`, the public-key bytes, the peer-ID length as a big-endian `u32`, and the peer-ID bytes. The same
configured consensus key identifies the validator, but proof, vote, and proposal purposes must not share an ambiguous
signing payload.

Preserve the v0.7.0 protocol contract rather than inventing a network ID, nonce, timestamp, or challenge. A proof is a
static binding to the sender's peer ID. A valid proof for a key outside the current validator set is stored with the
peer classified as a non-validator, then re-evaluated on validator-set changes. Peer-ID mismatch, invalid signature,
duplicate proof in one connected session, and proof-stream framing or timeout failure disconnect the peer. Match the
v0.7.0 implementation for a proof that cannot be decoded: consume the peer's one proof opportunity, keep it
unclassified, and do not promote or disconnect it solely for that decode failure. A full disconnect clears the
per-peer proof state, so a later connection may submit one proof again.

Emerald proves local and cloud-backed key loading, k256 address derivation, fixed proposal/vote bytes, validator-proof
bytes, and unchanged configuration behavior. No key migration or new custody role is introduced.

### 5. Implement timeouts, height parameters, and validator transitions

Implement `Context::Timeouts` using Emerald's existing timeout policy and map v0.7.0 `HeightParams` explicitly. The
height-start callback must supply the validator set, local role, timeouts, and other required per-height parameters
from one coherent application state.

Tests cover genesis, ordinary height advance, validator addition/removal, proposer selection, validator-to-full-node
and full-node-to-validator transitions, restart at an existing height, and malformed or unavailable height state.
Callback, storage, or execution failures remain failures; they cannot silently reuse stale parameters.

### 6. Adopt upstream sync architecture while proving downstream invariants

Use the v0.7.0 sync queue, validation, and scoring design as the target. Retire phase-1 downstream sync code only when
replacement tests demonstrate all retained properties:

- consensus height initializes before status- or tick-triggered requests;
- inflight and consensus-pending heights remain bounded by the configured allowance;
- delayed, stale, partial, unsolicited, wrong-peer, and wrong-range responses preserve validation and retry state;
- response byte limits apply before send and acceptance;
- local application or execution failure is not misclassified as peer-invalid input;
- persistent peers reconnect with discovery enabled and disabled;
- validator and full-node role changes preserve admission and catch-up;
- sync input ordering cannot lose an early response before consensus wiring exists; and
- saturation diagnostics retain actionable counts, bounds, and local progress.

Where the upstream architecture cannot express a required invariant, add the smallest generic compatibility
adaptation and record it as an explicit remaining fork patch. Do not restore the old architecture wholesale.

### 7. Preserve authenticated proposal forwarding

The removal of proposal-part signing trait methods does not remove Emerald's authentication requirement. Keep a
generic application boundary that lets Emerald authenticate the proposal origin and assembled content before it is
accepted or forwarded. Distinguish the declared publisher from the delivering peer where the upstream API does so.

Known-answer tests preserve proposal identity and signed bytes. Integration tests cover direct delivery, forwarding,
restream after hidden lock, mixed delivery paths, malformed parts, wrong publisher, and content whose identity does
not match its authentication. Resource accounting remains attached to the delivering peer even when assembly uses
the declared publisher.

### 8. Remove delayed WAL replay entirely

Do not retain v0.7.0's five-second `wal_replay_delay` path or its `WaitingForSync` bypass. Backport the v0.8.1 safety
disposition at the narrow v0.7.0 boundary: a validator with WAL entries replays them immediately before ordinary sync
recovery can affect that height.

Remove the timer, pending-entry state, phase transitions, and configuration behavior that can re-enable the delay.
An old `wal_replay_delay` configuration key may remain parse-compatible only if it has no runtime effect and the
operator documentation says so explicitly. Prefer matching the later upstream behavior for ignored legacy keys.

Focused tests prove:

- non-empty WAL recovery always begins replay immediately;
- a sync certificate cannot cause the recorded WAL to be skipped;
- replay cannot regress the height or round;
- recorded signed messages are preserved rather than regenerated with changed bytes;
- corrupt or inapplicable entries fail closed; and
- empty-WAL and non-validator startup retain their intended behavior.

This removes the known delayed-path risk but does not import v0.8.1's broader replay-index or divergence machinery.
Any newly discovered need for that machinery is a scope increase requiring a design update.

### 9. Extend WAL codecs and repair feature-matrix coverage

Retain decoding for old tags `0x01`, `0x02`, and `0x04`, and add the v0.7.0 polka-certificate tag `0x08`. Test actual
entry contents and replay effects, not only outer framing. New records written with `0x08` are unsupported by the old
engine. The phase-2 downgrade boundary is the old decoder's explicit failure plus operator recovery documentation;
it does not add a WAL marker, preflight command, or new CLI. Silent record loss or automatic WAL deletion is forbidden.

Repair the phase-1 compatibility test exposed by workspace-wide `--all-features`:

- keep the checked-in raw WAL fixtures and their SHA-256 manifests immutable;
- always verify their source commit, length, checksum, decoded entries, and historical-vote restoration;
- when a rewrite uses raw framing, require byte-for-byte equality with the source fixture;
- when `force-compression` is active, require compressed framing, successful decoding, semantic entry equality, and
  stable restart/replay behavior without comparing compressed bytes to the raw fixture checksum; and
- add a focused feature-matrix command or CI job that exercises both raw and forced-compression modes directly.

The assertion must distinguish framing mode from codec compatibility. It must not accept an arbitrary raw checksum
mismatch merely because the entries can still be decoded.

### 10. Integrate the candidate into Emerald

After the Malachite candidate commit is stable, create a separate Emerald branch and update dependency package names,
aliases, exact fork revision, and lockfile. Adapt `app/`, `types/`, `cli/`, and configuration surfaces to the builder,
context, signing, validator-proof, sync, and WAL APIs.

Emerald fixtures cover:

- fixed local and cloud-compatible key material, addresses, and signed bytes;
- validator proof codecs and admission behavior;
- old application WAL plus redb/history restart;
- new polka-certificate WAL restart and explicit unsupported downgrade;
- validator/full-node roles, transitions, persistent peers, and reconnects;
- normal consensus, proposal forwarding, hidden-lock/restream, and catch-up;
- application, execution-layer, actor, codec, and storage failure propagation; and
- startup and shutdown through the actual CLI/process path.

Keep the Malachite and Emerald pull requests linked. If Emerald integration requires a generic engine adaptation,
make it in Malachite first, pin the new candidate commit, and rerun affected evidence. Do not hide patches in Cargo
configuration.

### 11. Validation and merge gates

Use focused tests during reconciliation, then expand to repository-required suites. Networked Malachite tests must
run outside restricted sandboxes that block loopback listeners; report the environment rather than treating those
timeouts as product failures.

Before opening the Malachite pull request, record at least:

- package and feature checks for every changed crate;
- signing and validator-proof known-answer tests;
- raw and forced-compression WAL compatibility and replay tests;
- polka-certificate codec, restart, corruption, and downgrade tests;
- immediate-replay safety regressions;
- sync lifecycle, persistent-peer, reconnect, role-transition, and full integration tests;
- normal consensus, hidden-lock/restream, validator transition, and catch-up scenarios;
- application/channel failure and process-lifecycle tests;
- `cargo clippy --tests -- -D warnings`; and
- `cargo +nightly fmt --all --check`.

Before opening the Emerald pull request, run its focused Rust, storage, configuration, and lifecycle tests, then its
required clippy, nightly formatting, and Taplo checks. Run model-based or system tests when the required Reth and
Quint environment is available, and report any unavailable coverage exactly.

The phase-2 PRs must not merge until the parallel phase-1 Emerald qualification passes or the human explicitly
approves a different disposition. The authoritative gate is a linked evidence record that names the tested Emerald
revision, exact phase-1 Malachite revision, environment, commands or scenarios, results, and unresolved limitations,
followed by explicit human approval to merge phase 2. If that record or approval is absent, phase 2 remains blocked
from merge. An override must identify its approver, rationale, and accepted risk in the phase-2 pull requests.

Passing phase-1 qualification is a merge gate, not evidence that phase 2 is safe to deploy. Phase-2 merge also
requires no unresolved safety-critical development failure and linked evidence for every retained or retired
downstream behavior.

## Data, downgrade, and rollout position

No bulk database or WAL rewrite is planned. Existing raw records remain readable, and new records use the v0.7.0
codec. Once a node writes a polka-certificate record with tag `0x08`, the old engine cannot read that WAL. The old
decoder failure and documented recovery procedure are the phase-2 downgrade signal; no new marker or CLI is planned.
Operators must restore the new binary or a separately proven forward-compatible successor. Deleting the WAL or
blindly downgrading is not a recovery procedure.

Development completion does not establish rolling readiness. Production qualification must exercise the actual old
and new Emerald/Malachite pair with realistic voting power, persistent peers, restart state, proposal sizes, storage,
execution failures, and quorum margin. The v0.7.0-based fork may remain a development-only milestone.

## Deliverables

1. A Malachite branch and pull request preserving the two-stage Circle ancestry and updated patch inventory.
2. A v0.7.0-based engine with delayed WAL replay removed and WAL feature-matrix tests repaired.
3. Generic fixtures and regressions for signing, validator proofs, sync, lifecycle, WAL, and recovery behavior.
4. An Emerald branch and pull request pinned to the reviewed fork commit with application-specific adaptations.
5. Compatibility, configuration, recovery, downgrade, validation, and limitation documentation.
6. Exact check results, actual effort, unresolved qualification work, and a phase-3 handoff.

## Left to the implementation plan

The implementation plan may choose the exact commit boundaries within each conflict group, focused test commands,
and order of independent fixture work. It must preserve the two merge checkpoints, keep runtime conflict resolutions
reviewable, and avoid parallel edits to the same high-conflict files. It must identify every Malachite candidate SHA
that Emerald pins during development and finish with one reviewed candidate SHA for the linked PR.
