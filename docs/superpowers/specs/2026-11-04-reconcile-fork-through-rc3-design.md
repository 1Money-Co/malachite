# Reconcile the 1Money Malachite fork through Circle v0.6.0-rc.3

**Date:** 2026-11-04
**Status:** Draft for approval
**Tracking issue:** [1Money-Co/om-emerald#47](https://github.com/1Money-Co/om-emerald/issues/47)

## Intent

Reconcile the maintained 1Money Malachite fork with Circle's `v0.6.0-rc.1`, `v0.6.0-rc.2`, and
`v0.6.0-rc.3` history without dropping behavior required by Emerald. The result remains a 1Money fork and becomes the
candidate engine for the first phase of the staged upstream migration. This phase does not switch Emerald to an
untouched Circle revision and does not qualify a production rollout.

Development starts from the current fork rather than the production engine commit. Production compatibility starts
from the engine commit used by the currently deployed Emerald revision:

| Role | Repository and commit |
| --- | --- |
| Production Emerald source | `1Money-Co/om-emerald@e2cbc986d26fa2fb593004cb677c79cca40dec75` |
| Production Malachite source | `informalsystems/malachite@bcac2b2dbd369a6c29b9151fcd0889b133d7221a` |
| Development base | `1Money-Co/malachite@eb2405ba78fc2d5c4465652364299f3217428a36` |
| Circle rc.1 | `circlefin/malachite@b31e2fff7ac0cfbca8cc225606af925795755e1e` |
| Circle rc.2 | `circlefin/malachite@65955fd69e07ecb2e67b86b5bec63d93800f1c15` |
| Circle rc.3 | `circlefin/malachite@b205f4252f3064d9a74716056f63834ff33f2de9` |

The production commit is an ancestor of the development base. The changes between them are not yet deployed to
production, so the direct production-to-candidate qualification must treat those changes as new rollout behavior,
not as behavior already proven in production.

## Goals

- Integrate all relevant Circle behavior through rc.3 into the current 1Money fork with exact provenance.
- Preserve the ECDSA crate and the key, public-key, signature, and serialization behavior consumed by Emerald.
- Preserve response-size limits, value-sync request accounting, bounded capacity, stale-response handling, and the
  restored-height startup repair tracked from interoperability issue #375.
- Preserve or improve consensus-request failure reporting and rc.3 operator diagnostics.
- Prove that the candidate can consume compatible signing and WAL material originating at the production
  `bcac2b2d` baseline.
- Record every remaining downstream behavior, its provenance, its tests, and the condition under which it may be
  retired in a later migration phase.
- Produce a reviewed Malachite commit that Emerald can pin in the follow-up integration work.

## Non-goals

- Switching Emerald to the candidate commit in this Malachite pull request.
- Changing Emerald's application codecs, redb schema, proposal representation, or storage-schema activation state.
- Migrating to Malachite v0.7.0 or later, renaming packages to `arc-malachitebft-*`, or retiring the fork.
- Performing a production deployment, rolling rehearsal, fleet inspection, or release qualification.
- Reconstructing signing history that was lost before this work or repairing a damaged WAL automatically.
- Removing fork-owned QA or operational tooling merely because Circle removed it before rc.1.
- Combining this work with unrelated dependency, formatting, workflow, or repository cleanup.

## Anti-goals

- Treating an rc tag, a successful build, or a clean merge as evidence of behavior preservation.
- Resetting or deleting a WAL to make a restart test pass.
- Re-signing historical messages or changing signed bytes while preserving an old signature.
- Silently expanding request capacity, response bounds, or accepted stale ranges.
- Hiding remaining downstream patches behind a version label that suggests the fork is identical to Circle rc.3.
- Claiming rolling readiness from development tests alone.

## Constraints

- Malachite must remain generic. Emerald-specific codecs and redb fixtures belong in the Emerald follow-up.
- The production source is `bcac2b2d`, even though implementation starts from the newer `eb2405ba` fork head.
- The existing outer WAL entry tags and current application codec contracts must remain readable.
- The candidate must retain the current package names and ECDSA dependency surface expected by Emerald.
- Runtime conflict resolutions require focused regression tests; source resemblance is insufficient.
- Lockfile changes caused by the reconciliation are allowed but must be reviewed explicitly and never formatted.
- Documentation added by this work follows the workspace 120-column Markdown limit.

## Approaches considered

### 1. Current fork plus an explicit rc.3 merge — selected

Branch from `eb2405ba`, merge the exact rc.3 commit, resolve conflicts by behavior, and add follow-up commits for
fixtures, patch inventory, and any required adaptations. Use `bcac2b2d` as the compatibility-fixture origin and the
direct production-upgrade source.

This preserves both histories, makes the upstream provenance auditable, retains the already reviewed downstream
repairs, and avoids replaying work that exists between the production and development baselines. The merge currently
has content conflicts in the lockfile, consensus engine, sync actor, test parameters, and value-sync integration
tests. Those are the areas where behavior-based reconciliation is most important.

### 2. Fresh rc.3 base with selected downstream ports — rejected

Start from rc.3 and port the ECDSA implementation, value-sync series, response bounds, diagnostics, and restored-height
repairs. This produces a superficially cleaner tree but makes omission the default failure mode. The downstream sync
series is large and includes behavior that is not represented by one final patch or one upstream-equivalent commit.

### 3. Production commit followed by a full replay — rejected

Start at `bcac2b2d`, replay the later 1Money fork work, and then replay the rc changes. This mirrors the deployed
ancestry but duplicates already completed integration and review work. It also makes it harder to distinguish the
compatibility baseline from the development baseline. Production history is better represented by immutable fixtures
and an explicit upgrade matrix.

## Design

### 1. Establish the provenance and behavior inventory first

Before resolving runtime conflicts, expand `DOWNSTREAM_CHANGES.md` into the authoritative inventory for this phase.
Each behavior entry records:

- its originating commit or issue;
- whether it exists in production `bcac2b2d`, only in the current fork, or in Circle rc.3;
- the current fork implementation and any Circle equivalent or partial equivalent;
- the decision to retain, adapt, accept upstream, or retire;
- focused regression evidence; and
- an exact retirement criterion for a later phase.

The inventory must distinguish patch identity from behavior identity. The current ancestry comparison shows 70
fork-side and 17 Circle-side commits after the common ancestor. Patch-ID comparison already identifies several Circle
changes that the fork carries exactly or in adapted form, including the rc.2 WAL-height fix. The inventory, not a
commit count, determines completeness.

At minimum, the inventory covers:

- ECDSA signing and serialization;
- response-size enforcement;
- inflight and pending value-sync accounting;
- partial, delayed, stale, and unsolicited responses;
- restored-height startup and saturated-capacity diagnostics;
- consensus-request failure reporting;
- `StartHeight` WAL append height correctness;
- rc.3 logging levels and structured context; and
- any current fork behavior affected by automatically merged rc changes.

### 2. Merge the exact rc.3 commit into the current fork

Create the implementation branch from `eb2405ba` and merge
`b205f4252f3064d9a74716056f63834ff33f2de9`. Preserve the merge ancestry; do not squash the upstream history into an
unattributed patch.

Resolve conflicts in this order:

1. workspace manifests and lockfile;
2. core consensus and WAL effects;
3. consensus engine error propagation and logging;
4. sync actor state and response processing;
5. test framework parameters and value-sync scenarios; and
6. documentation and release notes.

For each runtime conflict, write down the competing invariants before selecting code. The resolution must either use
the upstream implementation with downstream regressions, retain the downstream implementation with an explanation of
the upstream behavior it satisfies, or introduce a narrow adaptation covered by both sets of tests.

The merge may apply non-conflicting upstream repository deletions automatically. Preserve the fork's existing QA and
operational tooling unless it is proven unused and removed in a separately reviewed change; repository cleanup is not
part of this migration.

### 3. Reconcile the three rc checkpoints by behavior

The rc.1 checkpoint supplies the bulk of the comparison. Equivalent changes remain credited to their upstream
provenance, while the fork-specific implementation remains when it carries additional required behavior. Missing
upstream behavior, such as relevant asynchronous application replies or restart handling, is integrated with focused
tests rather than accepted only because the merge applied cleanly.

For rc.2, verify that WAL append effects use the height carried by each entry when handling `StartHeight`. The fork's
existing adaptation is retained if it satisfies the rc.2 regression and does not weaken error reporting. The test
must exercise a height transition where the actor's current height differs from the appended entry height.

For rc.3, reconcile logging levels and structured context across consensus and sync paths. Preserve the fork's useful
capacity diagnostics, including inflight and pending height counts, their lowest starts, and the local tip. Logging
changes must not alter control flow or turn protocol failures into successful outcomes.

### 4. Preserve ECDSA compatibility

Keep `informalsystems-malachitebft-signing-ecdsa` in the workspace and preserve the API imported by Emerald. Add
known-answer fixtures using fixed private keys and fixed message bytes for every curve retained by the crate. The
fixtures cover:

- private-key byte decoding and round-trip serialization;
- public-key derivation and serialized bytes;
- deterministic signature bytes where the implementation promises determinism; and
- successful verification plus rejection of altered messages and signatures.

The Malachite fixture proves the generic signing crate. Emerald's follow-up must separately prove its k256 address
derivation, proposal and vote signed bytes, configuration/key-file decoding, and application protobuf codecs using
the same production-compatible key material.

### 5. Preserve WAL and restart compatibility

Add immutable WAL-entry fixtures produced by code at `bcac2b2d` and, where behavior differs, `eb2405ba`. Check the
fixture bytes into the test tree with a small provenance file containing the source commit, generator command, entry
types, height, and expected decoded values.

The candidate must:

- decode every production-baseline fixture without data loss;
- preserve the expected height, round, vote, proposal, timeout, and signed-message contents;
- re-encode to the expected bytes when the codec contract is canonical;
- replay a valid fixture and restore its exact signed historical vote into the vote keeper; and
- fail explicitly on corrupt or unsupported input.

Retain the existing restart and crash-recovery integration tests and add the rc.2 height regression. Never make a
test pass by resetting the WAL. This phase does not claim compatibility for Emerald application payloads solely from
generic test-context fixtures; the Emerald follow-up supplies actual Emerald-encoded WAL and redb/history fixtures.

### 6. Preserve value-sync invariants

Keep the current fork's separate inflight and pending-consensus accounting model unless an alternative demonstrates
all existing properties. Regressions must cover:

- no request before consensus initializes a restored high tip;
- early peer statuses before local height initialization;
- delayed replies validated against the original request;
- stale decided prefixes discarded or trimmed before reserving pending capacity;
- partial responses and re-request selection;
- unknown or unsolicited replies without misclassifying honest late replies;
- exact `batch_size * parallel_requests` height allowance;
- exhausted-capacity behavior and diagnostic fields;
- response-size bounds before sending or accepting values; and
- normal catch-up and consensus progress after the exceptional cases.

Reuse and extend `code/crates/test/tests/unit/sync_lifecycle.rs` and
`code/crates/test/tests/it/value_sync.rs`. Avoid duplicating scenarios between unit and integration layers unless the
integration case proves actor, network, or consensus wiring that the unit harness cannot represent.

### 7. Treat production-to-candidate as the compatibility path

The development candidate includes behavior added after the production engine commit. Qualification therefore uses
this matrix:

| Source | Candidate consumer | Required evidence in this phase |
| --- | --- | --- |
| `bcac2b2d` signing/key material | reconciled fork | Known-answer decode, derivation, sign, and verify fixtures |
| `bcac2b2d` generic WAL entries | reconciled fork | Decode, replay, and corruption handling |
| `eb2405ba` downstream fixtures | reconciled fork | No regression in newer fork behavior |
| reconciled fork fresh state | reconciled fork | Normal consensus, sync, restart, and crash recovery |

No intermediate rc.1 or rc.2 production deployment is required. Their commits are engineering checkpoints and
provenance anchors. Mixed-version network rehearsal, actual Emerald state restart, and production rolling readiness
remain follow-up qualification work.

### 8. Hand off to Emerald explicitly

The Malachite pull request records its exact delivered commit and the unresolved downstream inventory. The Emerald
follow-up then:

- pins that reviewed fork commit;
- runs actual Emerald key/address/signature and application-codec fixtures;
- opens production-era WAL and redb/history snapshots from `e2cbc986`;
- exercises Emerald restart, normal consensus, value sync, and proposal recovery;
- checks the current deployed schema marker instead of assuming schema-v2 activation; and
- records that development success does not authorize production promotion.

Any Emerald integration failure that requires generic engine behavior returns to the Malachite branch as a focused
change. Emerald-specific representation or storage work remains in Emerald.

## Error handling and safety

- Consensus-request failures retain their causal error and remain failures at the caller boundary.
- WAL append, flush, fetch, and replay failures stop the affected path and preserve diagnostics.
- Invalid peer data remains distinguishable from local storage, codec, actor, or application failure.
- Unsupported or corrupt fixture data fails explicitly; tests do not silently regenerate golden bytes.
- A safety halt is not repaired by deleting state, downgrading blindly, or restoring stale signing history.

## Validation

Run focused checks while resolving each concern, then the complete repository checks. At minimum:

- ECDSA unit and known-answer fixture tests;
- core consensus and engine tests for request errors, WAL heights, and logging-sensitive paths;
- sync lifecycle unit tests;
- focused value-sync, WAL replay, restart, and crash-recovery integration tests;
- the repository's `cargo all-tests` and `cargo lint` commands from `code/`, subject to documented environment
  limitations;
- `cargo clippy --tests -- -D warnings`; and
- `cargo +nightly fmt --all --check`.

Review the complete lockfile diff without formatting it. Record tests that are unavailable or flaky as limitations;
do not report them as passing. GitHub checks are reported separately from local checks.

## Documentation and review evidence

The Malachite pull request includes:

- the exact six source pins listed above;
- the completed downstream behavior inventory and retirement criteria;
- fixture provenance and regeneration instructions;
- a checkpoint-by-checkpoint rc.1, rc.2, and rc.3 reconciliation summary;
- exact local and GitHub validation results;
- the delivered fork commit for Emerald to pin;
- actual implementation effort; and
- outstanding Emerald integration and operational qualification work.

## Completion criteria

The fork portion is complete when:

- the exact rc.3 commit is present in the branch ancestry;
- every runtime conflict has a documented behavior decision and focused regression evidence;
- the required ECDSA surface and known-answer fixtures pass;
- production-baseline and current-fork generic WAL fixtures decode and replay as specified;
- sync bounds, accounting, restored-height, stale-response, partial-response, and diagnostics regressions pass;
- normal consensus, height transitions, restart, and crash-recovery development tests pass;
- `DOWNSTREAM_CHANGES.md` accurately lists all remaining patches and retirement gates;
- the Malachite PR identifies the exact candidate commit and validation limits; and
- the Emerald-specific fixture, pin, integration, and rollout work is handed off without being represented as done.

Issue #47 itself remains open until the Emerald follow-up pins and validates the reviewed fork commit and links both
pull requests.

## Builder decisions

The implementer may choose exact test module names, helper APIs, fixture directory names, and commit boundaries after
the upstream merge, provided the provenance remains auditable and the acceptance evidence above is preserved. The
implementer may also consolidate overlapping unit scenarios when an existing test already proves the required
property. These choices must not change the compatibility baseline, selected merge approach, safety rules, or
cross-repository ownership boundary.
