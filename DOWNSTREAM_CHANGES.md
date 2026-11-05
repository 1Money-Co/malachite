# 1Money downstream changes

## rc.3 reconciliation baseline

The first phase of the staged Emerald migration develops from the current 1Money fork while qualifying compatibility
from the Malachite commit used by the Emerald revision currently deployed to production.

| Role | Repository and exact commit |
| --- | --- |
| Production Emerald | `1Money-Co/om-emerald@e2cbc986d26fa2fb593004cb677c79cca40dec75` |
| Production Malachite | `informalsystems/malachite@bcac2b2dbd369a6c29b9151fcd0889b133d7221a` |
| Development base | `1Money-Co/malachite@eb2405ba78fc2d5c4465652364299f3217428a36` |
| Circle v0.6.0-rc.1 | `circlefin/malachite@b31e2fff7ac0cfbca8cc225606af925795755e1e` |
| Circle v0.6.0-rc.2 | `circlefin/malachite@65955fd69e07ecb2e67b86b5bec63d93800f1c15` |
| Circle v0.6.0-rc.3 | `circlefin/malachite@b205f4252f3064d9a74716056f63834ff33f2de9` |

Development starts at `eb2405ba`; production compatibility starts at `bcac2b2d`. The rc.1 and rc.2 commits are
engineering checkpoints and provenance anchors, not separate production deployments. The target remains a maintained
1Money fork until later phases satisfy every retirement gate.

The reconciliation merge includes the exact rc.3 commit in branch ancestry. Conflict resolution retains the fork's
ECDSA crate and separate inflight/pending value-sync architecture, while accepting compatible rc.3 lifecycle,
reconnection, and logging changes. Repository workflow changes are not accepted implicitly: the fork retains its QA
tooling and its explicit reverts of Circle's managed-action substitutions. This does not retire the fork or qualify
the result for production.

WAL and ECDSA compatibility fixtures in this branch were generated from the production source commit. They were not
captured from running production nodes.

### Behavior inventory

`Partial` means the source contains some of the behavior but not the complete downstream invariant. Tasks refer to
the approved implementation plan. Evidence remains pending until its command passes on the reconciled branch.

| Behavior | Production | Current fork | Circle rc.3 | Decision | Evidence | Gate |
| --- | --- | --- | --- | --- | --- | --- |
| `ecdsa-signing-and-serialization` | Yes | Yes | No | Retain; production bytes pinned | E2 | G1 |
| `sync-response-size-bound` | Yes | Yes | Yes | Retain; exact bound characterized | E4 | G2 |
| `sync-inflight-and-pending-accounting` | Yes | Yes | Partial | Retain; saturation characterized | E4 | G3 |
| `sync-restored-height-startup` | No | Yes | No | Retain; startup characterized | E4 | G4 |
| `sync-delayed-stale-and-partial-responses` | Partial | Yes | Partial | Retain; lifecycle characterized | E4 | G5 |
| `sync-capacity-diagnostics` | No | Yes | No | Retain; snapshot characterized | E4 | G6 |
| `sync-network-subscription-order` | Yes | Yes | No | Retain; consensus wiring precedes subscribe | E7 | G10 |
| `consensus-sync-input-queue` | Yes | Yes | No | Retain; dedicated queue and metrics | E7 | G11 |
| `consensus-request-errors` | Yes | Yes | Yes | Retain; exact variants pinned | E6 | G7 |
| `wal-entry-height` | Yes | Yes | Yes | Retain; both baselines pinned | E3 | G8 |
| `wal-same-height-no-replay` | Yes | Yes | No | Retain; repeated start returns no entries | E3 | G12 |
| `sync-peer-restart-recovery` | No | Yes | Yes | Retain fork free-window design | E4, E7 | G5 |
| `decided-reply-async-forwarding` | No | No | Yes | Adapt; log dropped application reply | E8 | G13 |
| `rc3-logging-context` | Partial | Partial | Yes | Adapt; rc.3 severity plus fork context | E6 | G9 |

### Provenance and implementation equivalence

The following table accounts for every first-parent Circle commit after Circle and the production source diverged at
`66f13c65`, through rc.3. `Equivalent` means `git cherry` found the fork commit patch-equivalent. `Adapted` means the
resulting behavior is retained through a different implementation or a review follow-up. `Rejected` records an
intentional fork decision rather than an omitted merge result.

| Circle commit | Fork commit | Status | Evidence |
| --- | --- | --- | --- |
| [5b6f414][c-1200] (#1200) | — | Rejected | P1 |
| [6f34e75][c-1189] (#1189) | [fa87a95][f-1189] | Equivalent | `git cherry` |
| [10893c2][c-1205] (#1205) | [72f3bba][f-1205] | Rejected after port | P2 |
| [76d25e5][c-1210] (#1210) | [b95f1a3][f-1210] | Rejected after port | P2 |
| [bd6a7c8][c-1191] (#1191) | [07e3050][f-1191] | Adapted | P3 |
| [dd349d6][c-1215] (#1215) | [e4e0109][f-1215] | Equivalent | `git cherry` |
| [2ce022d][c-1174] (#1174) | [c921191][f-1174] | Adapted | P4 |
| [3aab3d9][c-1211] (#1211) | [46a7b11][f-free-window] | Rejected; superseded | P5 |
| [644dd07][c-1221] (#1221) | [a64331c][f-1221] | Equivalent | `git cherry` |
| [0444b9f][c-1222] (#1222) | [98c7517][f-review-fixes] | Adapted after merge | P6 |
| [b31e2ff][c-1198] (#1198) | [3221578][f-1198] | Adapted | P7 |
| [9c2c7b0][c-1226] (#1226) | [758bce1][f-1226] | Equivalent | `git cherry`; E6 |
| [94401a7][c-1216] (#1216) | [4c522bd][f-1216] | Equivalent | `git cherry` |
| [ec45cc5][c-1228] (#1228) | [9b77fc3][f-1228] | Equivalent | `git cherry` |
| [65955fd][c-1233] (#1233) | [a07b0dc][f-1233] | Equivalent | `git cherry`; E3 |
| [0a87a5b][c-1234] (#1234) | [b3e778d][f-1234] | Equivalent | `git cherry` |
| [b205f42][c-1230] (#1230) | [4de3493][f-1230] | Adapted | P8; E4, E6 |

- **P1 — #1200:** Circle removed the QA workflow and supporting tree. The design explicitly excludes that deletion,
  so `.github/workflows/qa.yml` and `qa/` remain fork-owned.
- **P2 — #1205 and #1210:** the fork first received patch-equivalent ports, then explicitly reverted them in
  [8a15078][f-revert-1205] and [fb49559][f-revert-1210]. The reconciliation retains those reverts, including for the
  publish workflow that receives `CRATES_TOKEN`; changing the supply-chain policy is outside this migration.
- **P3 — #1191:** the fork carries the fallible signing-provider API in `07e3050` and adds the ECDSA implementation in
  `bcac2b2`. E2 pins the retained ECDSA bytes and error-capable signing surface.
- **P4 — #1174:** the sync-only mode is retained in the adapted fork commit, including fork-specific host and test
  integration.
- **P5 — #1211:** Circle reworked pending ranges to reset a single `sync_height` when no peer was available. The fork
  has no `sync_height`: `46a7b11` tracks inflight requests separately, timeout removes the exact inflight range, and
  the next status or tick recomputes the earliest free window across inflight and consensus-pending ranges. The
  upstream implementation and its now-unused sync-to-test dev dependency are therefore rejected. E4 characterizes
  the fork lifecycle, while [41642d1][f-1211-test] retains the full-node peer-restart regression scenario.
- **P6 — #1222:** the asynchronous `Decided` reply is accepted so the connector can process other messages while the
  application chooses the next height. `98c7517` also reports a dropped application reply instead of silently leaving
  consensus waiting.
- **P7 — #1198:** the discovery and reconnection fix is retained through the fork adaptation; its integration
  parameters preserve downstream controls, and the value-sync peer-restart scenarios remain in E7.
- **P8 — #1230:** the warning-level changes are adapted through the fork's diagnostic policy below rather than
  accepted mechanically.

[c-1200]: https://github.com/circlefin/malachite/commit/5b6f414b175abe60a9d146afc0837200c2f2ac82
[c-1189]: https://github.com/circlefin/malachite/commit/6f34e75eecb2e80419f8c2516875dd72b282ae4c
[c-1205]: https://github.com/circlefin/malachite/commit/10893c21c1151a7d8f0fffaccc3356182017b7fb
[c-1210]: https://github.com/circlefin/malachite/commit/76d25e596664e28b9ca1f4c2932a7c7006610545
[c-1191]: https://github.com/circlefin/malachite/commit/bd6a7c81db5cfea34dacd23da756b85c8827c0a1
[c-1215]: https://github.com/circlefin/malachite/commit/dd349d6c70d831792a4b18ada43a9f3dcd5ac6a0
[c-1174]: https://github.com/circlefin/malachite/commit/2ce022d131077d302064b9dc77ab2aec36fce5df
[c-1211]: https://github.com/circlefin/malachite/commit/3aab3d90e7187c807a32308e4d05656f4ebf7aa1
[c-1221]: https://github.com/circlefin/malachite/commit/644dd07d57a9fca3d74ab4930645f25c723378c6
[c-1222]: https://github.com/circlefin/malachite/commit/0444b9fa9ee9d77f7721451c3e1f8c8cbed476c4
[c-1198]: https://github.com/circlefin/malachite/commit/b31e2fff7ac0cfbca8cc225606af925795755e1e
[c-1226]: https://github.com/circlefin/malachite/commit/9c2c7b04f94d624e1bb4a22879f140ef37adc873
[c-1216]: https://github.com/circlefin/malachite/commit/94401a7fa231ad320ff34af74756ce0687ab72f4
[c-1228]: https://github.com/circlefin/malachite/commit/ec45cc5df7d9dc864c3931850f38feebd3832581
[c-1233]: https://github.com/circlefin/malachite/commit/65955fd69e07ecb2e67b86b5bec63d93800f1c15
[c-1234]: https://github.com/circlefin/malachite/commit/0a87a5bbab0666cd2a6974dc2110bda37f426328
[c-1230]: https://github.com/circlefin/malachite/commit/b205f4252f3064d9a74716056f63834ff33f2de9
[f-1189]: https://github.com/1Money-Co/malachite/commit/fa87a95094a6267b4808873ca91e5e03f8cef639
[f-1205]: https://github.com/1Money-Co/malachite/commit/72f3bba0773b92c8137cd5d02f3c9246ed892d02
[f-1210]: https://github.com/1Money-Co/malachite/commit/b95f1a336d37391e03c0a47f52e88e949f6a4b60
[f-1191]: https://github.com/1Money-Co/malachite/commit/07e3050231383f5827fcda94606cdcd74d5b1e2f
[f-1215]: https://github.com/1Money-Co/malachite/commit/e4e01096b935b11ddb5de124e9664d804a264761
[f-1174]: https://github.com/1Money-Co/malachite/commit/c921191213f33b6095e700024c9a69a6528caa8b
[f-free-window]: https://github.com/1Money-Co/malachite/commit/46a7b111c1ff92c5b13cc0a20d8f032ca8d3cd35
[f-1221]: https://github.com/1Money-Co/malachite/commit/a64331c26c9ad6c02f62a259e31c14f6d3717e31
[f-review-fixes]: https://github.com/1Money-Co/malachite/commit/98c7517534a6ac5f223f97d201a70e96caffe1a8
[f-1198]: https://github.com/1Money-Co/malachite/commit/32215786902ef2d31046441599c4355374e02f2e
[f-1226]: https://github.com/1Money-Co/malachite/commit/758bce1e5c4dd33b686627c91d37c65f171a09e4
[f-1216]: https://github.com/1Money-Co/malachite/commit/4c522bdc58c430b698d2e1d093e68964383b3df6
[f-1228]: https://github.com/1Money-Co/malachite/commit/9b77fc32b0aaf7cb5f7d1b49cf0d79a7dc605442
[f-1233]: https://github.com/1Money-Co/malachite/commit/a07b0dc9ab26450df438006c1c6f1529a81c7733
[f-1234]: https://github.com/1Money-Co/malachite/commit/b3e778d56928aee61a4f6e3905f2a7f87f4dbb5a
[f-1230]: https://github.com/1Money-Co/malachite/commit/4de3493d5bb309e48d50ea1fbb4ca33c5e1d5578
[f-revert-1205]: https://github.com/1Money-Co/malachite/commit/8a150786670a25d6babcba780520bda239027632
[f-revert-1210]: https://github.com/1Money-Co/malachite/commit/fb495590c27c2abb9bfdf56561294763c26c9034
[f-1211-test]: https://github.com/1Money-Co/malachite/commit/41642d100392e8c45fcbb399c20629a9ac09f2e3

- `ecdsa-signing-and-serialization`: `bcac2b2` (fork PR #23). The candidate retains the same crate/API and proves
  production key, public-key, signature, JSON decoding, and verification bytes; Emerald codecs remain G1 work.
- `sync-response-size-bound`: `5aac42b`, refined by `25c111a` and `81be837`. The candidate retains the encoded-byte
  bound before transport and characterizes the exact accepted boundary; a replacement must meet G2.
- `sync-inflight-and-pending-accounting`: `46a7b11`, building on `fefef41`. The candidate retains distinct network
  inflight and consensus-pending ranges rather than adopting a later queue model; G3 requires equivalent capacity.
- `sync-restored-height-startup`: `36c1bc5`. The candidate retains the startup guard unchanged and characterizes
  status- and tick-triggered requests; G4 requires the same restored-tip ordering.
- `sync-delayed-stale-and-partial-responses`: `b4428a3`, `94cb854`, and `d9dadcf`. The candidate preserves original
  request validation, prefix trimming, retry, and scoring semantics; G5 covers replacement equivalence.
- `sync-capacity-diagnostics`: `bd210ba`. The candidate implements the same fields from one immutable snapshot;
  G6 requires both counts, both lowest starts, and the local tip.
- `sync-network-subscription-order`: `aa2908d`. The candidate retains delayed network subscription until the
  consensus actor is installed, as exercised by E7; G10 requires the same actor-wiring order.
- `consensus-sync-input-queue`: `2789d7e`, with sync-first draining and metrics from `5695730`. The candidate keeps
  the dedicated bounded queue, sync-first ordering, and both gauges; G11 requires equivalent isolation and metrics.
- `consensus-request-errors`: Circle `9c2c7b0` / #1226, patch-equivalent fork commit `758bce1`. The candidate pins all
  three public variants through E6; G7 prevents collapsing them later.
- `wal-entry-height`: Circle `65955fd` / #1233, patch-equivalent fork commit `a07b0dc`, plus fork E3 fixtures. The
  candidate accepts the rc.3 height fix while proving both source WALs decode canonically and the source-generated
  production fixture restores its exact historical signed vote; G8 remains application-specific.
- `wal-same-height-no-replay`: the production source and fork return `Ok(None)` if the WAL actor is already at the
  requested height, while Circle rc.3 returns without replying. E3 pins the fork response and G12 prevents a future
  replacement from hanging its caller or replaying the current-height log.
- `sync-peer-restart-recovery`: Circle `3aab3d9` / #1211 is superseded by the fork free-window design described in
  P5. The candidate retains the end-to-end peer-restart regression while avoiding the dropped sync-to-test dev cycle.
- `decided-reply-async-forwarding`: Circle `0444b9f` / #1222 plus fork review fix `98c7517`. The candidate keeps the
  connector non-blocking and adds an error when the application drops its reply; G13 records both requirements.
- `rc3-logging-context`: Circle `b205f42` / #1230 plus adapted fork commit `4de3493` and the diagnostic policy below.
  The candidate retains downstream context and control flow; G9 defines replacement equivalence.

### Evidence

- **E2:** `cargo test -p informalsystems-malachitebft-signing-ecdsa --all-features --test known_answers`
- **E3:** `cargo test -p informalsystems-malachitebft-test --test unit wal_compatibility`
  and `cargo test -p informalsystems-malachitebft-core-consensus --test wal_height`
  plus `cargo nextest run -p informalsystems-malachitebft-test --test it wal` prove canonical decoding, restoration
  of the exact historical signed vote, same-height no-replay behavior, height selection, restart, and crash recovery.
- **E4:** `cargo test -p informalsystems-malachitebft-test --test unit sync_lifecycle`
  and `cargo nextest run -p informalsystems-malachitebft-test --test it value_sync`.
  The unit suite covers restored startup, saturated inflight/pending accounting and diagnostics, and delayed, stale,
  partial, unsolicited, and wrong-peer responses. The integration suite covers the exact encoded response-size bound
  and the complete value-sync scenario set.
- **E5:** after the rc.3 merge, `cargo check --workspace --all-targets --all-features` passed, every E2–E4 focused
  command passed, and the combined WAL/value-sync integration run passed all 22 scenarios without retry.
- **E6:** `cargo test -p informalsystems-malachitebft-app-channel --test consensus_request` pins `Closed`, `Full`,
  and `Recv`; the E4 lifecycle suite pins capacity state and control flow after the logging reconciliation.
- **E7:** `cargo nextest run -p informalsystems-malachitebft-test --test it value_sync` exercises the retained sync
  actor wiring and dedicated consensus sync queue end to end; `cargo all-tests` also covers the Starknet host path.
- **E8:** the app-channel test `decided_reply_is_async_and_logs_when_application_drops_it` proves that the connector
  processes another request while `Decided` is pending and emits an error when the application drops its reply.

### Retirement gates

- **G1:** an unmodified upstream dependency passes Emerald k256 address, signed-byte, key-file, and protobuf fixtures.
- **G2:** replacement response construction and transport tests prove the same send and receive byte bounds.
- **G3:** a replacement queue proves the combined inflight and pending height allowance under saturation.
- **G4:** a replacement waits for restored consensus height before any status- or tick-triggered request.
- **G5:** delayed, stale, partial, unsolicited, and wrong-peer responses preserve retry and scoring behavior.
- **G6:** replacement diagnostics retain inflight and pending counts, lowest starts, and the local tip.
- **G7:** application requests retain distinct closed, full, and dropped-response failures.
- **G8:** Emerald-encoded WAL and redb/history fixtures restart safely on the replacement engine in the Emerald
  follow-up.
- **G9:** replacement logs retain required severity and structured context without changing control flow.
- **G10:** a replacement installs the consensus actor before subscribing to sync network events and cannot silently
  lose an early value response.
- **G11:** a replacement retains a bounded dedicated sync-input queue, sync-first draining, and separate height and
  input-count gauges.
- **G12:** repeated startup at the WAL actor's current height replies with no replay entries and never hangs its caller.
- **G13:** the `Decided` application reply remains asynchronous, and a dropped reply emits an operator-visible error.

### rc.3 diagnostic policy

- Prevote, precommit, rebroadcast, and sync request timeouts use rc.3's informational severity.
- Wrong-peer, wrong-range, and wrong-certificate responses remain warnings and preserve scoring and retry behavior.
- Saturated sync capacity reports inflight and pending height counts, both lowest starts, and the local tip from one
  immutable snapshot; the log also retains request counts, the requested height count, and the configured maximum.
- WAL append, reset, flush, and reply failures remain errors with their causes. Consensus request send and receive
  failures likewise remain errors while returning the exact `Closed`, `Full`, or `Recv` variant.
- These logging decisions do not change return values, coroutine resumption, peer scoring, or retry state.

### Pending Emerald qualification

This Malachite reconciliation does not complete application integration or production qualification. The Emerald
follow-up must still:

- pin the exact candidate Malachite revision;
- run Emerald's k256 address and signed-byte compatibility fixtures against that pin;
- prove application WAL plus redb/history restart compatibility;
- rehearse mixed-version validator operation and rolling replacement; and
- complete the separate production promotion decision and rollout.

Until those steps pass, the fork remains required and this candidate must not be described as production-qualified.

### Local validation record

- `env RUST_LOG=warn cargo all-tests`: 282 tests passed and 12 were skipped in 1,147.663 seconds.
- Quint 0.33.1 emits traces for imported-module tests; the Starknet part-stream generator filters to its four intended
  scenarios, and both random and specified MBT tests pass.
- `cargo lint`: passed.
- `cargo clippy --tests -- -D warnings`: passed.
- `cargo +nightly fmt --all --check`: passed.
- The focused E2–E6 commands passed, including all 22 post-merge WAL/value-sync scenarios without retry.
- Review follow-up `98c7517`: the six WAL compatibility tests, WAL height test, ten sync lifecycle tests, three
  app-channel request tests, affected-crate all-target/all-feature check, clippy, and formatting check passed.
- The focused asynchronous `Decided` reply regression passed and was mutation-checked against a warning downgrade.

## Value-sync startup recovery

Based on Informal Systems Malachite commit `bcac2b2dbd369a6c29b9151fcd0889b133d7221a`.
Tracked in [interop issue #375](https://github.com/1Money-Co/1money-interoperability-protocol/issues/375).

- Wait for consensus to initialize the restored tip before periodic value-sync requests.
- Validate late responses against the original request, then discard already-decided values or trim their prefix.
- Retain the existing `batch_size * parallel_requests` allowance for inflight and received undecided heights.
- Report separate inflight/pending height totals, their lowest starts, and the local tip when the allowance is full.

These changes adapt invariants verified in Circle Malachite v0.8.1, commit
`f20bebd9c8a7c6a395605bfaa97f7d3a536e3bd3`; they are not a cherry-pick of its newer queue architecture:

- [`maybe_request_values` waits for consensus to start][startup].
- [Height transitions prune obsolete requests][pruning].
- [The sync actor ignores values below the current consensus height][stale].

This fork retains separate inflight and pending-consensus maps, so stale values are filtered at the response boundary
before they can reserve pending capacity. Obsolete inflight requests remain tracked until response or timeout:
removing their IDs alone would classify honest late replies as unknown requests and penalize the serving peers.

[startup]: https://github.com/circlefin/malachite/blob/v0.8.1/code/crates/sync/src/handle.rs#L258
[pruning]: https://github.com/circlefin/malachite/blob/v0.8.1/code/crates/sync/src/state.rs#L280
[stale]: https://github.com/circlefin/malachite/blob/v0.8.1/code/crates/engine/src/sync.rs#L670
