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
reconnection, workflow, and logging changes. This does not retire the fork or qualify the result for production.

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
| `consensus-request-errors` | Yes | Yes | Yes | Retain; exact variants pinned | E6 | G7 |
| `wal-entry-height` | Yes | Yes | Yes | Retain; both baselines pinned | E3 | G8 |
| `rc3-logging-context` | Partial | Partial | Yes | Reconciled; context audited | E6 | G9 |

### Evidence

- **E2:** `cargo test -p informalsystems-malachitebft-signing-ecdsa --all-features --test known_answers`
- **E3:** `cargo test -p informalsystems-malachitebft-test --test unit wal_compatibility`
  and `cargo test -p informalsystems-malachitebft-core-consensus --test wal_height`
  plus `cargo nextest run -p informalsystems-malachitebft-test --test it wal`
- **E4:** `cargo test -p informalsystems-malachitebft-test --test unit sync_lifecycle`
  and `cargo nextest run -p informalsystems-malachitebft-test --test it value_sync`.
  The unit suite covers restored startup, saturated inflight/pending accounting and diagnostics, and delayed, stale,
  partial, unsolicited, and wrong-peer responses. The integration suite covers the exact encoded response-size bound
  and the complete value-sync scenario set.
- **E5:** after the rc.3 merge, `cargo check --workspace --all-targets --all-features` passed, every E2–E4 focused
  command passed, and the combined WAL/value-sync integration run passed all 22 scenarios without retry.
- **E6:** `cargo test -p informalsystems-malachitebft-app-channel --test consensus_request` pins `Closed`, `Full`,
  and `Recv`; the E4 lifecycle suite pins capacity state and control flow after the logging reconciliation.

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

### rc.3 diagnostic policy

- Prevote, precommit, rebroadcast, and sync request timeouts use rc.3's informational severity.
- Wrong-peer, wrong-range, and wrong-certificate responses remain warnings and preserve scoring and retry behavior.
- Saturated sync capacity reports inflight and pending height counts, both lowest starts, and the local tip from one
  immutable snapshot; the log also retains request counts, the requested height count, and the configured maximum.
- WAL append, reset, flush, and reply failures remain errors with their causes. Consensus request send and receive
  failures likewise remain errors while returning the exact `Closed`, `Full`, or `Recv` variant.
- These logging decisions do not change return values, coroutine resumption, peer scoring, or retry state.

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
