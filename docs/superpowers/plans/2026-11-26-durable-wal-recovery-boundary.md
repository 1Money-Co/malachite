# Durable WAL Recovery Boundary Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans or superpowers:subagent-driven-development.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover a provably uncommitted torn WAL tail on Phase 2 restart and preserve all durable signing history.

**Architecture:** Keep v1 frame bytes. A file-backed recovery wrapper owns the checkpoint, recovery scan, and reset
transition. Keep version-specific replay changes in the engine and commit them separately from the storage wrapper.

**Tech Stack:** Rust, SHA-256, the existing exclusive file lock, fsync and atomic sidecar replacement, Tokio/ractor.

**Spec:** [Durable WAL recovery boundary](../specs/2026-11-26-durable-wal-recovery-boundary-design.md).

## Global constraints

- Use current Phase 2 branches and work in `code/` for Malachite Cargo commands. Preserve signed bytes and schemas.
- Authenticate the exact prefix and distinguish legacy initialization from incomplete/missing managed metadata.
- WAL fsync precedes checkpoint durability. Neither live nor replay output may rely on an unsealed record.
- An old binary cannot safely maintain the checkpoint. Repair requires an intact paired directory and rolling forward.
- No Phase 3 adoption, production deployment, or upstream submission in this change.

## Review focus

The tests must cover failed or interrupted activation, an invalid length within the checkpoint, repeated repair,
failed or interrupted forward reset, and a second crash after recovery publishes a newly recorded signing choice.

### Task 1: Shared WAL file protocol

**Files:** `code/crates/wal/src/{lib.rs,log.rs,log/recovery.rs}`, `code/crates/wal/Cargo.toml`,
`code/crates/wal/tests/it/recovery.rs`, `code/Cargo.lock`.

**Interface:** Add file-backed `RecoveryLog::open(path, validate: impl FnMut(&[u8]) -> io::Result<()>)`.
Expose `sequence`, `append`, `flush`, `iter`, `reset`, and `confirm_pending_reset`. The legacy `Log` remains strict.

- [x] Cover the byte-layout fixture through the new public interface. Append two records, flush, interrupt a final
  header or payload, and reopen. Require the exact prior records and the original tail bytes in the evidence file.
- [x] Run `cargo test -p arc-malachitebft-wal --test it recovery::` and read the failing baseline.
- [x] Implement bounded metadata, the activation handshake, strict raw frame validation and prefix hashing under
  the file lock. On managed reopen, only trim a short final frame outside the validated committed offset. Sync
  preserved evidence before mutation. On success seal valid retained frames before exposing them to the decoder.
- [x] Cover clean legacy bootstrapping, legacy short frames, invalid CRC/decompression, oversized and in-bound
  corrupt lengths, changed prefix, stale/missing checkpoint, complete uncommitted frames, and second reopen.
- [x] Implement journaled reset authorization and host confirmation, with interruption tests for the intent,
  WAL header, WAL sync and checkpoint replacement. A pending reset must block reading and writing active entries.
- [x] Test the WAL suite with default and compression features. Apply the isolated storage commits to a scratch
  checkout of exact v0.8.2 and run the same tests. Commit the storage protocol separately.

### Task 2: v0.7 engine adapter and replay output

**Files:** `code/crates/engine/src/wal{.rs,/thread.rs}`, `code/crates/engine/src/consensus.rs`,
`code/crates/app-channel/tests/wal_safety_hang.rs`, relevant core-consensus replay tests.

- [x] Reproduce a managed torn-tail restart through the real actor. Prove a failed reset is propagated from
  buffered `RestartHeight`, and a damaged committed record cannot trigger any replay send.
- [x] Wire the worker to the validated file wrapper. Give it the host's actual startup height before resolving
  reset intent. Treat wrong-height append as a failure on the managed path. Preserve existing actor error policy.
- [x] Test prior signed output reuse and divergence, a new replay-generated choice, and a restart after its
  publication. Make the signer distinguish a new call from reusing the recorded signature in these tests.
- [x] Adapt v0.8.2's recorded-message checks to v0.7 WAL types. Audit publish, restream, rebroadcast and decide
  effects, including restream before ordinary append. Durably seal new replay outputs before external effects.
- [ ] Run actor, recovery, WAL compatibility, and full Malachite tests. Update ADR-007, release notes and the
  patch inventory with reset, crash, downgrade and operational evidence handling. Commit adapter separately.

### Task 3: Emerald repin and qualification

**Files:** Emerald `Cargo.toml`, `Cargo.lock`, `app/tests/malachite_v07_recovery.rs`, and the Phase 2 qualification doc.

- [x] Pin all entries to the reviewed Malachite runtime SHA. Assert that only the intended source/lock entries
  move except for the reviewed storage dependency. Preserve the undecodable legacy short-tail fixture.
- [ ] Run Emerald recovery, non-MBT workspace and MBT suites. Run repo-required Clippy and format checks before
  pushing either branch. Run the relevant no-retry fault fixtures for both implementations.
- [x] Review the whole branch and confirm the testnet runbook covers artifact identity, repeated restart during
  append, flush latency, downtime and recovery-sidecar preservation. Report operational results separately.
- [ ] Update existing PRs and issue #48 with exact pins, verification and remaining release-artifact gates.
