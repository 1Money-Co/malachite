# Durable WAL recovery boundary for the v0.7 release

## Goal

Restore unattended startup after an interrupted append when the WAL proves that the incomplete bytes are beyond
the last committed consensus state. Preserve all committed signing history and fail closed on corruption or uncertain
durability. Deliver this in the v0.7-based fork for Emerald and arrange the patch so Phase 3 can reuse the storage
code and its fixtures on the v0.8.2-based fork. Circle adoption is a later release decision tracked in Emerald #49.

Phase 2 starts from Malachite PR #3 at `6c899e1e` (Emerald runtime pin `9828de16`), with Emerald PR #68 as the
consumer. Design against Circle v0.8.2 at `a453b32155f4ba9f959724279b52f601bae92e09`. Its low-level WAL file
code is the same as Circle v0.7.0. The worker and replay input types differ between the releases.

## Approach

Use a checksummed, versioned sidecar that records a WAL generation and the durable byte offset of the last
committed frame boundary. Keep the WAL header and frame encoding, signing identities, and Emerald database schema.
The sidecar adds persistent metadata and a sync to each advancing commit. Reuse the existing worker RPCs and keep
all filesystem mechanics in `malachitebft-wal`. An engine adapter applies the replay and flush rules in each release.

Archiving the observed suffix and then truncating it gives operators evidence. It supplies no proof of the original
committed offset because legacy payload CRCs exclude frame lengths. A durable append journal containing the full
encoded write batch could recover the bytes themselves. It would duplicate the payload writes and require a larger
journal protocol. The offset checkpoint is the proposed fit for this backport.

## Storage and recovery contract

- Hold the exclusive WAL lock throughout initialization, recovery, append, flush and generation transitions.
  Bind metadata to the active generation and a digest of the exact committed WAL prefix, including the header.
  Validate the metadata checksum, sequence, offset, prefix digest and exact frame boundary before using it.
  Maintain the digest incrementally as bytes commit. Keep metadata replacement bounded and crash-safe.
- When a flush advances the boundary, sync the WAL bytes before committing the new sidecar. Acknowledge the
  flush only after both are durable. A failed checkpoint update follows the existing WAL safety-hang path.
  A flush with an unchanged boundary may reuse an already durable checkpoint.
- A complete legacy WAL can be opened without conversion. Validate its full frame stream and application decode
  before replay or activation, then sync it and establish the first checkpoint. A legacy WAL with an incomplete
  tail has no trustworthy boundary and remains fail-closed. Preserve its bytes and report the exact reason.
- Use a durable activation record in the WAL directory to distinguish a legacy installation from a managed WAL
  whose checkpoint is missing. Before allowing managed writes, complete activation and sync the directory. Make
  partial activation restartable only while a durable initialization record proves no managed writes were allowed.
  A missing checkpoint on an activated WAL is a hard error. This assumes the node directory is kept intact. A
  storage restore that removes both the activation record and checkpoint is outside automatic recovery and must
  be handled as potentially stale signing state. There is no change to the WAL header to detect such a restore.
- On managed reopen, validate all committed frames. A short file, framing/CRC/decompression error in the
  committed prefix, or invalid/mismatched metadata prevents automatic repair and replay. Treat unreadable or
  damaged metadata as an error. Distinguish first activation from missing metadata on an already managed WAL.
- After validating the committed prefix, scan the uncommitted suffix. Keep complete valid records. Preserve a
  demonstrably incomplete final frame in a durable diagnostic artifact before removing it. Fsync the artifact
  and directory before truncating, and sync the WAL after truncation. Retry safely if interrupted during repair.
  Keep hard errors for malformed/oversized lengths, full frames with invalid CRC or compression, and decode errors.
- Validate and seal retained replay input before handing it to consensus. In both versions the normal append and
  flush wrappers skip recovery. Backport the minimal v0.8.2 recorded-message check and signature reuse needed to
  match replayed votes and proposals with their sealed records. A different re-derived signed body for an already
  recorded slot safety-hangs before signing. Persist and seal a new replay output before it escapes the process.
  Audit restream, rebroadcast and decide along with ordinary publication, including effects reached before the
  usual `WalAppend`. Reuse v0.8.2's existing recorded-message machinery when carrying the patch forward.

## Height changes and rollback boundary

Treat a height reset and its checkpoint update as one recoverable transition under the lock. Before mutating either
file, record the old and new generations for a forward transition authorized by the host's durable application
height. If a reset is pending on reopen, the worker waits for the host to confirm the target height before finishing
the transition. It exposes neither generation for replay or append until resolution. Mismatched height, ambiguous
metadata, backward transitions and interrupted same-height resets fail closed. Preserve the existing authorization
rules for normal height transitions and `RestartHeight`. A buffered reset/fetch failure must reach the same fatal
liveness path as its direct counterpart.

The WAL and its recovery metadata must travel together during operational backup or diagnostic collection. Once
managed metadata is active, an older binary can write the WAL without advancing it. Roll forward with a compatible
binary. Keep the existing Phase 1 `0x08` downgrade limit. Document startup behavior for legacy, fresh and managed
files and the procedure for preserving evidence after unrecoverable corruption.

## Backport and qualification

1. Isolate the file-level protocol and deterministic fault fixtures in a version-independent WAL crate patch. Check
   that it applies and its tests run against the exact Circle v0.8.2 source in an isolated checkout.
2. Keep the v0.7 worker and replay integration in a separate commit. Confine the corresponding v0.8.2 adaptation
   to its worker/input/replay differences. Port only the recovery behavior and tests during this release cycle.
3. Force interruption at every relevant ordering boundary, including checkpoint advance, initial activation,
   partial header and partial payload append, suffix preservation, repair, and forward-height reset. Check byte-for-byte
   preservation on rejection, including an in-bound corrupt length in committed history and a bad final CRC. Exercise
   both raw and compressed records, stale metadata, a repeated reopen, and repeated crashes after replay output.
4. Use the real engine path to prove that publish and decide wait for checkpoint durability, corruption has no replay
   effects, and buffered restart propagates a reset error. Retain the existing mutation-tested safety-hang checks.
5. Repin all Emerald dependencies and their lock sources together. Prove that historical WAL and redb state still
   reopen, and that a torn append after checkpoint activation recovers without losing the prior polka record. Run the
   focused suites, workspace tests and MBT. Record flush latency and I/O in release-artifact testnet rolling and restart
   qualification before shipping. Track the upstreamable patch and exact evidence in Emerald #49.
