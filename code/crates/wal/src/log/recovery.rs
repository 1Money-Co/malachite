use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::{read_payload, Log, MAX_ENTRY_SIZE};
use crate::constants::{ENTRY_HEADER_SIZE, HEADER_SIZE};
use crate::ext::{read_u32, read_u64, read_u8, write_u64};
use crate::{LogIter, Storage, Version};

const MAGIC: &[u8; 8] = b"RWAL0001";
const INITIALIZING: &[u8; 8] = b"RWALINIT";
const METADATA_LEN: usize = 105;

#[derive(Clone, Copy, Debug)]
struct Checkpoint {
    generation: u64,
    sequence: u64,
    end: u64,
    digest: [u8; 32],
}

impl Checkpoint {
    fn encode(self, target: Option<u64>) -> [u8; METADATA_LEN] {
        let mut bytes = [0; METADATA_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..16].copy_from_slice(&self.generation.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.sequence.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.end.to_be_bytes());
        bytes[32..64].copy_from_slice(&self.digest);
        if let Some(target) = target {
            bytes[64] = 1;
            bytes[65..73].copy_from_slice(&target.to_be_bytes());
        }
        let checksum = Sha256::digest(&bytes[..73]);
        bytes[73..].copy_from_slice(&checksum);
        bytes
    }

    fn decode(bytes: [u8; METADATA_LEN]) -> io::Result<(Self, Option<u64>)> {
        if &bytes[..8] != MAGIC || Sha256::digest(&bytes[..73])[..] != bytes[73..] {
            return Err(invalid("Invalid WAL checkpoint format or checksum"));
        }
        let pending = bytes[64] == 1;
        let generation = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let sequence = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let end = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        let digest = bytes[32..64].try_into().unwrap();
        let target = u64::from_be_bytes(bytes[65..73].try_into().unwrap());
        if generation == 0 || end < HEADER_SIZE || bytes[64] > 1 || (!pending && target != 0) {
            return Err(invalid("Invalid WAL checkpoint fields"));
        }
        Ok((
            Self {
                generation,
                sequence,
                end,
                digest,
            },
            pending.then_some(target),
        ))
    }
}

#[derive(Debug)]
enum State {
    Ready(Checkpoint, Sha256),
    Pending(Checkpoint, u64),
    Failed,
}

/// A v1 WAL with a durable recovery boundary. All companion files live beside the WAL and belong
/// to the same signing history. An older writer must never reopen a managed WAL.
#[derive(Debug)]
pub struct RecoveryLog {
    log: Log<File>,
    state: State,
    recovered_tail: Option<PathBuf>,
}

impl RecoveryLog {
    /// Opens and locks the WAL, validating the raw frames and their application payloads before
    /// accepting a checkpoint or preserving and trimming an incomplete uncommitted frame.
    pub fn open(
        path: impl AsRef<Path>,
        mut validate: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let marker_path = companion(path, ".managed");
        let marker = read_activation(&marker_path)?;

        // Acquire the original WAL's lock before inspecting the mutable checkpoint. A second
        // opener must not observe a mixture of pre- and post-flush state.
        let managed = marker.is_some() || companion(path, ".checkpoint").try_exists()?;
        let mut file = if managed {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            use advisory_lock::{AdvisoryFileLock, FileLockMode};
            AdvisoryFileLock::try_lock(&file, FileLockMode::Exclusive).map_err(|error| {
                io::Error::other(format!("Failed to acquire WAL lock: {error}"))
            })?;
            file
        } else {
            <File as Storage>::open_with(path, ())?
        };
        let marker = read_activation(&marker_path)?;
        let metadata = read_fixed(&companion(path, ".checkpoint"))?
            .map(Checkpoint::decode)
            .transpose()?;
        if marker.as_ref() == Some(MAGIC) && metadata.is_none() {
            return Err(invalid("Managed WAL checkpoint is missing"));
        }
        if let Some((checkpoint, Some(target))) = metadata {
            if marker.as_ref() != Some(MAGIC) {
                return Err(invalid("WAL reset intent has no activation record"));
            }
            let log =
                Log::from_raw_parts(file, path.to_owned(), Version::V1, checkpoint.sequence, 0);
            return Ok(Self {
                log,
                state: State::Pending(checkpoint, target),
                recovered_tail: None,
            });
        }

        let mut size = file.metadata()?.len();
        if metadata.is_none() && (size == 0 || marker.as_ref() == Some(INITIALIZING)) {
            if marker.is_none() {
                atomic_write(&marker_path, |marker| marker.write_all(INITIALIZING))?;
            }
            let mut header = [0; HEADER_SIZE as usize];
            header[..4].copy_from_slice(&(Version::V1 as u32).to_be_bytes());
            if size > HEADER_SIZE || !header.starts_with(&read_prefix(&mut file, size as usize)?) {
                return Err(invalid("Unexpected WAL bytes during fresh activation"));
            }
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            size = HEADER_SIZE;
        }
        let version = Version::try_from(read_u32_at_start(&mut file)?)
            .map_err(|_| invalid("Invalid WAL version"))?;
        let sequence = read_u64(&mut file)?;
        let previous = metadata.map(|(checkpoint, _)| checkpoint);
        let committed_end = previous.map_or(size, |checkpoint| checkpoint.end);
        if committed_end > size || previous.is_some_and(|cp| cp.sequence != sequence) {
            return Err(invalid("WAL does not match the committed checkpoint"));
        }
        let mut digest = Sha256::new();
        hash_range(&mut file, 0, committed_end, &mut digest)?;
        if previous.is_some_and(|checkpoint| checkpoint.digest != finish_digest(&digest)) {
            return Err(invalid("Committed WAL prefix digest mismatch"));
        }

        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        let (mut len, end) = scan(&mut file, HEADER_SIZE, committed_end, false, &mut validate)?;
        debug_assert_eq!(end, committed_end);
        let (suffix_len, end) = scan(&mut file, committed_end, size, true, &mut validate)?;
        len += suffix_len;
        let recovered_tail = if end < size {
            // Bytes at or below committed_end have been authenticated and fully validated.
            let archive = preserve_tail(&mut file, path, end, size)?;
            file.set_len(end)?;
            Some(archive)
        } else {
            None
        };
        hash_range(&mut file, committed_end, end, &mut digest)?;
        let checkpoint = Checkpoint {
            generation: previous.map_or(1, |cp| cp.generation),
            sequence,
            end,
            digest: finish_digest(&digest),
        };
        file.sync_all()?;
        if previous.is_none() || end != committed_end {
            save_checkpoint(path, checkpoint, None)?;
        } else {
            // A prior checkpoint rename can survive a process crash before its directory sync.
            // Seal that directory entry before handing replay data to the engine.
            sync_directory(path)?;
        }
        if marker.as_ref() != Some(MAGIC) {
            atomic_write(&marker_path, |file| file.write_all(MAGIC))?;
        }
        let log = Log::from_raw_parts(file, path.to_owned(), version, sequence, len);
        Ok(Self {
            log,
            state: State::Ready(checkpoint, digest),
            recovered_tail,
        })
    }

    pub fn sequence(&self) -> u64 {
        self.log.sequence()
    }

    pub fn len(&self) -> usize {
        self.log.len()
    }

    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    pub fn size_bytes(&self) -> io::Result<u64> {
        self.log.size_bytes()
    }

    /// Diagnostic artifact created by this open, if it recovered an incomplete append.
    pub fn recovered_tail(&self) -> Option<&Path> {
        self.recovered_tail.as_deref()
    }

    pub fn iter(&mut self) -> io::Result<LogIter<'_>> {
        if self.log.size_bytes()? != self.ready()?.0.end {
            return Err(invalid("WAL replay requires a sealed prefix"));
        }
        self.log.iter()
    }

    pub fn append(&mut self, payload: impl AsRef<[u8]>) -> io::Result<()> {
        self.ready()?;
        let payload = payload.as_ref();
        if payload.len() > MAX_ENTRY_SIZE {
            return Err(invalid("WAL append exceeds the maximum frame size"));
        }
        self.log
            .append(payload)
            .inspect_err(|_| self.state = State::Failed)
    }

    /// Syncs WAL bytes first and acknowledges only after its new committed prefix is durable.
    pub fn flush(&mut self) -> io::Result<()> {
        let (checkpoint, digest) = self.ready()?;
        let checkpoint = *checkpoint;
        let mut digest = digest.clone();
        self.state = State::Failed;
        let end = self.log.size_bytes()?;
        self.log.flush()?;
        if end == checkpoint.end {
            self.state = State::Ready(checkpoint, digest);
            return Ok(());
        }
        hash_range(&mut self.log.storage, checkpoint.end, end, &mut digest)?;
        let checkpoint = Checkpoint {
            end,
            digest: finish_digest(&digest),
            ..checkpoint
        };
        save_checkpoint(self.log.path(), checkpoint, None)?;
        self.state = State::Ready(checkpoint, digest);
        Ok(())
    }

    /// Begins an authorized height transition. A crash between the durable intent and the
    /// new checkpoint leaves a WAL that can only resume through host confirmation.
    pub fn reset(&mut self, sequence: u64) -> io::Result<()> {
        let checkpoint = *self.ready()?.0;
        if sequence < checkpoint.sequence {
            return Err(invalid(
                "Refusing to reset the managed WAL to an earlier height",
            ));
        }
        self.state = State::Failed;
        save_checkpoint(self.log.path(), checkpoint, Some(sequence))?;
        self.finish_reset(checkpoint, sequence)
    }

    /// Finishes an interrupted forward reset only when the application confirms its target.
    /// Returns false when the WAL was already ready.
    pub fn confirm_pending_reset(&mut self, host_sequence: u64) -> io::Result<bool> {
        let (checkpoint, target) = match self.state {
            State::Pending(checkpoint, target) => (checkpoint, target),
            State::Ready(..) => return Ok(false),
            State::Failed => return Err(invalid("WAL is unavailable after a storage failure")),
        };
        if target != host_sequence || target <= checkpoint.sequence {
            return Err(invalid(
                "Interrupted WAL reset does not match a confirmed forward height",
            ));
        }

        let file = &mut self.log.storage;
        let size = file.metadata()?.len();
        let version = read_u32_at_start(file)?;
        let sequence = read_u64(file)?;
        let old_sequence = checkpoint.sequence.to_be_bytes();
        let new_sequence = target.to_be_bytes();
        let sequence_bytes = sequence.to_be_bytes();
        // File length and header can persist in different orders. The header must be either
        // the old/new value or a partial write of the new bytes over the old bytes.
        let valid_header = version == Version::V1 as u32
            && (0..=8).any(|split| {
                sequence_bytes[..split] == new_sequence[..split]
                    && sequence_bytes[split..] == old_sequence[split..]
            });
        if !valid_header {
            return Err(invalid("Invalid WAL header during reset recovery"));
        }
        if size != HEADER_SIZE {
            let mut digest = Sha256::new();
            digest.update(version.to_be_bytes());
            digest.update(old_sequence);
            if size < checkpoint.end {
                return Err(invalid("WAL was shortened outside the reset protocol"));
            }
            hash_range(file, HEADER_SIZE, checkpoint.end, &mut digest)?;
            if finish_digest(&digest) != checkpoint.digest {
                return Err(invalid("Old committed WAL mismatch during reset recovery"));
            }
        }
        self.state = State::Failed;
        self.finish_reset(checkpoint, target)?;
        Ok(true)
    }

    fn finish_reset(&mut self, old: Checkpoint, sequence: u64) -> io::Result<()> {
        let generation = old
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("WAL generation overflow"))?;
        let file = &mut self.log.storage;
        file.set_len(HEADER_SIZE)?;
        file.seek(SeekFrom::Start(4))?;
        write_u64(file, sequence)?;
        file.sync_all()?;
        let mut digest = Sha256::new();
        hash_range(file, 0, HEADER_SIZE, &mut digest)?;
        let checkpoint = Checkpoint {
            generation,
            sequence,
            end: HEADER_SIZE,
            digest: finish_digest(&digest),
        };
        save_checkpoint(self.log.path(), checkpoint, None)?;
        self.log.sequence = sequence;
        self.log.len = 0;
        self.state = State::Ready(checkpoint, digest);
        Ok(())
    }

    fn ready(&self) -> io::Result<(&Checkpoint, &Sha256)> {
        match &self.state {
            State::Ready(checkpoint, digest) => Ok((checkpoint, digest)),
            State::Pending(..) => Err(invalid("WAL reset requires host height confirmation")),
            State::Failed => Err(invalid("WAL is unavailable after a storage failure")),
        }
    }
}

fn scan(
    file: &mut File,
    mut pos: u64,
    bound: u64,
    allow_torn_tail: bool,
    validate: &mut impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<(usize, u64)> {
    let mut count = 0;
    while pos < bound {
        let remaining = bound - pos;
        let flag = read_u8(file)?;
        if flag > 1 {
            return Err(invalid("Invalid WAL compression flag"));
        }
        let length = if remaining >= 9 {
            Some(read_u64(file)?)
        } else {
            None
        };
        if length.is_some_and(|length| length > MAX_ENTRY_SIZE as u64) {
            return Err(invalid("WAL frame length exceeds the maximum"));
        }
        let Some(length) = length.filter(|length| {
            ENTRY_HEADER_SIZE
                .checked_add(*length)
                .is_some_and(|frame| frame <= remaining)
        }) else {
            if allow_torn_tail {
                return Ok((count, pos));
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Incomplete WAL frame at offset {pos} before committed boundary {bound}"),
            ));
        };
        let crc = read_u32(file)?;
        let payload = read_payload(&mut *file, flag != 0, length, crc)?;
        validate(&payload)?;
        pos += ENTRY_HEADER_SIZE + length;
        count += 1;
    }
    Ok((count, pos))
}

fn read_u32_at_start(file: &mut File) -> io::Result<u32> {
    file.seek(SeekFrom::Start(0))?;
    read_u32(file)
}

fn read_prefix(file: &mut File, length: usize) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn finish_digest(digest: &Sha256) -> [u8; 32] {
    digest.clone().finalize().into()
}

fn hash_range(file: &mut File, start: u64, end: u64, digest: &mut Sha256) -> io::Result<()> {
    let length = end
        .checked_sub(start)
        .ok_or_else(|| invalid("WAL shortened during hashing"))?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = file.take(length);
    if io::copy(&mut reader, digest)? != length {
        return Err(invalid("WAL ended during hashing"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn companion(path: &Path, suffix: impl AsRef<OsStr>) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn read_fixed<const N: usize>(path: &Path) -> io::Result<Option<[u8; N]>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() != N as u64 {
        return Err(invalid(format!(
            "Invalid WAL metadata length: {}",
            path.display()
        )));
    }
    let mut bytes = [0; N];
    file.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

fn read_activation(path: &Path) -> io::Result<Option<[u8; 8]>> {
    let marker = read_fixed(path)?;
    if marker
        .as_ref()
        .is_some_and(|value| value != MAGIC && value != INITIALIZING)
    {
        return Err(invalid("Invalid WAL activation record"));
    }
    Ok(marker)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

fn atomic_write(path: &Path, write: impl FnOnce(&mut File) -> io::Result<()>) -> io::Result<()> {
    let temporary = companion(path, ".tmp");
    let mut file = File::create(&temporary)?;
    write(&mut file)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, path)?;
    sync_directory(path)
}

fn save_checkpoint(path: &Path, checkpoint: Checkpoint, target: Option<u64>) -> io::Result<()> {
    atomic_write(&companion(path, ".checkpoint"), |file| {
        file.write_all(&checkpoint.encode(target))
    })
}

fn preserve_tail(file: &mut File, path: &Path, start: u64, end: u64) -> io::Result<PathBuf> {
    let mut digest = Sha256::new();
    hash_range(file, start, end, &mut digest)?;
    let archive = companion(path, format!(".torn-{start}-{:x}", digest.finalize()));
    atomic_write(&archive, |archive| {
        file.seek(SeekFrom::Start(start))?;
        if io::copy(&mut file.take(end - start), archive)? != end - start {
            return Err(invalid("WAL ended during tail preservation"));
        }
        Ok(())
    })?;
    Ok(archive)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_forward_reset_waits_for_host_across_disk_boundaries() -> io::Result<()> {
        for (shorten, sequence_bytes) in [(false, 0), (true, 0), (true, 4), (true, 8), (false, 8)] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("consensus.wal");
            let mut wal = RecoveryLog::open(&path, |_| Ok(()))?;
            wal.reset(7)?;
            wal.append(b"old signing choice")?;
            wal.flush()?;
            let target = 0x1020_3040_5060_7080_u64;
            save_checkpoint(&path, *wal.ready()?.0, Some(target))?;
            if shorten {
                wal.log.storage.set_len(HEADER_SIZE)?;
            }
            wal.log.storage.seek(SeekFrom::Start(4))?;
            wal.log
                .storage
                .write_all(&target.to_be_bytes()[..sequence_bytes])?;
            wal.log.storage.sync_all()?;
            drop(wal);

            let before = fs::read(&path)?;
            let mut reopened = RecoveryLog::open(&path, |_| Ok(()))?;
            assert!(reopened.iter().is_err());
            assert!(reopened.append(b"new signing choice").is_err());
            assert!(reopened.confirm_pending_reset(7).is_err());
            assert_eq!(fs::read(&path)?, before);
            assert!(reopened.confirm_pending_reset(target)?);
            assert_eq!(reopened.sequence(), target);
            assert_eq!(reopened.iter()?.count(), 0);
            reopened.append(b"new signing choice")?;
            reopened.flush()?;
            drop(reopened);

            let mut reopened = RecoveryLog::open(&path, |_| Ok(()))?;
            assert!(!reopened.confirm_pending_reset(target)?);
            assert_eq!(
                reopened.iter()?.collect::<io::Result<Vec<_>>>()?,
                [b"new signing choice".to_vec()]
            );
        }
        Ok(())
    }

    #[test]
    fn interrupted_same_height_reset_cannot_be_reauthorized_by_height() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("consensus.wal");
        let mut wal = RecoveryLog::open(&path, |_| Ok(()))?;
        wal.reset(7)?;
        wal.append(b"old vote")?;
        wal.flush()?;
        save_checkpoint(&path, *wal.ready()?.0, Some(7))?;
        drop(wal);

        let before = fs::read(&path)?;
        let mut reopened = RecoveryLog::open(&path, |_| Ok(()))?;
        assert!(reopened.confirm_pending_reset(7).is_err());
        assert_eq!(fs::read(&path)?, before);
        Ok(())
    }
}
