use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use arc_malachitebft_wal::{Log, RecoveryLog};

fn open(path: &Path) -> io::Result<RecoveryLog> {
    RecoveryLog::open(path, |_| Ok(()))
}

#[test]
fn recovers_interrupted_append_after_a_confirmed_flush() -> io::Result<()> {
    // A writer that rejects all short frames cannot recover either interruption point.
    let mut complete_header = vec![0];
    complete_header.extend_from_slice(&6_u64.to_be_bytes());
    complete_header.extend_from_slice(&0_u32.to_be_bytes());
    complete_header.extend_from_slice(b"par");
    for tail in [vec![0, 0, 0], complete_header] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("consensus.wal");
        {
            let mut wal = open(&path)?;
            wal.append(b"saved vote")?;
            wal.append(b"saved lock")?;
            wal.flush()?;
        }
        let committed = fs::read(&path)?;
        let mut writer = OpenOptions::new().append(true).open(&path)?;
        writer.write_all(&tail)?;
        writer.sync_all()?;
        drop(writer);

        for _ in 0..2 {
            let mut reopened = open(&path).expect("recover the incomplete, uncommitted append");
            let entries = reopened.iter()?.collect::<io::Result<Vec<_>>>()?;
            assert_eq!(entries, [b"saved vote".to_vec(), b"saved lock".to_vec()]);
            assert_eq!(fs::read(&path)?, committed);
            let evidence = fs::read_dir(dir.path())?
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("consensus.wal.torn-")
                })
                .map(|entry| fs::read(entry.path()))
                .collect::<io::Result<Vec<_>>>()?;
            assert_eq!(evidence, std::slice::from_ref(&tail));
        }
    }
    Ok(())
}

#[test]
fn rejects_an_in_bound_corrupt_length_in_committed_history() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"saved vote")?;
    wal.append(b"saved lock")?;
    wal.flush()?;
    drop(wal);

    let mut writer = OpenOptions::new().write(true).open(&path)?;
    writer.seek(SeekFrom::Start(13))?;
    writer.write_all(&4096_u64.to_be_bytes())?;
    drop(writer);
    let corrupted = fs::read(&path)?;
    assert_eq!(open(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::read(&path)?, corrupted);
    Ok(())
}

#[test]
fn keeps_valid_uncommitted_records_and_seals_them_before_replay() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"first")?;
    wal.flush()?;
    let old_end = fs::metadata(&path)?.len();
    wal.append(b"second")?;
    drop(wal);

    let mut reopened = open(&path)?;
    assert_eq!(
        reopened.iter()?.collect::<io::Result<Vec<_>>>()?,
        [b"first".to_vec(), b"second".to_vec()]
    );
    drop(reopened);
    OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(old_end)?;
    assert_eq!(open(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::metadata(&path)?.len(), old_end);
    Ok(())
}

#[test]
fn missing_managed_checkpoint_blocks_restart() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"signed vote")?;
    wal.flush()?;
    drop(wal);

    fs::remove_file(dir.path().join("consensus.wal.checkpoint"))?;
    let original = fs::read(&path)?;
    assert_eq!(open(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::read(&path)?, original);
    assert!(!dir.path().join("consensus.wal.checkpoint").exists());
    Ok(())
}

#[test]
fn refuses_short_legacy_frame_without_creating_a_checkpoint() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = Log::open(&path)?;
    wal.append(b"historic vote")?;
    wal.flush()?;
    drop(wal);
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(&[0])?;

    let before = fs::read(&path)?;
    assert_eq!(
        open(&path).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(fs::read(&path)?, before);
    assert!(!dir.path().join("consensus.wal.checkpoint").exists());
    Ok(())
}

#[test]
fn refuses_a_full_bad_frame_even_after_the_checkpoint() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"committed")?;
    wal.flush()?;
    let second_frame = fs::metadata(&path)?.len();
    wal.append(b"uncommitted")?;
    drop(wal);

    let mut writer = OpenOptions::new().read(true).write(true).open(&path)?;
    writer.seek(SeekFrom::Start(second_frame + 9))?;
    writer.write_all(&0_u32.to_be_bytes())?;
    drop(writer);
    let before = fs::read(&path)?;
    assert_eq!(open(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::read(&path)?, before);
    Ok(())
}

#[test]
fn refuses_an_oversized_length_beyond_the_checkpoint() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"saved vote")?;
    wal.flush()?;
    drop(wal);

    // An in-range u64 that exceeds the 1 GiB frame limit must not count as a torn append.
    let mut writer = OpenOptions::new().append(true).open(&path)?;
    writer.write_all(&[0])?;
    writer.write_all(&((1_u64 << 30) + 1).to_be_bytes())?;
    writer.write_all(&[0; 4])?;
    drop(writer);
    let before = fs::read(&path)?;

    assert_eq!(open(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::read(&path)?, before);
    assert_eq!(fs::read_dir(dir.path())?.count(), 3);
    Ok(())
}

#[test]
fn failed_checkpoint_update_cannot_acknowledge_flush() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let mut wal = open(&path)?;
    wal.append(b"sealed")?;
    wal.flush()?;
    let checkpoint = dir.path().join("consensus.wal.checkpoint");
    let before = fs::read(&checkpoint)?;
    let obstructed = dir.path().join("consensus.wal.checkpoint.tmp");
    fs::create_dir(&obstructed)?;
    wal.append(b"unacknowledged")?;
    assert!(wal.flush().is_err());
    assert!(wal.append(b"must not advance").is_err());
    assert_eq!(fs::read(checkpoint)?, before);
    drop(wal);
    fs::remove_dir(obstructed)?;

    assert_eq!(
        open(&path)?.iter()?.collect::<io::Result<Vec<_>>>()?,
        [b"sealed".to_vec(), b"unacknowledged".to_vec()]
    );
    Ok(())
}

#[test]
fn finishes_a_fresh_header_after_interrupted_activation() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    fs::write(dir.path().join("consensus.wal.managed"), b"RWALINIT")?;
    fs::write(&path, [0, 0, 0, 1, 0])?;

    let mut wal = open(&path)?;
    assert_eq!(wal.sequence(), 0);
    wal.append(b"first")?;
    wal.flush()?;
    drop(wal);
    assert_eq!(
        open(&path)?.iter()?.collect::<io::Result<Vec<_>>>()?,
        [b"first".to_vec()]
    );
    Ok(())
}
