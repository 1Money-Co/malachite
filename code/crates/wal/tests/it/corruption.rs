use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::LazyLock;

use testdir::{NumberedDir, NumberedDirBuilder};

use arc_malachitebft_wal::ext::*;
use arc_malachitebft_wal::log::constants::*;
use arc_malachitebft_wal::{Log, Version};

static TESTDIR: LazyLock<NumberedDir> =
    LazyLock::new(|| NumberedDirBuilder::new("wal".to_string()).create().unwrap());

macro_rules! testwal {
    ($e:expr) => {{
        let module_path = ::std::module_path!();
        let test_name = ::testdir::private::extract_test_name(&module_path);
        let subdir_path = ::std::path::Path::new(&module_path.replace("::", "/")).join(&test_name);
        TESTDIR
            .create_subdir(subdir_path)
            .unwrap()
            .join(format!("wal{}.log", $e))
    }};
    () => {{
        testwal!("")
    }};
}

#[test]
fn corrupted_crc() -> io::Result<()> {
    let path = testwal!();

    // Write initial entries
    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.append(b"entry2")?;
        wal.flush()?;
    }

    // Corrupt the CRC of the second entry
    {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Skip version (4 bytes) + sequence (8 bytes) + first entry
        file.seek(SeekFrom::Start(12))?;
        read_u8(&mut file)?; // Skip compression flag
        let first_entry_len = read_u64(&mut file)?;
        file.seek(SeekFrom::Current(first_entry_len as i64 + 4))?; // +4 for CRC

        // Now at the start of second entry, skip compression flag and length
        file.seek(SeekFrom::Current(1 + 8))?;

        // Write incorrect CRC
        write_u32(&mut file, 0xdeadbeef)?;
    }

    // Reopen and verify
    {
        let mut wal = Log::open(&path)?;
        let mut entries = wal.iter()?;

        // First entry should be readable
        assert!(entries.next().is_some());

        // Second entry should fail CRC check
        match entries.next() {
            Some(Err(e)) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            _ => panic!("Expected CRC error for corrupted entry"),
        }
    }

    Ok(())
}

#[test]
fn incomplete_entries() -> io::Result<()> {
    let path = testwal!();

    // Write initial entries
    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.append(b"entry2")?;
        wal.flush()?;
    }

    // Truncate file in the middle of the second entry
    {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Skip header
        file.seek(SeekFrom::Start(12))?;

        read_u8(&mut file)?; // Skip compression flag
        let first_entry_len = read_u64(&mut file)?;

        // header + compression flag + length + CRC + data + partial second entry
        let truncate_pos = 12 + 1 + 8 + 4 + first_entry_len + 3;

        // Seek to middle of second entry
        file.set_len(truncate_pos)?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::UnexpectedEof);

    Ok(())
}

#[test]
fn invalid_version() -> io::Result<()> {
    let path = testwal!();

    // Create WAL file with invalid version
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;

        // Write invalid version
        write_u32(&mut file, 0xFFFFFFFF)?;
        write_u64(&mut file, 0)?; // sequence
    }

    // Attempt to open WAL
    match Log::open(&path) {
        Err(e) => {
            assert_eq!(e.kind(), io::ErrorKind::InvalidData);
            // Verify error message contains version information
            assert!(e.to_string().contains("version"));
        }
        Ok(_) => panic!("Expected error when opening WAL with invalid version"),
    }

    Ok(())
}

#[test]
fn invalid_sequence() -> io::Result<()> {
    let path = testwal!();

    // Create WAL with valid version but corrupted sequence
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;

        write_u32(&mut file, Version::V1 as u32)?;

        // Write partial/corrupted sequence number
        file.write_all(&[0xFF, 0xFF])?; // Only write 2 bytes instead of 8
    }

    // Attempt to open WAL
    match Log::open(&path) {
        Err(e) => {
            assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
        }
        Ok(_) => panic!("Expected error when opening WAL with invalid sequence"),
    }

    Ok(())
}

#[test]
fn multiple_corruptions() -> io::Result<()> {
    let path = testwal!();

    // Create initial WAL with entries
    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.append(b"entry2")?;
        wal.append(b"entry3")?;
        wal.flush()?;
    }

    // Introduce multiple types of corruption
    {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Corrupt sequence number
        file.seek(SeekFrom::Start(4))?;
        write_u64(&mut file, u64::MAX)?;

        // Corrupt entry length
        file.seek(SeekFrom::Start(12 + 1))?;
        write_u64(&mut file, u64::MAX - 1)?;

        // Corrupt CRC of another entry
        file.seek(SeekFrom::Start(50 + 1))?;
        write_u32(&mut file, 0xdeadbeef)?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::InvalidData);

    Ok(())
}

#[test]
fn zero_length_entries() -> io::Result<()> {
    let path = testwal!();

    // Create WAL with zero-length entry
    {
        let mut wal = Log::open(&path)?;
        wal.append(b"")?;
        wal.append(b"normal entry")?;
        wal.flush()?;
    }

    // Verify reading
    {
        let mut wal = Log::open(&path)?;
        let entries: Vec<_> = wal.iter()?.collect::<Result<Vec<_>, _>>()?;

        assert_eq!(entries.len(), 2);
        assert_eq!(&entries[0], b"");
        assert_eq!(&entries[1], b"normal entry");
    }

    Ok(())
}

#[test]
#[ignore]
fn max_entry_size() -> io::Result<()> {
    let path = testwal!();

    let mut wal = Log::open(&path)?;

    // Try to write an entry that's too large
    let large_entry = vec![0u8; usize::MAX / 2];
    assert!(wal.append(&large_entry).is_err());

    // Verify WAL is still usable
    wal.append(b"normal entry")?;

    let entries: Vec<_> = wal.iter()?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(entries.len(), 1);
    assert_eq!(&entries[0], b"normal entry");

    Ok(())
}

/// This test creates a scenario where a valid entry's length field is corrupted
/// with a value that is invalid, but small enough to fool the original,
/// flawed recovery logic.
///
/// The old logic would incorrectly validate this entry and fail to truncate
/// the log, leading to a corrupt state. The corrected logic must identify
/// that the full entry cannot fit and truncate the file correctly.
#[test]
fn plausible_corrupted_length_fails_without_mutating_wal() -> io::Result<()> {
    let path = testwal!();

    let second_entry_start_pos;

    // Write two valid entries
    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;

        // Position after entry 1 is the start of entry 2
        second_entry_start_pos = wal.size_bytes()?;

        wal.append(b"entry2")?;
        wal.flush()?;
    }

    // The total remaining space for the second entry is (total_size - second_entry_start_pos).
    // Let's say this is 19 bytes.
    // We will corrupt the length to a value that is *less than* this, to fool the old check.
    // The actual data + crc for entry2 is 6+4=10 bytes. The header is 13 bytes.
    // Let's corrupt the data_length to 15. The old code would calculate an
    // entry_length of 15+4=19. The check `19 < 19` would be false, and it would
    // incorrectly validate the entry.
    let malicious_data_length = 15u64;

    // Manually corrupt the length field of the *second* entry.
    {
        let mut file = OpenOptions::new().write(true).open(&path)?;
        // Seek to the start of the second entry, then skip the compression flag (1 byte)
        file.seek(SeekFrom::Start(second_entry_start_pos + 1))?;
        // Overwrite the 8-byte length with our malicious value.
        write_u64(&mut file, malicious_data_length)?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::UnexpectedEof);

    Ok(())
}

#[test]
fn oversized_corrupted_length_fails_without_mutating_wal() -> io::Result<()> {
    let path = testwal!();

    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.flush()?;
    }

    {
        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.seek(SeekFrom::Start(
            FIRST_ENTRY_OFFSET + ENTRY_COMPRESSION_FLAG_SIZE,
        ))?;
        write_u64(&mut file, u64::MAX)?;
        file.sync_all()?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::InvalidData);

    Ok(())
}

#[test]
fn partial_entry_header_fails_without_mutating_wal() -> io::Result<()> {
    let path = testwal!();

    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.flush()?;
    }

    {
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&[0, 0, 0])?;
        file.sync_all()?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::UnexpectedEof);

    Ok(())
}

#[test]
fn incomplete_entry_payload_fails_without_mutating_wal() -> io::Result<()> {
    let path = testwal!();

    {
        let mut wal = Log::open(&path)?;
        wal.append(b"entry1")?;
        wal.flush()?;
    }

    {
        let mut file = OpenOptions::new().append(true).open(&path)?;
        write_u8(&mut file, 0)?;
        write_u64(&mut file, 4)?;
        write_u32(&mut file, 0)?;
        file.write_all(&[1, 2])?;
        file.sync_all()?;
    }

    assert_open_fails_without_mutation(&path, io::ErrorKind::UnexpectedEof);

    Ok(())
}

fn assert_open_fails_without_mutation(path: &Path, expected_kind: io::ErrorKind) {
    let expected = fs::read(path).expect("read WAL before failed open");

    for attempt in 1..=2 {
        let error = Log::open(path).expect_err("corrupt WAL must fail to open");
        assert_eq!(error.kind(), expected_kind, "failed open attempt {attempt}");
        assert_eq!(
            fs::read(path).expect("read WAL after failed open"),
            expected,
            "failed open attempt {attempt} mutated WAL bytes"
        );
    }
}

#[test]
fn truncate_on_corruption() {
    for idx in 0..10 {
        eprintln!("Testing truncation at index: {idx}");
        corrupt_and_truncate_at(idx).unwrap();
    }
}

fn corrupt_and_truncate_at(idx: usize) -> io::Result<()> {
    let path = testwal!(idx);
    let entry_count = 5;

    // Setup a valid WAL with LEN entries
    setup_valid_wal(&path, entry_count)?;

    // Corrupt the CRC of entry `idx`
    if idx < entry_count {
        corrupt_wal_entry_crc(&path, idx)?;
    }

    // Reopen WAL and iterate entries
    let mut wal = Log::open(&path)?;

    // Verify WAL state before truncation
    verify_wal_state_pre_fix(&mut wal, idx, entry_count);

    // Truncate the WAL to the valid entries
    wal.truncate(idx as u64)?;

    // Verify WAL recovery
    let expected_len = idx.min(entry_count);
    verify_wal_recovery(&mut wal, expected_len)?;

    Ok(())
}

/// Sets up a valid WAL file with the specified number of entries.
fn setup_valid_wal(path: &Path, count: usize) -> io::Result<()> {
    let mut wal = Log::open(path)?;
    for i in 0..count {
        wal.append(format!("entry{i}").as_bytes())?;
    }
    wal.flush()
}

/// Navigates the file structure to corrupt a specific CRC.
/// This encapsulates the "fragile" byte-logic in one place.
fn corrupt_wal_entry_crc(path: &Path, idx: usize) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    // Header offset (Version + Sequence)
    file.seek(SeekFrom::Start(HEADER_SIZE))?;

    for _ in 0..idx {
        read_u8(&mut file)?; // skip compression flag
        let entry_len = read_u64(&mut file)?;
        read_u32(&mut file)?; // skip CRC
        file.seek(SeekFrom::Current(entry_len as i64))?; // skip entry data
    }

    // Skip compression flag and length to reach the CRC
    read_u8(&mut file)?;
    read_u64(&mut file)?;

    // Corrupt the CRC
    write_u32(&mut file, 0xDEADBEEF)?;

    // Write changes to disk
    file.sync_all()
}

/// Verifies the WAL iterator behavior before the fix.
fn verify_wal_state_pre_fix(wal: &mut Log, corrupt_idx: usize, total: usize) {
    let results: Vec<_> = wal.iter().unwrap().collect();
    let expected_len = (corrupt_idx + 1).min(total);

    assert_eq!(
        results.len(),
        expected_len,
        "Iterator stopped at wrong point"
    );

    if corrupt_idx < total {
        assert!(
            results[corrupt_idx].is_err(),
            "Expected CRC error at index {corrupt_idx}"
        );
    }
}

/// Verifies that the WAL contains the expected number of valid entries.
fn verify_wal_recovery(wal: &mut Log, expected_len: usize) -> io::Result<()> {
    assert_eq!(wal.len(), expected_len);

    let entries: Vec<Vec<u8>> = wal.iter()?.collect::<Result<_, _>>()?;
    assert_eq!(entries.len(), expected_len);

    for (i, data) in entries.iter().enumerate() {
        assert_eq!(data, format!("entry{i}").as_bytes());
    }
    Ok(())
}
