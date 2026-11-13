use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use arc_malachitebft_test::codec::proto::ProtobufCodec;
use arc_malachitebft_test::{Address, Height, Signature, TestContext, Value, ValueId, Vote};
use bytes::Bytes;
use eyre::{bail, eyre, Result};
use malachitebft_app::engine::wal::{Msg, Wal, WalEntry};
use malachitebft_app::metrics::SharedRegistry;
use malachitebft_core_consensus::{ProposedValue, SignedConsensusMsg};
use malachitebft_core_types::{NilOrVal, Round, SignedVote, Timeout, TimeoutKind, Validity};
use serde::Serialize;

const MESSAGE: &[u8] = b"wal-compatibility-v1";

#[derive(Debug, Serialize)]
struct Manifest {
    source_commit: String,
    entry_kinds: Vec<String>,
    height: u64,
    round: u32,
    value_id: u64,
    wal_byte_length: u64,
    sha256: String,
}

fn fixture_entries() -> Vec<WalEntry<TestContext>> {
    let height = Height::new(7);
    let round = Round::new(2);
    let address = Address::new([0x22; 20]);
    let value_id = ValueId::new(7002);

    let vote = Vote::new_prevote(height, round, NilOrVal::Val(value_id), address);
    let signed_vote = SignedVote::new(vote, Signature::from_bytes([0x33; 64]));
    let proposed_value = ProposedValue {
        height,
        round,
        valid_round: Round::Nil,
        proposer: address,
        value: Value {
            value: value_id.as_u64(),
            extensions: Bytes::from_static(MESSAGE),
        },
        validity: Validity::Valid,
    };

    vec![
        WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(signed_vote)),
        WalEntry::Timeout(Timeout::new(round, TimeoutKind::Prevote)),
        WalEntry::ProposedValue(proposed_value),
    ]
}

async fn write_wal(path: PathBuf) -> Result<()> {
    let ctx = TestContext::new();
    let wal = Wal::spawn(
        &ctx,
        ProtobufCodec,
        path,
        SharedRegistry::global().clone(),
        tracing::Span::none(),
    )
    .await?;

    wal.call(|reply| Msg::StartedHeight(Height::new(7), reply), None)
        .await?
        .success_or(eyre!("WAL StartedHeight call failed"))??;

    for entry in fixture_entries() {
        wal.call(|reply| Msg::Append(Height::new(7), entry, reply), None)
            .await?
            .success_or(eyre!("WAL Append call failed"))??;
    }

    wal.call(Msg::Flush, None)
        .await?
        .success_or(eyre!("WAL Flush call failed"))??;
    wal.stop_and_wait(None, None).await?;

    Ok(())
}

fn sha256(path: &Path) -> Result<String> {
    let output = Command::new("shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()?;
    if !output.status.success() {
        bail!("shasum failed with status {}", output.status);
    }

    let stdout = String::from_utf8(output.stdout)?;
    stdout
        .split_whitespace()
        .next()
        .map(str::to_owned)
        .ok_or_else(|| eyre!("shasum returned no checksum"))
}

fn write_fixture(path: &Path, source_commit: &str) -> Result<Manifest> {
    if source_commit.len() != 40 || !source_commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("source commit must be a full 40-character hexadecimal SHA");
    }

    fs::create_dir_all(path)?;
    let wal_path = path.join("wal.log");
    if wal_path.exists() {
        fs::remove_file(&wal_path)?;
    }

    tokio::runtime::Runtime::new()?.block_on(write_wal(wal_path.clone()))?;

    Ok(Manifest {
        source_commit: source_commit.to_owned(),
        entry_kinds: ["vote", "timeout", "proposed_value"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        height: 7,
        round: 2,
        value_id: 7002,
        wal_byte_length: fs::metadata(&wal_path)?.len(),
        sha256: sha256(&wal_path)?,
    })
}

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| eyre!("usage: wal_compatibility_fixture OUTPUT_DIR SOURCE_COMMIT"))?;
    let source_commit = args
        .next()
        .ok_or_else(|| eyre!("usage: wal_compatibility_fixture OUTPUT_DIR SOURCE_COMMIT"))?
        .into_string()
        .map_err(|_| eyre!("source commit is not valid UTF-8"))?;
    if args.next().is_some() {
        bail!("usage: wal_compatibility_fixture OUTPUT_DIR SOURCE_COMMIT");
    }

    let manifest = write_fixture(&path, &source_commit)?;
    let mut json = serde_json::to_string_pretty(&manifest)?;
    json.push('\n');
    fs::write(path.join("manifest.json"), json)?;

    Ok(())
}
