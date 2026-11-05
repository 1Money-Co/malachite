use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use eyre::{bail, eyre, Result};
use informalsystems_malachitebft_test::codec::proto::ProtobufCodec;
use informalsystems_malachitebft_test::utils::validators::make_validators;
use informalsystems_malachitebft_test::{
    Address, Height, Signature, TestContext, ValidatorSet, ValueId, Vote,
};
use malachitebft_app::engine::wal::{log_entries, Msg, Wal, WalEntry};
use malachitebft_app::metrics::{Metrics, SharedRegistry};
use malachitebft_app::wal::Log;
use malachitebft_core_consensus as consensus;
use malachitebft_core_consensus::{
    Effect, Input, Params, ProposedValue, Resumable, Resume, SignedConsensusMsg, State,
};
use malachitebft_core_types::{
    NilOrVal, Round, SignedMessage, Timeout, TimeoutKind, Validity, ValueOrigin, ValuePayload,
    Vote as _,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const PRODUCTION_SOURCE: &str = "bcac2b2dbd369a6c29b9151fcd0889b133d7221a";
const CURRENT_SOURCE: &str = "eb2405ba78fc2d5c4465652364299f3217428a36";

const PRODUCTION_WAL: &[u8] = include_bytes!("../fixtures/wal/bcac2b2d/wal.log");
const PRODUCTION_MANIFEST: &str = include_str!("../fixtures/wal/bcac2b2d/manifest.json");
const CURRENT_WAL: &[u8] = include_bytes!("../fixtures/wal/eb2405ba/wal.log");
const CURRENT_MANIFEST: &str = include_str!("../fixtures/wal/eb2405ba/manifest.json");

#[derive(Debug, Deserialize)]
struct Manifest {
    source_commit: String,
    entry_kinds: Vec<String>,
    height: u64,
    round: u32,
    value_id: u64,
    wal_byte_length: u64,
    sha256: String,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn materialize_fixture(directory: &Path, bytes: &[u8], name: &str) -> Result<PathBuf> {
    let path = directory.join(name);
    fs::write(&path, bytes)?;
    Ok(path)
}

fn read_fixture(path: &Path) -> Result<Vec<WalEntry<TestContext>>> {
    let original_length = fs::metadata(path)?.len();
    let mut log = Log::open(path)?;
    let opened_length = fs::metadata(path)?.len();
    if opened_length != original_length {
        bail!("opening the WAL discarded an invalid suffix");
    }

    log_entries(&mut log, &ProtobufCodec)?.collect()
}

async fn rewrite_fixture(path: PathBuf, entries: Vec<WalEntry<TestContext>>) -> Result<()> {
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
    for entry in entries {
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

fn assert_entries(entries: &[WalEntry<TestContext>]) {
    assert_eq!(entries.len(), 3);

    let WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(vote)) = &entries[0] else {
        panic!("first entry is not a signed vote");
    };
    assert_eq!(vote.height(), Height::new(7));
    assert_eq!(vote.round(), Round::new(2));
    assert_eq!(vote.value(), &NilOrVal::Val(ValueId::new(7002)));
    assert_eq!(vote.validator_address(), &Address::new([0x22; 20]));
    assert_eq!(vote.signature.to_bytes(), [0x33; 64]);

    let WalEntry::Timeout(timeout) = &entries[1] else {
        panic!("second entry is not a timeout");
    };
    assert_eq!(timeout, &Timeout::new(Round::new(2), TimeoutKind::Prevote));

    let WalEntry::ProposedValue(ProposedValue {
        height,
        round,
        valid_round,
        proposer,
        value,
        validity,
    }) = &entries[2]
    else {
        panic!("third entry is not a proposed value");
    };
    assert_eq!(height, &Height::new(7));
    assert_eq!(round, &Round::new(2));
    assert_eq!(valid_round, &Round::Nil);
    assert_eq!(proposer, &Address::new([0x22; 20]));
    assert_eq!(value.value, 7002);
    assert_eq!(
        value.extensions,
        Bytes::from_static(b"wal-compatibility-v1")
    );
    assert_eq!(validity, &Validity::Valid);
}

fn process_replay_input(
    state: &mut State<TestContext>,
    input: Input<TestContext>,
    metrics: &Metrics,
    signed_votes: &mut Vec<Vote>,
) {
    let processed: Result<(), consensus::Error<TestContext>> = consensus::process!(
        input: input,
        state: state,
        metrics: metrics,
        with: effect => {
            #[allow(unreachable_patterns)]
            let result: Result<Resume<TestContext>, consensus::Error<TestContext>> = match effect {
                Effect::SignVote(vote, resume) => {
                    signed_votes.push(vote.clone());
                    Ok(resume.resume_with(SignedMessage::new(vote, Signature::test())))
                }
                Effect::SignProposal(proposal, resume) => Ok(resume.resume_with(
                    SignedMessage::new(proposal, Signature::test()),
                )),
                Effect::VerifySignature(_, _, resume) => Ok(resume.resume_with(true)),
                Effect::ExtendVote(_, _, _, resume) => Ok(resume.resume_with(None)),
                Effect::VerifyVoteExtension(_, _, _, _, _, resume) => {
                    Ok(resume.resume_with(Ok(())))
                }
                Effect::VerifyCommitCertificate(_, _, _, resume)
                | Effect::VerifyPolkaCertificate(_, _, _, resume)
                | Effect::VerifyRoundCertificate(_, _, _, resume) => {
                    Ok(resume.resume_with(Ok(())))
                }
                Effect::ResetTimeouts(resume)
                | Effect::CancelAllTimeouts(resume)
                | Effect::PublishConsensusMsg(_, resume)
                | Effect::PublishLivenessMsg(_, resume)
                | Effect::RepublishVote(_, resume)
                | Effect::RepublishRoundCertificate(_, resume)
                | Effect::WalAppend(_, _, resume) => Ok(resume.resume_with(())),
                Effect::CancelTimeout(_, resume)
                | Effect::ScheduleTimeout(_, resume) => Ok(resume.resume_with(())),
                Effect::StartRound(_, _, _, _, resume) => Ok(resume.resume_with(())),
                Effect::GetValue(_, _, _, resume) => Ok(resume.resume_with(())),
                Effect::RestreamProposal(_, _, _, _, _, resume) => Ok(resume.resume_with(())),
                Effect::ValidSyncValue(_, _, resume) => Ok(resume.resume_with(())),
                Effect::InvalidSyncValue(_, _, _, resume) => Ok(resume.resume_with(())),
                Effect::Decide(_, _, resume) => Ok(resume.resume_with(())),
                other => panic!("unexpected replay effect: {other:?}"),
            };
            result
        }
    );
    processed.expect("process replayed WAL input");
}

#[test]
fn production_wal_fixture_restores_historical_vote() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let fixture_path = materialize_fixture(directory.path(), PRODUCTION_WAL, "fixture.log")?;
    let entries = read_fixture(&fixture_path)?;
    assert_entries(&entries);

    let WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(historical_vote)) = &entries[0] else {
        panic!("first entry is not a signed vote");
    };
    let historical_signed_vote = historical_vote.clone();
    let historical_vote = historical_signed_vote.message.clone();

    let [(mut validator, _private_key)] = make_validators([1]);
    validator.address = historical_vote.validator_address;
    let validator_set = ValidatorSet::new([validator]);
    let params = Params {
        initial_height: Height::new(historical_vote.height.as_u64() - 1),
        initial_validator_set: validator_set.clone(),
        address: historical_vote.validator_address,
        threshold_params: Default::default(),
        value_payload: ValuePayload::ProposalAndParts,
        enabled: true,
    };
    let mut state = State::new(TestContext::new(), params, 32);
    let metrics = Metrics::default();
    let mut signed_votes = Vec::new();

    process_replay_input(
        &mut state,
        Input::StartHeight(historical_vote.height, validator_set, false),
        &metrics,
        &mut signed_votes,
    );
    for entry in entries {
        let input = match entry {
            WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(vote)) => Input::Vote(vote),
            WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(proposal)) => {
                Input::Proposal(proposal)
            }
            WalEntry::Timeout(timeout) => Input::TimeoutElapsed(timeout),
            WalEntry::ProposedValue(value) => Input::ProposedValue(value, ValueOrigin::Consensus),
        };
        process_replay_input(&mut state, input, &metrics, &mut signed_votes);
    }

    assert_eq!(state.height(), historical_vote.height);
    assert!(
        state.round() >= historical_vote.round,
        "fixture replay must advance consensus through the historical round"
    );
    let round_votes = state
        .driver
        .votes()
        .per_round(historical_vote.round)
        .expect("fixture replay must create the historical round in the vote keeper");
    assert!(
        round_votes
            .received_votes()
            .iter()
            .any(|vote| vote == &historical_signed_vote),
        "fixture replay must restore the exact signed historical vote"
    );
    assert!(
        signed_votes.is_empty(),
        "this fixture restores a vote but does not contain enough inputs to trigger signing"
    );

    Ok(())
}

async fn assert_fixture(bytes: &[u8], manifest_json: &str, expected_source: &str) -> Result<()> {
    let manifest: Manifest = serde_json::from_str(manifest_json)?;
    assert_eq!(manifest.source_commit, expected_source);
    assert_eq!(manifest.entry_kinds, ["vote", "timeout", "proposed_value"]);
    assert_eq!(manifest.height, 7);
    assert_eq!(manifest.round, 2);
    assert_eq!(manifest.value_id, 7002);
    assert_eq!(manifest.wal_byte_length, bytes.len() as u64);
    assert_eq!(manifest.sha256, sha256(bytes));

    let directory = tempfile::tempdir()?;
    let fixture_path = materialize_fixture(directory.path(), bytes, "fixture.log")?;
    let entries = read_fixture(&fixture_path)?;
    assert_entries(&entries);

    let rewritten_path = directory.path().join("rewritten.log");
    rewrite_fixture(rewritten_path.clone(), entries).await?;
    let rewritten = fs::read(rewritten_path)?;
    assert_eq!(sha256(&rewritten), manifest.sha256);

    Ok(())
}

#[tokio::test]
async fn production_wal_fixture_decodes_and_rewrites_canonically() -> Result<()> {
    assert_fixture(PRODUCTION_WAL, PRODUCTION_MANIFEST, PRODUCTION_SOURCE).await
}

#[tokio::test]
async fn current_fork_wal_fixture_decodes_and_rewrites_canonically() -> Result<()> {
    assert_fixture(CURRENT_WAL, CURRENT_MANIFEST, CURRENT_SOURCE).await
}

#[tokio::test]
async fn repeated_started_height_returns_no_replay_entries() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let wal = Wal::spawn(
        &TestContext::new(),
        ProtobufCodec,
        directory.path().join("wal.log"),
        SharedRegistry::global().clone(),
        tracing::Span::none(),
    )
    .await?;

    let first = wal
        .call(|reply| Msg::StartedHeight(Height::new(7), reply), None)
        .await?
        .success_or(eyre!("first WAL StartedHeight call failed"))??;
    assert!(first.is_none());

    let fixture_path = materialize_fixture(directory.path(), PRODUCTION_WAL, "fixture.log")?;
    let entry = read_fixture(&fixture_path)?
        .into_iter()
        .next()
        .expect("production fixture must contain an entry");
    wal.call(|reply| Msg::Append(Height::new(7), entry, reply), None)
        .await?
        .success_or(eyre!("WAL Append call failed"))??;
    wal.call(Msg::Flush, None)
        .await?
        .success_or(eyre!("WAL Flush call failed"))??;

    let repeated = wal
        .call(|reply| Msg::StartedHeight(Height::new(7), reply), None)
        .await?
        .success_or(eyre!("repeated WAL StartedHeight call failed"))??;
    assert!(repeated.is_none());

    wal.stop_and_wait(None, None).await?;
    Ok(())
}

#[test]
fn truncated_production_wal_fails_without_returning_a_prefix() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut truncated = PRODUCTION_WAL.to_vec();
    truncated.pop().expect("production fixture is not empty");
    let path = materialize_fixture(directory.path(), &truncated, "truncated.log")?;
    assert!(read_fixture(&path).is_err());
    Ok(())
}

#[test]
fn corrupt_production_wal_fails_without_returning_a_prefix() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut corrupt = PRODUCTION_WAL.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    let path = materialize_fixture(directory.path(), &corrupt, "corrupt.log")?;
    assert!(read_fixture(&path).is_err());
    Ok(())
}
