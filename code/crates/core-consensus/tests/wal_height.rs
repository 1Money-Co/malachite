use arc_malachitebft_core_consensus as consensus;
use malachitebft_core_types::{
    NilOrVal, Round, SignedMessage, SignedProposal, SignedVote, Timeout, TimeoutKind, Validity,
    ValueOrigin, ValuePayload,
};
use malachitebft_test::utils::validators::make_validators;
use malachitebft_test::{
    Height, Proposal, Signature, TestContext, ValidatorSet, Value, ValueId, Vote,
};

use consensus::{Effect, Input, Params, ProposedValue, Resumable, Resume, State, WalEntry};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryKind {
    Proposal,
    Vote,
    ProposedValue,
    PolkaCertificate,
    Timeout,
}

fn process(
    state: &mut State<TestContext>,
    input: Input<TestContext>,
    appends: &mut Vec<(Height, EntryKind)>,
) {
    let metrics = Default::default();
    let processed: Result<(), consensus::Error<TestContext>> = consensus::process!(
        input: input,
        state: state,
        metrics: &metrics,
        with: effect => {
            #[allow(unreachable_patterns)]
            let result: Result<Resume<TestContext>, consensus::Error<TestContext>> = match effect {
                Effect::WalAppend(height, entry, resume) => {
                    let kind = match &entry {
                        WalEntry::ConsensusMsg(consensus::SignedConsensusMsg::Proposal(proposal)) => {
                            assert_eq!(height, proposal.message.height);
                            EntryKind::Proposal
                        }
                        WalEntry::ConsensusMsg(consensus::SignedConsensusMsg::Vote(vote)) => {
                            assert_eq!(height, vote.message.height);
                            EntryKind::Vote
                        }
                        WalEntry::ProposedValue(value) => {
                            assert_eq!(height, value.height);
                            EntryKind::ProposedValue
                        }
                        WalEntry::PolkaCertificate(_) => EntryKind::PolkaCertificate,
                        WalEntry::Timeout(_) => EntryKind::Timeout,
                    };
                    appends.push((height, kind));
                    Ok(resume.resume_with(()))
                }
                Effect::VerifySignature(_, _, resume) => Ok(resume.resume_with(true)),
                Effect::SignVote(vote, resume) => Ok(resume.resume_with(SignedVote::new(
                    vote,
                    Signature::test(),
                ))),
                Effect::SignProposal(proposal, resume) => Ok(resume.resume_with(
                    SignedProposal::new(proposal, Signature::test()),
                )),
                Effect::ExtendVote(_, _, _, resume) => Ok(resume.resume_with(None)),
                Effect::VerifyVoteExtension(_, _, _, _, _, resume) => {
                    Ok(resume.resume_with(Ok(())))
                }
                Effect::VerifyCommitCertificate(_, _, _, resume)
                | Effect::VerifyPolkaCertificate(_, _, _, resume)
                | Effect::VerifyRoundCertificate(_, _, _, resume) => {
                    Ok(resume.resume_with(Ok(())))
                }
                Effect::CancelAllTimeouts(resume)
                | Effect::PublishConsensusMsg(_, resume)
                | Effect::PublishLivenessMsg(_, resume)
                | Effect::RepublishVote(_, resume)
                | Effect::RepublishRoundCertificate(_, resume) => Ok(resume.resume_with(())),
                Effect::CancelTimeout(_, resume)
                | Effect::ScheduleTimeout(_, resume) => Ok(resume.resume_with(())),
                Effect::StartRound(_, _, _, _, resume) => Ok(resume.resume_with(())),
                Effect::GetValue(_, _, _, resume) => Ok(resume.resume_with(())),
                Effect::RestreamProposal(_, _, _, _, _, resume) => Ok(resume.resume_with(())),
                Effect::ValidSyncValue(_, _, resume) => Ok(resume.resume_with(())),
                Effect::InvalidSyncValue(_, _, _, resume) => Ok(resume.resume_with(())),
                Effect::Decide(_, _, resume) => Ok(resume.resume_with(())),
                other => panic!("unexpected effect: {other:?}"),
            };
            result
        }
    );
    processed.expect("process consensus input");
}

#[test]
fn wal_appends_use_entry_height_across_start_height_transition() {
    let [(validator, _private_key)] = make_validators([1]);
    let address = validator.address;
    let validator_set = ValidatorSet::new([validator]);
    let params = Params {
        address,
        threshold_params: Default::default(),
        value_payload: ValuePayload::ProposalAndParts,
        enabled: true,
    };
    let mut state = State::new(
        TestContext::new(),
        Height::new(6),
        validator_set.clone(),
        params,
        32,
        32,
    );
    let mut appends = Vec::new();

    let height = Height::new(7);
    let round = Round::new(0);
    let value = Value::new(7002);
    let proposal = Proposal::new(height, round, value.clone(), Round::Nil, address);
    let vote = Vote::new_prevote(height, round, NilOrVal::Val(ValueId::new(7002)), address);
    let proposed_value = ProposedValue {
        height,
        round,
        valid_round: Round::Nil,
        proposer: address,
        value: Value::new(7003),
        validity: Validity::Valid,
    };

    process(
        &mut state,
        Input::Proposal(SignedMessage::new(proposal, Signature::test())),
        &mut appends,
    );
    process(
        &mut state,
        Input::Vote(SignedMessage::new(vote, Signature::test())),
        &mut appends,
    );
    process(
        &mut state,
        Input::ProposedValue(proposed_value, ValueOrigin::Consensus),
        &mut appends,
    );
    assert!(
        appends.is_empty(),
        "future-height inputs must remain buffered"
    );

    process(
        &mut state,
        Input::StartHeight(height, validator_set, false, None),
        &mut appends,
    );
    process(
        &mut state,
        Input::TimeoutElapsed(Timeout::new(round, TimeoutKind::Prevote)),
        &mut appends,
    );

    assert!(appends
        .iter()
        .all(|(append_height, _)| *append_height == height));
    assert!(appends.iter().any(|(_, kind)| *kind == EntryKind::Proposal));
    assert!(appends.iter().any(|(_, kind)| *kind == EntryKind::Vote));
    assert!(appends
        .iter()
        .any(|(_, kind)| *kind == EntryKind::ProposedValue));
    assert!(appends.iter().any(|(_, kind)| *kind == EntryKind::Timeout));
}
