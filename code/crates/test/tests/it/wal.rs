use std::time::Duration;

use eyre::bail;
use tracing::info;

use arc_malachitebft_test::{self as malachitebft_test};

use malachitebft_codec::Codec;
use malachitebft_config::ValuePayload;
use malachitebft_core_consensus::{LocallyProposedValue, WalEntry};
use malachitebft_core_types::SignedVote;
use malachitebft_engine::util::events::Event;
use malachitebft_test::codec::proto::ProtobufCodec;
use malachitebft_test::TestContext;

use crate::middlewares::{ByzantineProposer, PrevoteNil};
use crate::{HandlerResult, TestBuilder, TestParams};

#[tokio::test]
async fn proposer_crashes_after_proposing_parts_only() {
    proposer_crashes_after_proposing(TestParams {
        value_payload: ValuePayload::PartsOnly,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
async fn proposer_crashes_after_proposing_proposal_and_parts() {
    proposer_crashes_after_proposing(TestParams {
        value_payload: ValuePayload::ProposalAndParts,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
#[ignore]
async fn proposer_crashes_after_proposing_proposal_only() {
    proposer_crashes_after_proposing(TestParams {
        value_payload: ValuePayload::ProposalOnly,
        ..TestParams::default()
    })
    .await
}

async fn proposer_crashes_after_proposing(params: TestParams) {
    #[derive(Clone, Debug, Default)]
    struct State {
        first_proposed_value: Option<LocallyProposedValue<TestContext>>,
    }

    const CRASH_HEIGHT: u64 = 3;

    let mut test = TestBuilder::<State>::new();

    test.add_node().with_voting_power(10).start().success();
    test.add_node().with_voting_power(10).start().success();

    test.add_node()
        .with_voting_power(40)
        .start()
        .wait_until(CRASH_HEIGHT)
        // Wait until this node proposes a value
        .on_event(|event, state| match event {
            Event::ProposedValue(value) => {
                info!("Proposer proposed block: {:?}", value.value);
                state.first_proposed_value = Some(value);
                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        // Crash right after
        .crash()
        // Restart after 5 seconds
        .restart_after(Duration::from_secs(5))
        // Check that we replay messages from the WAL
        .expect_wal_replay(CRASH_HEIGHT)
        // Wait until it proposes a value again, while replaying WAL
        // Check that it is the same value as the first time
        .on_proposed_value(|value, state| {
            let Some(first_value) = state.first_proposed_value.as_ref() else {
                bail!("Proposer did not propose a block");
            };

            if first_value.value == value.value {
                info!("Proposer re-proposed the same block: {:?}", value.value);
                Ok(HandlerResult::ContinueTest)
            } else {
                bail!(
                    "Proposer just equivocated: expected {:?}, got {:?}",
                    first_value.value,
                    value.value
                );
            }
        })
        .success();

    test.build()
        .run_with_params(
            Duration::from_secs(60),
            TestParams {
                enable_value_sync: false,
                ..params
            },
        )
        .await
}

#[tokio::test]
#[ignore] // NOTE: To re-enable once #997 is merged
async fn non_proposer_crashes_after_voting_parts_only() {
    non_proposer_crashes_after_voting(TestParams {
        value_payload: ValuePayload::PartsOnly,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
#[ignore] // NOTE: To re-enable once #997 is merged
async fn non_proposer_crashes_after_voting_proposal_and_parts() {
    non_proposer_crashes_after_voting(TestParams {
        value_payload: ValuePayload::ProposalAndParts,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
#[ignore] // NOTE: The test application does not support proposal-only mode yet
async fn non_proposer_crashes_after_voting_proposal_only() {
    non_proposer_crashes_after_voting(TestParams {
        value_payload: ValuePayload::ProposalOnly,
        ..TestParams::default()
    })
    .await
}

async fn non_proposer_crashes_after_voting(params: TestParams) {
    #[derive(Clone, Debug, Default)]
    struct State {
        first_vote: Option<SignedVote<TestContext>>,
    }

    const CRASH_HEIGHT: u64 = 2;

    let mut test = TestBuilder::<State>::new();

    test.add_node()
        .with_voting_power(40)
        .start()
        .wait_until(CRASH_HEIGHT)
        // Wait until this node proposes a value
        .on_vote(|vote, state| {
            info!("Non-proposer voted");
            state.first_vote = Some(vote);

            Ok(HandlerResult::ContinueTest)
        })
        // Crash right after
        .crash()
        // Restart after 5 seconds
        .restart_after(Duration::from_secs(5))
        // Check that we replay messages from the WAL
        .expect_wal_replay(CRASH_HEIGHT)
        // Wait until the previous vote is replayed
        // Check that it is the for the same value as the first time
        .on_vote(|vote, state| {
            let Some(first_vote) = state.first_vote.as_ref() else {
                bail!("Non-proposer did not vote");
            };

            if first_vote.value == vote.value {
                info!("Non-proposer voted the same way: {first_vote:?}");
                Ok(HandlerResult::ContinueTest)
            } else {
                bail!(
                    "Non-proposer just equivocated: expected {:?}, got {:?}",
                    first_vote.value,
                    vote.value
                );
            }
        })
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.add_node().with_voting_power(10).start().success();
    test.add_node().with_voting_power(10).start().success();

    test.build()
        .run_with_params(
            Duration::from_secs(60),
            TestParams {
                enable_value_sync: false,
                ..params
            },
        )
        .await
}

#[tokio::test]
#[ignore]
async fn restart_with_byzantine_proposer_1_parts_only() {
    byzantine_proposer_crashes_after_proposing_1(TestParams {
        value_payload: ValuePayload::PartsOnly,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
#[ignore]
async fn restart_with_byzantine_proposer_1_proposal_and_parts() {
    byzantine_proposer_crashes_after_proposing_1(TestParams {
        value_payload: ValuePayload::ProposalAndParts,
        ..TestParams::default()
    })
    .await
}

async fn byzantine_proposer_crashes_after_proposing_1(params: TestParams) {
    #[derive(Clone, Debug, Default)]
    struct State {
        first_proposed_value: Option<LocallyProposedValue<TestContext>>,
    }

    const CRASH_HEIGHT: u64 = 3;

    let mut test = TestBuilder::<State>::new();

    test.add_node()
        .with_voting_power(10)
        .start()
        .wait_until(CRASH_HEIGHT)
        .crash()
        .restart_after(Duration::from_secs(5))
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.add_node()
        .with_voting_power(10)
        .start()
        .wait_until(CRASH_HEIGHT)
        .crash()
        .restart_after(Duration::from_secs(5))
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.add_node()
        .with_voting_power(10)
        .with_middleware(ByzantineProposer)
        .start()
        .wait_until(CRASH_HEIGHT)
        // Wait until this node proposes a value
        .on_event(|event, state| match event {
            Event::ProposedValue(value) => {
                info!("Proposer proposed block: {:?}", value.value);
                state.first_proposed_value = Some(value);
                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        // Crash right after
        .crash()
        // Restart after 5 seconds
        .restart_after(Duration::from_secs(5))
        // Check that we replay messages from the WAL
        .expect_wal_replay(CRASH_HEIGHT)
        // Wait until it proposes a value again, while replaying WAL
        .on_proposed_value(|value, state| {
            let Some(first_value) = state.first_proposed_value.as_ref() else {
                bail!("Proposer did not propose a block");
            };

            if first_value.value == value.value {
                bail!(
                    "Byzantine proposer unexpectedly re-proposed the same value: {:?}",
                    value.value
                );
            }

            info!(
                "As per the test, the proposer just equivocated: expected {:?}, got {:?}",
                first_value.value, value.value
            );

            Ok(HandlerResult::ContinueTest)
        })
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.build()
        .run_with_params(
            Duration::from_secs(60),
            TestParams {
                enable_value_sync: true,
                ..params
            },
        )
        .await
}

#[tokio::test]
#[ignore]
async fn restart_with_byzantine_proposer_2_parts_only() {
    byzantine_proposer_crashes_after_proposing_2(TestParams {
        value_payload: ValuePayload::PartsOnly,
        ..TestParams::default()
    })
    .await
}

#[tokio::test]
#[ignore]
async fn restart_with_byzantine_proposer_2_proposal_and_parts() {
    byzantine_proposer_crashes_after_proposing_2(TestParams {
        value_payload: ValuePayload::ProposalAndParts,
        ..TestParams::default()
    })
    .await
}

async fn byzantine_proposer_crashes_after_proposing_2(params: TestParams) {
    #[derive(Clone, Debug, Default)]
    struct State {
        first_proposed_value: Option<LocallyProposedValue<TestContext>>,
        first_vote: Option<SignedVote<TestContext>>,
    }

    const CRASH_HEIGHT: u64 = 3;

    let mut test = TestBuilder::<State>::new();
    test.add_node()
        .with_voting_power(10)
        .start()
        .wait_until(CRASH_HEIGHT)
        .crash()
        .restart_after(Duration::from_secs(6))
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.add_node()
        .with_voting_power(10)
        .start()
        .wait_until(CRASH_HEIGHT)
        .on_vote(|vote, state| {
            info!("Non-proposer voted");
            state.first_vote = Some(vote);

            Ok(HandlerResult::ContinueTest)
        })
        // Crash right after
        .crash()
        .restart_after(Duration::from_secs(5))
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.add_node()
        .with_voting_power(10)
        .with_middleware(ByzantineProposer)
        .start()
        .wait_until(CRASH_HEIGHT)
        // Wait until this node proposes a value
        .on_event(|event, state| match event {
            Event::ProposedValue(value) => {
                info!("Proposer proposed block: {:?}", value.value);
                state.first_proposed_value = Some(value);

                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        // Crash right after
        .crash()
        // Restart after 5 seconds
        .restart_after(Duration::from_secs(5))
        // Check that we replay messages from the WAL
        .expect_wal_replay(CRASH_HEIGHT)
        // Wait until it proposes a value again, while replaying WAL
        .on_proposed_value(|value, state| {
            let Some(first_value) = state.first_proposed_value.as_ref() else {
                bail!("Proposer did not propose a block");
            };

            if first_value.value == value.value {
                bail!(
                    "Byzantine proposer unexpectedly re-proposed the same value: {:?}",
                    value.value
                );
            }

            info!(
                "As per the test, the proposer just equivocated: expected {:?}, got {:?}",
                first_value.value, value.value
            );

            Ok(HandlerResult::ContinueTest)
        })
        .wait_until(CRASH_HEIGHT + 2)
        .success();

    test.build()
        .run_with_params(Duration::from_secs(60), params)
        .await
}

async fn test_multi_rounds(crash_height: u64, restart_after: Duration) {
    let crash_round: u32 = 3;
    let final_height: u64 = crash_height + 2;

    let mut test = TestBuilder::<()>::new();

    test.add_node()
        .with_middleware(PrevoteNil::when(move |height, round, _| {
            height.as_u64() == crash_height && round.as_u32() <= Some(crash_round)
        }))
        .start()
        .wait_until(crash_height)
        .wait_until_round(crash_round)
        .crash()
        .restart_after(restart_after)
        .expect_wal_replay(crash_height)
        .wait_until(final_height)
        .success();

    test.add_node().start().wait_until(final_height).success();
    test.add_node().start().wait_until(final_height).success();

    test.build()
        .run_with_params(
            Duration::from_secs(90),
            TestParams {
                enable_value_sync: false,
                ..TestParams::default()
            },
        )
        .await
}

#[tokio::test]
async fn multi_rounds_1() {
    test_multi_rounds(1, Duration::from_secs(30)).await
}

#[tokio::test]
async fn multi_rounds_2() {
    test_multi_rounds(3, Duration::from_secs(10)).await
}

#[tokio::test]
async fn non_empty_wal_replays_before_sync_recovery() {
    #[derive(Clone, Debug, Default)]
    struct State {
        recorded_signed_bytes: Option<Vec<u8>>,
    }

    const CRASH_HEIGHT: u64 = 3;
    const FINAL_HEIGHT: u64 = CRASH_HEIGHT + 2;

    let mut test = TestBuilder::<State>::new();

    test.add_node()
        .start()
        .wait_until(CRASH_HEIGHT)
        // Capture the exact signed bytes that must survive the crash.
        .on_event(|event, state| match event {
            Event::Published(msg) if msg.height().as_u64() == CRASH_HEIGHT => {
                state.recorded_signed_bytes = Some(ProtobufCodec.encode(&msg)?.to_vec());
                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .crash()
        // While the validator is down, the other three keep making progress.
        .restart_after(Duration::from_secs(10))
        // Replay must begin before a sync certificate can decide the crash height.
        .on_event(move |event, _| match event {
            Event::WalReplayBegin(height, count) if height.as_u64() == CRASH_HEIGHT => {
                if count == 0 {
                    bail!("WAL replay at crash height contained no entries");
                }
                Ok(HandlerResult::ContinueTest)
            }
            Event::WalReplayBegin(height, _) => {
                bail!("Unexpected WAL replay at height {height}, expected {CRASH_HEIGHT}")
            }
            Event::Decided { commit_certificate }
                if commit_certificate.height.as_u64() == CRASH_HEIGHT =>
            {
                bail!("Sync decided the crash height before WAL replay began")
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        // The replayed signed message must be byte-identical to the one recorded before the crash.
        .on_event(|event, state| match event {
            Event::WalReplayEntry(WalEntry::ConsensusMsg(msg)) => {
                let replayed = ProtobufCodec.encode(&msg)?.to_vec();
                let recorded = state
                    .recorded_signed_bytes
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("missing pre-crash signed bytes"))?;
                if replayed == *recorded {
                    Ok(HandlerResult::ContinueTest)
                } else {
                    Ok(HandlerResult::WaitForNextEvent)
                }
            }
            Event::WalReplayDone(height) => {
                bail!("WAL replay at height {height} contained no signed consensus entry")
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .wait_until(FINAL_HEIGHT)
        .success();

    test.add_node().start().wait_until(FINAL_HEIGHT).success();
    test.add_node().start().wait_until(FINAL_HEIGHT).success();
    test.add_node().start().wait_until(FINAL_HEIGHT).success();

    test.build()
        .run_with_params(
            Duration::from_secs(90),
            TestParams {
                enable_value_sync: true,
                ..TestParams::default()
            },
        )
        .await
}

#[tokio::test]
async fn sync_certificate_cannot_skip_recorded_wal() {
    const CRASH_HEIGHT: u64 = 3;
    const FINAL_HEIGHT: u64 = CRASH_HEIGHT + 2;

    let mut test = TestBuilder::<()>::new();

    test.add_node()
        .start()
        .wait_until(CRASH_HEIGHT)
        // Wait for an `Event::Published` so the WAL is not empty when crashing.
        .on_event(|event, _| match event {
            Event::Published(_) => Ok(HandlerResult::ContinueTest),
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .crash()
        // While the validator is down, the other three keep making progress.
        .restart_after(Duration::from_secs(10))
        .on_event(move |event, _| match event {
            Event::WalReplayBegin(height, _) if height.as_u64() == CRASH_HEIGHT => {
                Ok(HandlerResult::ContinueTest)
            }
            Event::WalReplayBegin(height, _count) => {
                bail!("Unexpected WAL replay at height {height}, expected {CRASH_HEIGHT}");
            }
            Event::Decided { commit_certificate }
                if commit_certificate.height.as_u64() == CRASH_HEIGHT =>
            {
                bail!("Sync certificate skipped the recorded WAL at the crash height")
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .wait_until(FINAL_HEIGHT)
        .success();

    test.add_node().start().wait_until(FINAL_HEIGHT).success();
    test.add_node().start().wait_until(FINAL_HEIGHT).success();
    test.add_node().start().wait_until(FINAL_HEIGHT).success();

    test.build()
        .run_with_params(
            Duration::from_secs(90),
            TestParams {
                enable_value_sync: true,
                ..TestParams::default()
            },
        )
        .await
}

#[tokio::test]
async fn empty_wal_and_full_node_start_without_replay() {
    const FINAL_HEIGHT: u64 = 2;

    let mut test = TestBuilder::<()>::new();

    test.add_node()
        .start()
        .on_event(|event, _| match event {
            Event::WalReplayBegin(height, count) => {
                bail!("empty validator WAL replayed {count} entries at height {height}")
            }
            Event::StartedRound(height, _, _, _) if height.as_u64() == 1 => {
                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .wait_until(FINAL_HEIGHT)
        .success();

    test.add_node().start().wait_until(FINAL_HEIGHT).success();
    test.add_node().start().wait_until(FINAL_HEIGHT).success();

    test.add_node()
        .full_node()
        .start()
        .on_event(|event, _| match event {
            Event::WalReplayBegin(height, count) => {
                bail!("full node WAL replayed {count} entries at height {height}")
            }
            Event::StartedRound(height, _, _, _) if height.as_u64() == 1 => {
                Ok(HandlerResult::ContinueTest)
            }
            _ => Ok(HandlerResult::WaitForNextEvent),
        })
        .wait_until(FINAL_HEIGHT)
        .success();

    test.build()
        .run_with_params(
            Duration::from_secs(60),
            TestParams {
                enable_value_sync: true,
                ..TestParams::default()
            },
        )
        .await
}
