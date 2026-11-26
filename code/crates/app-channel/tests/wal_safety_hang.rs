use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_malachitebft_app_channel::app::config::NodeConfig;
use arc_malachitebft_app_channel::app::consensus::{
    LocallyProposedValue, ProposedValue, SignedConsensusMsg,
};
use arc_malachitebft_app_channel::app::engine::consensus::Msg as ConsensusActorMsg;
use arc_malachitebft_app_channel::app::engine::host::{HeightParams, HostMsg};
use arc_malachitebft_app_channel::app::engine::network::{Msg as EngineNetworkMsg, NetworkEvent};
use arc_malachitebft_app_channel::app::engine::util::events::Event;
use arc_malachitebft_app_channel::app::engine::wal::{
    encode_entry, log_entries, Msg as WalMsg, WalEntry,
};
use arc_malachitebft_app_channel::app::metrics;
use arc_malachitebft_app_channel::app::types::core::{
    Context, NilOrVal, Round, RoundCertificate, RoundCertificateType, SignedMessage,
    SignedProposal, SignedVote, Timeout, ValidatorProof, Validity, ValueOrigin, VoteType,
};
use arc_malachitebft_app_channel::app::types::PeerId;
use arc_malachitebft_app_channel::app::wal::Log;
use arc_malachitebft_app_channel::{
    AppMsg, ConsensusContext, EngineBuilder, NetworkMsg, RequestContext, WalContext,
};
use malachitebft_config::{ConsensusConfig, ValuePayload, ValueSyncConfig};
use malachitebft_signing::{Error as SignError, Signer};
use malachitebft_test::codec::proto::ProtobufCodec;
use malachitebft_test::utils::validators::make_validators;
use malachitebft_test::{
    Address, Ed25519Signer, Ed25519Verifier, Height, LinearTimeouts, PrivateKey, Proposal,
    TestContext, ValidatorSet, Value, ValueId, Vote,
};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::{mpsc, oneshot, Notify};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Append,
    AppendCall,
    Fetch,
    Flush(usize),
    FlushBeforeFinalize,
    FlushBeforeRestream,
    Observe,
    Replay,
    Reset,
    BufferedReset,
    Registration,
}

struct RecordedAppend {
    entry: WalEntry<TestContext>,
    prior_flushes: usize,
}

impl RecordedAppend {
    fn is_reproposal(&self) -> bool {
        matches!(
            &self.entry,
            WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(proposal))
                if proposal.message.pol_round.is_defined()
        )
    }
}

#[derive(Clone)]
struct FaultProbe {
    fault: Fault,
    append_calls: Arc<AtomicUsize>,
    flush_calls: Arc<AtomicUsize>,
    reached: Arc<Notify>,
    appends: Arc<Mutex<Vec<RecordedAppend>>>,
}

impl FaultProbe {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            append_calls: Arc::new(AtomicUsize::new(0)),
            flush_calls: Arc::new(AtomicUsize::new(0)),
            reached: Arc::new(Notify::new()),
            appends: Arc::default(),
        }
    }
}

struct FaultingWal;

#[ractor::async_trait]
impl Actor for FaultingWal {
    type Msg = WalMsg<TestContext>;
    type State = FaultProbe;
    type Arguments = FaultProbe;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        probe: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(probe)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        probe: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            WalMsg::StartedHeight(_, reply) => match probe.fault {
                Fault::Fetch => {
                    probe.reached.notify_one();
                    let _ = reply.send(Err(eyre::eyre!("injected WAL fetch failure")));
                }
                Fault::Reset => {
                    probe.reached.notify_one();
                    let _ = reply.send(Err(eyre::eyre!("injected WAL forward-reset failure")));
                }
                Fault::Replay => {
                    probe.reached.notify_one();
                    let _ = reply.send(Ok(vec![
                        Ok(WalEntry::Timeout(Timeout::propose(Round::new(0)))),
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "injected corrupt replay entry",
                        )),
                    ]));
                }
                _ => {
                    let _ = reply.send(Ok(Vec::new()));
                }
            },
            WalMsg::Reset(_, reply) => {
                if matches!(probe.fault, Fault::Reset | Fault::BufferedReset) {
                    probe.reached.notify_one();
                    let _ = reply.send(Err(eyre::eyre!("injected WAL reset failure")));
                } else {
                    let _ = reply.send(Ok(()));
                }
            }
            WalMsg::Append(_, entry, reply) => {
                probe.append_calls.fetch_add(1, Ordering::SeqCst);
                match probe.fault {
                    Fault::Append => {
                        probe.reached.notify_one();
                        let _ = reply.send(Err(eyre::eyre!("injected WAL append failure")));
                    }
                    Fault::AppendCall => {
                        probe.reached.notify_one();
                        myself.stop(Some("injected WAL actor loss".to_owned()));
                    }
                    Fault::Observe | Fault::FlushBeforeRestream => {
                        probe.appends.lock().unwrap().push(RecordedAppend {
                            entry,
                            prior_flushes: probe.flush_calls.load(Ordering::SeqCst),
                        });
                        let _ = reply.send(Ok(()));
                    }
                    Fault::Fetch
                    | Fault::Flush(_)
                    | Fault::FlushBeforeFinalize
                    | Fault::Replay
                    | Fault::Reset
                    | Fault::BufferedReset
                    | Fault::Registration => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            WalMsg::Flush(reply) => {
                let call = probe.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let fail_at = match probe.fault {
                    Fault::Flush(fail_at) => Some(fail_at),
                    // In the single-validator proposal path the first two flushes precede
                    // publication and Decide. The next one gates immediate Finalize.
                    Fault::FlushBeforeFinalize => Some(3),
                    // SignProposal flushes the new entry. Fail the next flush, which
                    // should be the barrier before delivering Restream to the host.
                    Fault::FlushBeforeRestream => probe
                        .appends
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|append| append.is_reproposal())
                        .map(|append| append.prior_flushes + 2),
                    _ => None,
                };
                if fail_at == Some(call) {
                    let appends = probe.append_calls.load(Ordering::SeqCst);
                    probe.reached.notify_one();
                    let _ = reply.send(Err(eyre::eyre!(
                        "injected WAL flush failure on call {call} after {appends} appends"
                    )));
                } else {
                    let _ = reply.send(Ok(()));
                }
            }
            WalMsg::LivenessProbe(reply) => {
                if probe.fault == Fault::Registration {
                    probe.reached.notify_one();
                    myself.stop(Some("injected WAL loss during child commit".to_owned()));
                } else {
                    let _ = reply.send(());
                }
            }
            WalMsg::Dump => {}
        }

        Ok(())
    }
}

struct NetworkSink;

#[ractor::async_trait]
impl Actor for NetworkSink {
    type Msg = EngineNetworkMsg<TestContext>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let EngineNetworkMsg::Subscribe(subscriber) = msg {
            subscriber.send(NetworkEvent::Listening(
                "/memory/1".parse().expect("valid memory multiaddr"),
            ));
        }
        Ok(())
    }
}

struct Config {
    moniker: String,
    consensus: ConsensusConfig,
    value_sync: ValueSyncConfig,
}

impl NodeConfig for Config {
    fn moniker(&self) -> &str {
        &self.moniker
    }

    fn consensus(&self) -> &ConsensusConfig {
        &self.consensus
    }

    fn consensus_mut(&mut self) -> &mut ConsensusConfig {
        &mut self.consensus
    }

    fn value_sync(&self) -> &ValueSyncConfig {
        &self.value_sync
    }

    fn value_sync_mut(&mut self) -> &mut ValueSyncConfig {
        &mut self.value_sync
    }
}

fn reject_decisions(msg: &AppMsg<TestContext>) {
    assert!(
        !matches!(msg, AppMsg::Decided { .. } | AppMsg::Finalized { .. }),
        "consensus decided after injected WAL failure"
    );
}

async fn drive_application(
    mut rx: mpsc::Receiver<AppMsg<TestContext>>,
    validators: ValidatorSet,
    mut inspect: impl FnMut(&AppMsg<TestContext>),
) {
    while let Some(msg) = rx.recv().await {
        inspect(&msg);
        match msg {
            AppMsg::ConsensusReady { reply } => {
                let params = HeightParams::new(validators.clone(), LinearTimeouts::default(), None);
                let _ = reply.send((Height::new(1), params));
            }
            AppMsg::StartedRound { reply_value, .. } => {
                let _ = reply_value.send(Vec::new());
            }
            AppMsg::GetValue {
                height,
                round,
                reply,
                ..
            } => {
                let _ = reply.send(LocallyProposedValue::new(height, round, Value::new(1)));
            }
            AppMsg::ExtendVote { reply, .. } => {
                let _ = reply.send(None);
            }
            AppMsg::VerifyVoteExtension { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
            AppMsg::Decided { reply, .. } => {
                let _ = reply.send(());
            }
            AppMsg::GetHistoryMinHeight { reply } => {
                let _ = reply.send(Height::new(1));
            }
            _ => {}
        }
    }
}

async fn run_fault_scenario(fault: Fault) {
    run_fault_scenario_with_quorum(fault, true, None).await;
}

async fn run_fault_scenario_with_quorum(
    fault: Fault,
    single_validator_quorum: bool,
    expected_appends: Option<usize>,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("arc_malachitebft=trace")
        .with_test_writer()
        .try_init();
    let moniker = format!("wal-safety-{fault:?}").to_lowercase();
    let [(validator, private_key), (other_validator, _)] = make_validators([1, 1]);
    let mut validator_entries = vec![validator.clone()];
    if !single_validator_quorum {
        validator_entries.push(other_validator);
    }
    let validators = ValidatorSet::new(validator_entries);
    let probe = FaultProbe::new(fault);
    let (wal, _) = Actor::spawn(None, FaultingWal, probe.clone())
        .await
        .expect("spawn faulting WAL");
    let (network, _) = Actor::spawn(None, NetworkSink, ())
        .await
        .expect("spawn network sink");
    let (tx_network, _rx_network) = mpsc::channel::<NetworkMsg<TestContext>>(1);
    let consensus = ConsensusConfig {
        value_payload: ValuePayload::ProposalOnly,
        ..ConsensusConfig::default()
    };
    let config = Config {
        moniker: moniker.clone(),
        consensus,
        value_sync: ValueSyncConfig::default(),
    };

    let (channels, mut engine) = EngineBuilder::new(TestContext::default(), config)
        .with_custom_wal(wal.clone())
        .with_custom_network(network.clone(), tx_network)
        .with_no_sync()
        .with_default_consensus(ConsensusContext::new_validator(
            validator.address,
            Box::new(Ed25519Verifier),
            Box::new(Ed25519Signer::new(private_key)),
        ))
        .with_default_request(RequestContext::new(1))
        .build()
        .await
        .expect("build engine");
    let mut events = channels.events.subscribe();
    let expect_decision = fault == Fault::FlushBeforeFinalize;
    let mut app = tokio::spawn(drive_application(
        channels.consensus,
        validators,
        move |msg| {
            if !expect_decision || !matches!(msg, AppMsg::Decided { .. }) {
                reject_decisions(msg);
            }
        },
    ));

    tokio::time::timeout(Duration::from_secs(2), probe.reached.notified())
        .await
        .expect("consensus did not reach injected WAL fault");
    wal.wait(Some(Duration::from_secs(2)))
        .await
        .expect("node did not stop WAL child");
    network
        .wait(Some(Duration::from_secs(2)))
        .await
        .expect("node did not stop network child");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut app)
            .await
            .is_err(),
        "application connector stopped and exposed the safety hang as a process failure"
    );

    let mut decisions = 0;
    while let Ok(event) = events.try_recv() {
        if expect_decision && matches!(event, Event::Decided { .. }) {
            decisions += 1;
            continue;
        }
        assert!(
            !matches!(
                event,
                Event::Published(_)
                    | Event::Decided { .. }
                    | Event::Finalized { .. }
                    | Event::WalReplayEntry(_)
            ),
            "consensus effect escaped after WAL fault: {event}"
        );
        assert!(
            !matches!(event, Event::WalWriteError(_)),
            "runtime WAL fault must not request process exit"
        );
    }
    assert_eq!(decisions, usize::from(expect_decision));

    let mut exported = String::new();
    metrics::export(&mut exported);
    let expected_metric =
        format!("malachitebft_core_consensus_node_safety_failure{{moniker=\"{moniker}\"}} 1");
    assert!(
        exported.lines().any(|line| line == expected_metric),
        "safety gauge was not set:\n{exported}"
    );
    assert!(
        !engine.handle.is_finished(),
        "engine supervisor exited instead of safety-hanging"
    );

    match fault {
        Fault::Append | Fault::AppendCall => {
            assert_eq!(probe.append_calls.load(Ordering::SeqCst), 1)
        }
        Fault::Flush(fail_at) => {
            assert_eq!(probe.flush_calls.load(Ordering::SeqCst), fail_at)
        }
        Fault::FlushBeforeFinalize => assert_eq!(probe.flush_calls.load(Ordering::SeqCst), 3),
        Fault::Replay => {}
        Fault::Fetch | Fault::Reset | Fault::BufferedReset => {
            unreachable!("startup failure uses its own scenario")
        }
        Fault::Registration => unreachable!("registration failure uses its own scenario"),
        Fault::Observe | Fault::FlushBeforeRestream => {
            unreachable!("reproposal uses its own scenario")
        }
    }
    if let Some(expected_appends) = expected_appends {
        assert_eq!(probe.append_calls.load(Ordering::SeqCst), expected_appends);
    }

    engine.actor.stop(None);
    tokio::time::timeout(Duration::from_secs(2), &mut engine.handle)
        .await
        .expect("engine cleanup timeout")
        .expect("join engine cleanup");
    tokio::time::timeout(Duration::from_secs(2), app)
        .await
        .expect("application connector did not stop during explicit cleanup")
        .expect("application driver panicked");
}

#[tokio::test]
async fn append_failure_enters_safety_hang_before_publish_or_decide() {
    run_fault_scenario(Fault::Append).await;
}

#[tokio::test]
async fn append_call_failure_enters_safety_hang_before_publish_or_decide() {
    run_fault_scenario(Fault::AppendCall).await;
}

#[tokio::test]
async fn flush_failure_enters_safety_hang_before_publish_or_decide() {
    run_fault_scenario(Fault::Flush(1)).await;
}

#[tokio::test]
async fn second_flush_failure_stops_before_publish() {
    run_fault_scenario_with_quorum(Fault::Flush(2), false, Some(3)).await;
}

#[tokio::test]
async fn second_flush_failure_stops_before_decide() {
    run_fault_scenario_with_quorum(Fault::Flush(2), true, Some(4)).await;
}

#[tokio::test]
async fn flush_failure_after_decide_stops_before_finalize() {
    run_fault_scenario_with_quorum(Fault::FlushBeforeFinalize, true, Some(4)).await;
}

#[tokio::test]
async fn corrupt_replay_enters_supervisor_safety_hang() {
    run_fault_scenario(Fault::Replay).await;
}

struct ReproposalRun {
    appends: Vec<RecordedAppend>,
    certificate_votes: Vec<SignedVote<TestContext>>,
    expected_reproposal: SignedProposal<TestContext>,
}

async fn run_reproposal(fault: Fault, rebuilt_vote_type: VoteType) -> eyre::Result<ReproposalRun> {
    let height = Height::new(1);
    let locked_round = Round::new(0);
    let reproposal_round = Round::new(1);
    let value = Value::new(17);
    let value_id = value.id();
    let ctx = TestContext::default();
    let keys = make_validators([1; 4]);
    let validators = ValidatorSet::new(keys.iter().map(|(validator, _)| validator.clone()));
    let proposer = ctx
        .select_proposer(&validators, height, locked_round)
        .address;
    let local = ctx
        .select_proposer(&validators, height, reproposal_round)
        .address;
    let (_, private_key) = keys
        .iter()
        .find(|(validator, _)| validator.address == local)
        .unwrap();
    let expected_reproposal = Ed25519Signer::new(private_key.clone())
        .sign_proposal(Proposal::new(
            height,
            reproposal_round,
            value.clone(),
            locked_round,
            local,
        ))
        .await?;

    let probe = FaultProbe::new(fault);
    let (wal, _) = Actor::spawn(None, FaultingWal, probe.clone()).await?;
    let (network, _) = Actor::spawn(None, NetworkSink, ()).await?;
    let (tx_network, _rx_network) = mpsc::channel::<NetworkMsg<TestContext>>(1);
    let config = Config {
        moniker: format!("wal-reproposal-{fault:?}").to_lowercase(),
        consensus: ConsensusConfig {
            // Ordinary proposals in PartsOnly mode have no signed proposal WAL entry.
            value_payload: ValuePayload::PartsOnly,
            ..ConsensusConfig::default()
        },
        value_sync: ValueSyncConfig::default(),
    };
    let (channels, engine) = EngineBuilder::new(ctx, config)
        .with_custom_wal(wal.clone())
        .with_custom_network(network, tx_network)
        .with_no_sync()
        .with_default_consensus(ConsensusContext::new_validator(
            local,
            Box::new(Ed25519Verifier),
            Box::new(Ed25519Signer::new(private_key.clone())),
        ))
        .with_default_request(RequestContext::new(1))
        .build()
        .await?;
    let children = engine.actor.get_cell().get_children();
    let consensus: ActorRef<ConsensusActorMsg<TestContext>> = children
        .iter()
        .find(|child| child.is_message_type_of::<ConsensusActorMsg<TestContext>>() == Some(true))
        .expect("engine supervises consensus")
        .clone()
        .into();
    let mut events = channels.events.subscribe();
    let (restream_tx, mut restream_rx) = oneshot::channel();
    let mut restream_tx = Some(restream_tx);
    let app = tokio::spawn(drive_application(
        channels.consensus,
        validators,
        move |msg| {
            reject_decisions(msg);
            if let AppMsg::RestreamProposal {
                height: h,
                round,
                valid_round,
                address,
                value_id: id,
            } = msg
            {
                assert_eq!(
                    (*h, *round, *valid_round, *address, *id),
                    (height, reproposal_round, locked_round, local, value_id)
                );
                restream_tx
                    .take()
                    .expect("one restream")
                    .send(())
                    .expect("restream observer");
            }
        },
    ));

    let wait = Duration::from_secs(3);
    tokio::time::timeout(wait, async {
        loop {
            if matches!(events.recv().await.unwrap(), Event::StartedRound(h, r, _, _) if h == height && r == locked_round)
            {
                break;
            }
        }
    })
    .await
    .expect("start the round with the original proposal");
    consensus
        .cast(ConsensusActorMsg::ReceivedProposedValue(
            ProposedValue {
                height,
                round: locked_round,
                valid_round: Round::Nil,
                proposer,
                value,
                validity: Validity::Valid,
            },
            ValueOrigin::Consensus,
        ))
        .expect("deliver the original proposal");

    let mut certificate_votes = Vec::new();
    let peer = PeerId::random();
    for ((validator, key), vote_type) in keys
        .iter()
        .filter(|(validator, _)| validator.address != local)
        .zip([VoteType::Prevote, rebuilt_vote_type])
    {
        let signer = Ed25519Signer::new(key.clone());
        let vote = signer
            .sign_vote(Vote::new_prevote(
                height,
                locked_round,
                NilOrVal::Val(value_id),
                validator.address,
            ))
            .await?;
        consensus
            .cast(ConsensusActorMsg::NetworkEvent(NetworkEvent::Vote(
                peer, vote,
            )))
            .expect("deliver a peer prevote for the original value");
        let make_vote = match vote_type {
            VoteType::Prevote => Vote::new_prevote,
            VoteType::Precommit => Vote::new_precommit,
        };
        certificate_votes.push(
            signer
                .sign_vote(make_vote(
                    height,
                    reproposal_round,
                    NilOrVal::Nil,
                    validator.address,
                ))
                .await?,
        );
    }
    tokio::time::timeout(wait, async {
        loop {
            if let Event::Published(SignedConsensusMsg::Vote(vote)) = events.recv().await.unwrap() {
                if vote.message.typ == VoteType::Precommit
                    && vote.message.value == NilOrVal::Val(value_id)
                {
                    break;
                }
            }
        }
    })
    .await
    .expect("the validator locks the original value");

    // The first future-round vote arrives as ordinary gossip. The second arrives only in
    // the verified certificate and triggers the round change that re-proposes the value.
    consensus
        .cast(ConsensusActorMsg::NetworkEvent(NetworkEvent::Vote(
            peer,
            certificate_votes[0].clone(),
        )))
        .expect("deliver the already known certificate vote");
    let certificate = RoundCertificate::new_from_votes(
        height,
        reproposal_round,
        RoundCertificateType::Skip,
        certificate_votes.clone(),
    );
    consensus
        .cast(ConsensusActorMsg::NetworkEvent(
            NetworkEvent::RoundCertificate(peer, certificate),
        ))
        .expect("deliver the signed round certificate");

    if fault == Fault::FlushBeforeRestream {
        tokio::time::timeout(wait, async {
            tokio::select! {
                _ = probe.reached.notified() => {}
                result = &mut restream_rx => {
                    panic!("restream escaped before the WAL flush failure: {result:?}");
                }
            }
        })
        .await
        .expect("reach the flush immediately before Restream");
        wal.wait(Some(wait))
            .await
            .expect("safety hang stops the WAL actor");
        let host: ActorRef<HostMsg<TestContext>> = children
            .into_iter()
            .find(|child| child.is_message_type_of::<HostMsg<TestContext>>() == Some(true))
            .expect("engine supervises the application connector")
            .into();
        // The connector survives the safety hang. Its reply is a mailbox barrier for
        // any Restream message that consensus could have queued before the WAL error.
        tokio::time::timeout(wait, async {
            ractor::call!(host, |reply_to| HostMsg::GetHistoryMinHeight { reply_to })
        })
        .await
        .expect("application connector answers after the safety hang")
        .expect("query application connector");
        assert!(
            matches!(
                restream_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "restream escaped after the WAL flush failure"
        );
    } else {
        tokio::time::timeout(wait, restream_rx)
            .await
            .expect("re-proposal reaches Restream")
            .expect("receive Restream");
    }

    tokio::time::timeout(wait, engine.shutdown(None)).await??;
    engine.handle.await?;
    app.await?;
    let appends = std::mem::take(&mut *probe.appends.lock().unwrap());
    Ok(ReproposalRun {
        appends,
        certificate_votes,
        expected_reproposal,
    })
}

#[tokio::test]
async fn parts_only_reproposal_persists_the_signed_proposal() -> eyre::Result<()> {
    let run = run_reproposal(Fault::Observe, VoteType::Prevote).await?;
    let proposals: Vec<_> = run
        .appends
        .iter()
        .filter_map(|append| match &append.entry {
            WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(proposal)) => Some(proposal),
            _ => None,
        })
        .collect();
    assert_eq!(
        proposals,
        [&run.expected_reproposal],
        "PartsOnly re-proposal must persist its signed proposal"
    );
    Ok(())
}

#[tokio::test]
async fn failed_reproposal_flush_stops_before_restream() -> eyre::Result<()> {
    run_reproposal(Fault::FlushBeforeRestream, VoteType::Prevote).await?;
    Ok(())
}

#[tokio::test]
async fn round_certificate_persists_rebuilt_votes_before_the_reproposal() -> eyre::Result<()> {
    for vote_type in [VoteType::Prevote, VoteType::Precommit] {
        let run = run_reproposal(Fault::Observe, vote_type).await?;
        let before_reproposal: Vec<_> = run
            .appends
            .iter()
            .take_while(|append| !append.is_reproposal())
            .filter_map(|append| match &append.entry {
                WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(vote))
                    if vote.message.round == Round::new(1) =>
                {
                    Some(vote)
                }
                _ => None,
            })
            .collect();
        assert_eq!(before_reproposal, run.certificate_votes.iter().collect::<Vec<_>>(),
            "persist the reconstructed {vote_type:?} before the round transition and retain the known vote only once");
    }
    Ok(())
}

struct CountingSigner {
    inner: Ed25519Signer,
    calls: Arc<AtomicUsize>,
}

type TestExtension = <TestContext as Context>::Extension;

#[ractor::async_trait]
impl Signer<TestContext> for CountingSigner {
    async fn sign_vote(&self, vote: Vote) -> Result<SignedMessage<TestContext, Vote>, SignError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.sign_vote(vote).await
    }

    async fn sign_proposal(
        &self,
        proposal: Proposal,
    ) -> Result<SignedMessage<TestContext, Proposal>, SignError> {
        self.inner.sign_proposal(proposal).await
    }

    async fn sign_vote_extension(
        &self,
        extension: TestExtension,
    ) -> Result<SignedMessage<TestContext, TestExtension>, SignError> {
        self.inner.sign_vote_extension(extension).await
    }

    async fn sign_validator_proof(
        &self,
        public_key: Vec<u8>,
        peer_id: Vec<u8>,
    ) -> Result<ValidatorProof<TestContext>, SignError> {
        self.inner.sign_validator_proof(public_key, peer_id).await
    }
}

#[tokio::test]
async fn replay_persists_new_votes_and_reuses_them_on_the_next_restart() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let [(validator, private_key)] = make_validators([1]);
    let validators = ValidatorSet::new([validator.clone()]);
    seed_fixture(&path, [WalEntry::Timeout(Timeout::propose(Round::new(0)))])?;

    for expected_signatures in [2, 0] {
        let result = replay_fixture(
            &path,
            &validators,
            validator.address,
            &private_key,
            ValuePayload::ProposalOnly,
            false,
        )
        .await?;
        assert_eq!(result, (2, expected_signatures, 3));
    }
    Ok(())
}

#[tokio::test]
async fn parts_only_replay_does_not_persist_foreign_implicit_proposals() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let [(validator, private_key), (foreign, _)] = make_validators([1, 1]);
    let proposer = foreign.address;
    let validators = ValidatorSet::new([validator.clone(), foreign]);
    seed_fixture(
        &path,
        [1, 2].map(|value| {
            WalEntry::ProposedValue(ProposedValue {
                height: Height::new(1),
                round: Round::new(1),
                valid_round: Round::Nil,
                proposer,
                value: Value::new(value),
                validity: Validity::Valid,
            })
        }),
    )?;
    for _ in 0..2 {
        let result = replay_fixture(
            &path,
            &validators,
            validator.address,
            &private_key,
            ValuePayload::PartsOnly,
            false,
        )
        .await?;
        assert_eq!(result, (0, 0, 2));
    }
    Ok(())
}

#[tokio::test]
async fn legacy_vote_blocks_a_conflicting_live_signature_after_replay() -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("consensus.wal");
    let [(validator, private_key), (peer, _)] = make_validators([1, 1]);
    let validators = ValidatorSet::new([validator.clone(), peer]);
    // A historical vote can be present without the input that produced it. With two validators,
    // replaying this vote alone cannot advance the round. The live propose timeout chooses nil.
    let vote = Ed25519Signer::new(private_key.clone())
        .sign_vote(Vote::new_prevote(
            Height::new(1),
            Round::new(0),
            NilOrVal::Val(ValueId::new(12)),
            validator.address,
        ))
        .await?;
    seed_fixture(
        &path,
        [WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(vote))],
    )?;
    let (published, signatures, _) = replay_fixture(
        &path,
        &validators,
        validator.address,
        &private_key,
        ValuePayload::ProposalOnly,
        true,
    )
    .await?;
    assert_eq!((published, signatures), (0, 0));
    Ok(())
}

fn seed_fixture(
    path: &std::path::Path,
    entries: impl IntoIterator<Item = WalEntry<TestContext>>,
) -> eyre::Result<()> {
    let mut log = Log::open(path)?;
    log.reset(1)?;
    for entry in entries {
        let mut bytes = Vec::new();
        encode_entry(&entry, &ProtobufCodec, &mut bytes)?;
        log.append(bytes)?;
    }
    log.flush()?;
    Ok(())
}

async fn replay_fixture(
    path: &std::path::Path,
    validators: &ValidatorSet,
    local: Address,
    private_key: &PrivateKey,
    value_payload: ValuePayload,
    wait_for_conflict: bool,
) -> eyre::Result<(usize, usize, usize)> {
    let (network, _) = Actor::spawn(None, NetworkSink, ()).await?;
    let (tx_network, _rx_network) = mpsc::channel::<NetworkMsg<TestContext>>(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let moniker = format!("replay-signatures-{value_payload:?}-{wait_for_conflict}");
    let config = Config {
        moniker: moniker.clone(),
        consensus: ConsensusConfig {
            value_payload,
            ..ConsensusConfig::default()
        },
        value_sync: ValueSyncConfig::default(),
    };
    let (channels, engine) = EngineBuilder::new(TestContext::default(), config)
        .with_default_wal(WalContext::new(path.to_owned(), ProtobufCodec))
        .with_custom_network(network, tx_network)
        .with_no_sync()
        .with_default_consensus(ConsensusContext::new_validator(
            local,
            Box::new(Ed25519Verifier),
            Box::new(CountingSigner {
                inner: Ed25519Signer::new(private_key.clone()),
                calls: Arc::clone(&calls),
            }),
        ))
        .with_default_request(RequestContext::new(1))
        .build()
        .await?;
    let mut events = channels.events.subscribe();
    let mut requests = channels.consensus;
    let validators = validators.clone();
    let application = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            match request {
                AppMsg::ConsensusReady { reply } => {
                    let mut timeouts = LinearTimeouts::default();
                    if wait_for_conflict {
                        timeouts.propose = Duration::from_millis(50);
                    }
                    let params = HeightParams::new(validators.clone(), timeouts, None);
                    let _ = reply.send((Height::new(1), params));
                }
                AppMsg::StartedRound { reply_value, .. } => {
                    let _ = reply_value.send(Vec::new());
                }
                AppMsg::ExtendVote { reply, .. } => {
                    let _ = reply.send(None);
                }
                _ => {}
            }
        }
    });
    let mut published = tokio::time::timeout(Duration::from_secs(2), async {
        let mut published = 0;
        loop {
            match events.recv().await? {
                Event::Published(_) => published += 1,
                Event::WalReplayDone(_) => {
                    return Ok::<_, tokio::sync::broadcast::error::RecvError>(published)
                }
                _ => {}
            }
        }
    })
    .await??;
    if wait_for_conflict {
        let expected =
            format!("malachitebft_core_consensus_node_safety_failure{{moniker=\"{moniker}\"}} 1");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut output = String::new();
                metrics::export(&mut output);
                if output.lines().any(|line| line == expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        published += std::iter::from_fn(|| events.try_recv().ok())
            .filter(|event| matches!(event, Event::Published(_)))
            .count();
    }
    let signatures = calls.load(Ordering::SeqCst);
    engine.shutdown(None).await?;
    engine.handle.await?;
    application.await?;

    let mut log = Log::open(path)?;
    let entries =
        log_entries::<TestContext, _>(&mut log, &ProtobufCodec)?.collect::<io::Result<Vec<_>>>()?;
    Ok((published, signatures, entries.len()))
}

async fn run_startup_liveness_scenario(fault: Fault) {
    let moniker = format!("wal-startup-{fault:?}").to_lowercase();
    let [(validator, private_key), (full_node, _)] = make_validators([1, 2]);
    let validators = ValidatorSet::new([validator.clone()]);
    let probe = FaultProbe::new(fault);
    let (wal, _) = Actor::spawn(None, FaultingWal, probe.clone())
        .await
        .expect("spawn faulting WAL");
    let (network, _) = Actor::spawn(None, NetworkSink, ())
        .await
        .expect("spawn network sink");
    let (tx_network, _rx_network) = mpsc::channel::<NetworkMsg<TestContext>>(1);
    let config = Config {
        moniker: moniker.clone(),
        consensus: ConsensusConfig {
            value_payload: ValuePayload::ProposalOnly,
            ..ConsensusConfig::default()
        },
        value_sync: ValueSyncConfig::default(),
    };

    let builder = EngineBuilder::new(TestContext::default(), config)
        .with_custom_wal(wal.clone())
        .with_custom_network(network.clone(), tx_network)
        .with_no_sync();
    let builder = match fault {
        Fault::Fetch | Fault::BufferedReset => {
            builder.with_default_consensus(ConsensusContext::new_validator(
                validator.address,
                Box::new(Ed25519Verifier),
                Box::new(Ed25519Signer::new(private_key)),
            ))
        }
        Fault::Reset => builder.with_default_consensus(ConsensusContext::new_full_node(
            full_node.address,
            Box::new(Ed25519Verifier),
        )),
        _ => unreachable!("only fetch and reset are startup liveness failures"),
    };
    let (channels, mut engine) = builder
        .with_default_request(RequestContext::new(1))
        .build()
        .await
        .expect("build engine");
    let mut events = channels.events.subscribe();
    let mut requests = channels.consensus;
    if fault == Fault::BufferedReset {
        let ready = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .expect("consensus ready request timed out")
            .expect("application channel closed");
        let AppMsg::ConsensusReady { reply } = ready else {
            panic!("expected ConsensusReady before buffering RestartHeight");
        };
        let consensus: ActorRef<ConsensusActorMsg<TestContext>> = engine
            .actor
            .get_cell()
            .get_children()
            .into_iter()
            .find(|child| {
                child.is_message_type_of::<ConsensusActorMsg<TestContext>>() == Some(true)
            })
            .expect("engine should supervise consensus")
            .into();
        let params = HeightParams::new(validators.clone(), LinearTimeouts::default(), None);
        consensus
            .cast(ConsensusActorMsg::RestartHeight(
                Height::new(1),
                params.clone(),
            ))
            .expect("enqueue restart before starting consensus");
        reply
            .send((Height::new(1), params))
            .expect("start consensus after enqueueing restart");
    }
    let app = tokio::spawn(drive_application(requests, validators, reject_decisions));

    tokio::time::timeout(Duration::from_secs(2), probe.reached.notified())
        .await
        .expect("consensus did not reach injected startup WAL fault");

    if tokio::time::timeout(Duration::from_secs(2), &mut engine.handle)
        .await
        .is_err()
    {
        engine.actor.stop(None);
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut engine.handle).await;
        app.abort();
        panic!("startup WAL failure left the engine invisibly hung");
    }

    tokio::time::timeout(Duration::from_secs(2), app)
        .await
        .expect("application connector did not stop after liveness failure")
        .expect("application driver panicked");

    let reset_errors = std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| matches!(event, Event::WalResetError(_)))
        .count();
    assert_eq!(reset_errors, 1, "startup WAL failure was not emitted");

    let mut exported = String::new();
    metrics::export(&mut exported);
    let unexpected_metric =
        format!("malachitebft_core_consensus_node_safety_failure{{moniker=\"{moniker}\"}} 1");
    assert!(
        exported.lines().all(|line| line != unexpected_metric),
        "restartable startup failure was misclassified as a safety hang"
    );
}

#[tokio::test]
async fn wal_fetch_failure_terminates_through_liveness_path() {
    run_startup_liveness_scenario(Fault::Fetch).await;
}

#[tokio::test]
async fn wal_reset_failure_terminates_through_liveness_path() {
    run_startup_liveness_scenario(Fault::Reset).await;
}

#[tokio::test]
async fn buffered_restart_wal_failure_terminates_the_engine() {
    run_startup_liveness_scenario(Fault::BufferedReset).await;
}

#[tokio::test]
async fn wal_dying_during_child_commit_is_rejected() {
    let [(validator, private_key)] = make_validators([1]);
    let probe = FaultProbe::new(Fault::Registration);
    let (wal, _) = Actor::spawn(None, FaultingWal, probe.clone())
        .await
        .expect("spawn registration-failing WAL");
    let (network, _) = Actor::spawn(None, NetworkSink, ())
        .await
        .expect("spawn network sink");
    let (tx_network, _rx_network) = mpsc::channel::<NetworkMsg<TestContext>>(1);
    let config = Config {
        moniker: "wal-registration-race".to_owned(),
        consensus: ConsensusConfig {
            value_payload: ValuePayload::ProposalOnly,
            ..ConsensusConfig::default()
        },
        value_sync: ValueSyncConfig::default(),
    };

    let result = EngineBuilder::new(TestContext::default(), config)
        .with_custom_wal(wal.clone())
        .with_custom_network(network, tx_network)
        .with_no_sync()
        .with_default_consensus(ConsensusContext::new_validator(
            validator.address,
            Box::new(Ed25519Verifier),
            Box::new(Ed25519Signer::new(private_key)),
        ))
        .with_default_request(RequestContext::new(1))
        .build()
        .await;

    match result {
        Err(error) => assert!(
            error.to_string().contains("WAL liveness probe failed"),
            "unexpected build error: {error}"
        ),
        Ok((_channels, engine)) => {
            engine.actor.stop(None);
            let _ = engine.handle.await;
            panic!("builder accepted a WAL that died during child commit");
        }
    }
    tokio::time::timeout(Duration::from_secs(2), probe.reached.notified())
        .await
        .expect("child commit did not probe WAL liveness");
    tokio::time::timeout(Duration::from_secs(2), async {
        while wal.get_status() != ractor::ActorStatus::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registration-failing WAL did not stop");
}
