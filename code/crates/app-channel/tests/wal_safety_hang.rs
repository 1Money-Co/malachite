use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_malachitebft_app_channel::app::config::NodeConfig;
use arc_malachitebft_app_channel::app::consensus::{
    LocallyProposedValue, ProposedValue, SignedConsensusMsg,
};
use arc_malachitebft_app_channel::app::engine::consensus::Msg as ConsensusActorMsg;
use arc_malachitebft_app_channel::app::engine::host::HeightParams;
use arc_malachitebft_app_channel::app::engine::network::{Msg as EngineNetworkMsg, NetworkEvent};
use arc_malachitebft_app_channel::app::engine::util::events::Event;
use arc_malachitebft_app_channel::app::engine::wal::{
    encode_entry, log_entries, Msg as WalMsg, WalEntry,
};
use arc_malachitebft_app_channel::app::metrics;
use arc_malachitebft_app_channel::app::types::core::{
    Context, NilOrVal, Round, SignedMessage, Timeout, ValidatorProof, Validity,
};
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
use tokio::sync::{mpsc, Notify};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Append,
    AppendCall,
    Fetch,
    Flush(usize),
    Replay,
    Reset,
    BufferedReset,
    Registration,
}

#[derive(Clone)]
struct FaultProbe {
    fault: Fault,
    append_calls: Arc<AtomicUsize>,
    flush_calls: Arc<AtomicUsize>,
    reached: Arc<Notify>,
}

impl FaultProbe {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            append_calls: Arc::new(AtomicUsize::new(0)),
            flush_calls: Arc::new(AtomicUsize::new(0)),
            reached: Arc::new(Notify::new()),
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
            WalMsg::Append(_, _, reply) => {
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
                    Fault::Fetch
                    | Fault::Flush(_)
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

async fn drive_application(mut rx: mpsc::Receiver<AppMsg<TestContext>>, validators: ValidatorSet) {
    while let Some(msg) = rx.recv().await {
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
            AppMsg::Decided { .. } | AppMsg::Finalized { .. } => {
                panic!("consensus decided after injected WAL failure");
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
    let mut app = tokio::spawn(drive_application(channels.consensus, validators));

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

    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(
                event,
                Event::Published(_) | Event::Decided { .. } | Event::WalReplayEntry(_)
            ),
            "consensus effect escaped after WAL fault: {event}"
        );
        assert!(
            !matches!(event, Event::WalWriteError(_)),
            "runtime WAL fault must not request process exit"
        );
    }

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
        Fault::Replay => {}
        Fault::Fetch | Fault::Reset | Fault::BufferedReset => {
            unreachable!("startup failure uses its own scenario")
        }
        Fault::Registration => unreachable!("registration failure uses its own scenario"),
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
async fn corrupt_replay_enters_supervisor_safety_hang() {
    run_fault_scenario(Fault::Replay).await;
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
    let app = tokio::spawn(drive_application(requests, validators));

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
