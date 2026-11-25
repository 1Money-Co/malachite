use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_malachitebft_app_channel::app::config::NodeConfig;
use arc_malachitebft_app_channel::app::consensus::LocallyProposedValue;
use arc_malachitebft_app_channel::app::engine::host::HeightParams;
use arc_malachitebft_app_channel::app::engine::network::{Msg as EngineNetworkMsg, NetworkEvent};
use arc_malachitebft_app_channel::app::engine::util::events::Event;
use arc_malachitebft_app_channel::app::engine::wal::Msg as WalMsg;
use arc_malachitebft_app_channel::app::metrics;
use arc_malachitebft_app_channel::{
    AppMsg, ConsensusContext, EngineBuilder, NetworkMsg, RequestContext,
};
use malachitebft_config::{ConsensusConfig, ValuePayload, ValueSyncConfig};
use malachitebft_test::utils::validators::make_validators;
use malachitebft_test::{
    Ed25519Signer, Ed25519Verifier, Height, LinearTimeouts, TestContext, ValidatorSet, Value,
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
                Fault::Replay => {
                    probe.reached.notify_one();
                    let _ = reply.send(Ok(vec![Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "injected corrupt replay entry",
                    ))]));
                }
                _ => {
                    let _ = reply.send(Ok(Vec::new()));
                }
            },
            WalMsg::Reset(_, reply) => {
                if probe.fault == Fault::Reset {
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
            !matches!(event, Event::Published(_) | Event::Decided { .. }),
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
        Fault::Fetch | Fault::Reset => unreachable!("startup failure uses its own scenario"),
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
        Fault::Fetch => builder.with_default_consensus(ConsensusContext::new_validator(
            validator.address,
            Box::new(Ed25519Verifier),
            Box::new(Ed25519Signer::new(private_key)),
        )),
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
    let app = tokio::spawn(drive_application(channels.consensus, validators));

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
