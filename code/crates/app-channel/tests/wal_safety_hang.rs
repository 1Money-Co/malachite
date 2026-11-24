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
    Flush,
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
            WalMsg::StartedHeight(_, reply) => {
                let _ = reply.send(Ok(Vec::new()));
            }
            WalMsg::Reset(_, reply) => {
                let _ = reply.send(Ok(()));
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
                    Fault::Flush => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            WalMsg::Flush(reply) => {
                probe.flush_calls.fetch_add(1, Ordering::SeqCst);
                if probe.fault == Fault::Flush {
                    probe.reached.notify_one();
                    let _ = reply.send(Err(eyre::eyre!("injected WAL flush failure")));
                } else {
                    let _ = reply.send(Ok(()));
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter("arc_malachitebft=trace")
        .with_test_writer()
        .try_init();
    let moniker = format!("wal-safety-{fault:?}").to_lowercase();
    let [(validator, private_key)] = make_validators([1]);
    let validators = ValidatorSet::new([validator.clone()]);
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
    let app = tokio::spawn(drive_application(channels.consensus, validators));

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
    tokio::time::timeout(Duration::from_secs(2), app)
        .await
        .expect("application connector did not stop")
        .expect("application driver panicked");

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
        Fault::Flush => assert_eq!(probe.flush_calls.load(Ordering::SeqCst), 1),
    }

    engine.actor.stop(None);
    tokio::time::timeout(Duration::from_secs(2), &mut engine.handle)
        .await
        .expect("engine cleanup timeout")
        .expect("join engine cleanup");
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
    run_fault_scenario(Fault::Flush).await;
}
