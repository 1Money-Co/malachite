use std::marker::PhantomData;
use std::time::Duration;

use arc_malachitebft_app_channel::app::engine::consensus::ConsensusMsg;
use arc_malachitebft_app_channel::app::engine::host::HostMsg;
use arc_malachitebft_app_channel::app::engine::network::{NetworkEvent, NetworkMsg, Status};
use arc_malachitebft_app_channel::app::engine::sync::{Params, Sync, SyncMsg};
use arc_malachitebft_app_channel::app::metrics::SharedRegistry;
use arc_malachitebft_app_channel::app::types::core::{CommitCertificate, Round};
use arc_malachitebft_app_channel::app::types::PeerId;
use bytes::Bytes;
use malachitebft_sync::{
    Config, HeightStartType, Metrics, OutboundRequestId, RawDecidedValue, Request, Response,
    ValueResponse,
};
use malachitebft_test::codec::proto::ProtobufCodec;
use malachitebft_test::{Height, TestContext, ValueId};
use ractor::{Actor, ActorProcessingErr, ActorRef, Message};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};

const WAIT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(200);

struct Mailbox<M>(PhantomData<fn() -> M>);

impl<M: Message> Mailbox<M> {
    async fn spawn() -> (ActorRef<M>, mpsc::UnboundedReceiver<M>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (actor, _) = Actor::spawn(None, Self(PhantomData), sender).await.unwrap();
        (actor, receiver)
    }
}

#[ractor::async_trait]
impl<M: Message> Actor for Mailbox<M> {
    type Msg = M;
    type State = mpsc::UnboundedSender<M>;
    type Arguments = Self::State;

    async fn pre_start(
        &self,
        _myself: ActorRef<M>,
        sender: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(sender)
    }

    async fn handle(
        &self,
        _myself: ActorRef<M>,
        message: M,
        sender: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = sender.send(message);
        Ok(())
    }
}

#[tokio::test]
async fn retries_last_peer_after_timeout_in_eager_mode() {
    retries_last_peer_after_timeout(Duration::ZERO).await;
}

#[tokio::test]
async fn retries_last_peer_after_timeout_between_periodic_status_updates() {
    retries_last_peer_after_timeout(Duration::from_secs(60)).await;
}

async fn retries_last_peer_after_timeout(status_update_interval: Duration) {
    let (network, mut network_messages) = Mailbox::<NetworkMsg<TestContext>>::spawn().await;
    let (host, _host_messages) = Mailbox::<HostMsg<TestContext>>::spawn().await;
    let (consensus, mut consensus_messages) = Mailbox::<ConsensusMsg<TestContext>>::spawn().await;
    let sync = Sync::spawn(
        TestContext::new(),
        network.clone(),
        host.clone(),
        consensus.clone(),
        Params {
            request_timeout: REQUEST_TIMEOUT,
            status_update_interval,
        },
        ProtobufCodec,
        Config::default().with_request_timeout(REQUEST_TIMEOUT),
        Metrics::register(
            &SharedRegistry::new(Default::default(), None),
            status_update_interval,
        ),
        tracing::Span::none(),
    )
    .await
    .unwrap();

    let Some(NetworkMsg::Subscribe(subscriber)) = timeout(WAIT, network_messages.recv())
        .await
        .expect("sync should subscribe to network events")
    else {
        panic!("expected the sync actor to subscribe to the network");
    };

    let peer = PeerId::random();
    let missing_height = Height::new(10);
    subscriber.send(NetworkEvent::Status(
        peer,
        Status::new(missing_height, Height::new(1)),
    ));
    sync.cast(SyncMsg::StartedHeight(
        missing_height,
        HeightStartType::Start,
    ))
    .unwrap();

    let Some(NetworkMsg::OutgoingRequest(request_peer, Request::ValueRequest(request), reply)) =
        timeout(WAIT, network_messages.recv())
            .await
            .expect("sync should request the missing height")
    else {
        panic!("expected the initial sync request");
    };
    assert_eq!(request_peer, peer);
    assert_eq!(request.range, missing_height..=missing_height);

    // The network accepted the request, but the only peer never replies to it.
    let first_attempt = Instant::now();
    reply.send(OutboundRequestId::new("first")).unwrap();

    let Some(NetworkMsg::OutgoingRequest(request_peer, Request::ValueRequest(request), reply)) =
        timeout(WAIT, network_messages.recv())
            .await
            .expect("sync should retry the cached peer after timeout without a new status")
    else {
        panic!("expected a retry to the peer");
    };
    assert_eq!(request_peer, peer);
    assert_eq!(request.range, missing_height..=missing_height);
    assert!(
        first_attempt.elapsed() >= REQUEST_TIMEOUT + Duration::from_millis(500),
        "retry should back off after the request times out",
    );

    let retry_id = OutboundRequestId::new("retry");
    let bytes = Bytes::from_static(b"recovered value");
    let certificate =
        CommitCertificate::new(missing_height, Round::new(0), ValueId::new(42), vec![]);
    reply.send(retry_id.clone()).unwrap();
    subscriber.send(NetworkEvent::SyncResponse(
        retry_id,
        peer,
        Some(Response::ValueResponse(ValueResponse::new(
            missing_height,
            vec![RawDecidedValue::new(bytes.clone(), certificate.clone())],
        ))),
    ));

    let Some(ConsensusMsg::ProcessSyncResponse(response)) =
        timeout(WAIT, consensus_messages.recv())
            .await
            .expect("the retry response should reach consensus")
    else {
        panic!("expected consensus to receive the recovered value");
    };
    assert_eq!(response.peer, peer);
    assert_eq!(response.certificate, certificate);
    assert_eq!(response.value_bytes, bytes);

    sync.stop(None);
    consensus.stop(None);
    host.stop(None);
    network.stop(None);
}
