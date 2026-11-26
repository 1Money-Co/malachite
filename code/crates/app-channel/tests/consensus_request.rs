use arc_malachitebft_app_channel::{ConsensusRequest, ConsensusRequestError};
use malachitebft_test::TestContext;
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn dump_state_returns_closed_when_receiver_is_gone() {
    let (tx_request, rx_request) = mpsc::channel::<ConsensusRequest<TestContext>>(1);
    drop(rx_request);

    let error = ConsensusRequest::dump_state(&tx_request)
        .await
        .expect_err("closed request channel must reject the request");

    assert_eq!(error, ConsensusRequestError::Closed);
}

#[tokio::test]
async fn dump_state_returns_full_when_request_buffer_is_saturated() {
    let (tx_request, _rx_request) = mpsc::channel::<ConsensusRequest<TestContext>>(1);
    let (tx_reply, _rx_reply) = oneshot::channel();
    tx_request
        .try_send(ConsensusRequest::DumpState(tx_reply))
        .expect("prefill capacity-one request channel");

    let error = ConsensusRequest::dump_state(&tx_request)
        .await
        .expect_err("full request channel must reject the request");

    assert_eq!(error, ConsensusRequestError::Full);
}

#[tokio::test]
async fn dump_state_returns_recv_when_responder_drops_reply() {
    let (tx_request, mut rx_request) = mpsc::channel::<ConsensusRequest<TestContext>>(1);
    let request = tokio::spawn(async move { ConsensusRequest::dump_state(&tx_request).await });

    let ConsensusRequest::DumpState(tx_reply) = rx_request
        .recv()
        .await
        .expect("dump-state request must reach the responder");
    drop(tx_reply);

    let error = request
        .await
        .expect("request task must not panic")
        .expect_err("dropped responder must close the reply channel");

    assert_eq!(error, ConsensusRequestError::Recv);
}
