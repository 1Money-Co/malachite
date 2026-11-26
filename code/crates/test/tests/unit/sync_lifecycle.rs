use std::ops::RangeInclusive;
use std::time::Duration;

use arc_malachitebft_test::{Height, TestContext, ValueId};
use bytes::Bytes;
use malachitebft_core_types::{CommitCertificate, Round};
use malachitebft_sync::{
    co::{CoState, Gen},
    height_allowance_snapshot, Config, Effect, HeightStartType, Input, Metrics, OutboundRequestId,
    PeerId, RawDecidedValue, RequestStage, Resume, State, Status, ValueResponse,
};
use rand::{rngs::StdRng, SeedableRng};

struct Harness {
    state: State<TestContext>,
    metrics: Metrics,
    next_request: u64,
}

impl Harness {
    fn new(batch_size: usize, parallel_requests: usize) -> Self {
        Self {
            state: State::new(
                Box::new(StdRng::seed_from_u64(375)),
                Config::default()
                    .with_batch_size(batch_size)
                    .with_parallel_requests(parallel_requests),
            ),
            metrics: Metrics::new(Duration::from_secs(1)),
            next_request: 0,
        }
    }

    fn apply(&mut self, input: Input<TestContext>) -> Vec<Effect<TestContext>> {
        let mut generator = Gen::new(|co| {
            malachitebft_sync::handle::handle(co, &mut self.state, &self.metrics, input)
        });
        let mut resume = Resume::default();
        let mut effects = Vec::new();

        loop {
            match generator.resume_with(resume) {
                CoState::Yielded(effect) => {
                    resume = if matches!(effect, Effect::SendValueRequest(..)) {
                        self.next_request += 1;
                        Resume::ValueRequestId(Some(OutboundRequestId::new(self.next_request)))
                    } else {
                        Resume::default()
                    };
                    effects.push(effect);
                }
                CoState::Complete(result) => {
                    result.unwrap();
                    return effects;
                }
            }
        }
    }

    fn start(&mut self, height: u64) -> Vec<Effect<TestContext>> {
        self.apply(Input::StartedHeight(
            Height::new(height),
            HeightStartType::Start,
        ))
    }

    fn status(&mut self, peer: PeerId, tip: u64) -> Vec<Effect<TestContext>> {
        self.apply(Input::Status(Status {
            peer_id: peer,
            tip_height: Height::new(tip),
            history_min_height: Height::new(1),
        }))
    }
}

fn requested_ranges(effects: &[Effect<TestContext>]) -> Vec<RangeInclusive<Height>> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendValueRequest(_, request, _) => Some(request.range.clone()),
            _ => None,
        })
        .collect()
}

fn forwarded(effects: &[Effect<TestContext>]) -> Vec<ValueResponse<TestContext>> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::ProcessValueResponse(_, _, response, _) => Some(response.clone()),
            _ => None,
        })
        .collect()
}

fn requests_in_stage(
    state: &State<TestContext>,
    stage: RequestStage,
) -> Vec<(OutboundRequestId, RangeInclusive<Height>, PeerId)> {
    state
        .pending_requests
        .iter()
        .filter(|(id, _)| state.request_stage(id) == Some(stage))
        .map(|(id, entry)| (id.clone(), entry.range.clone(), entry.peer))
        .collect()
}

fn response(start: u64, end: u64) -> ValueResponse<TestContext> {
    ValueResponse::new(
        Height::new(start),
        (start..=end)
            .map(|height| {
                RawDecidedValue::new(
                    Bytes::from(height.to_be_bytes().to_vec()),
                    CommitCertificate::new(
                        Height::new(height),
                        Round::new(0),
                        ValueId::new(height),
                        Vec::new(),
                    ),
                )
            })
            .collect(),
    )
}

#[test]
fn startup_tick_waits_for_restored_height() {
    let mut harness = Harness::new(500, 25);
    let peer = PeerId::random();

    assert!(requested_ranges(&harness.status(peer, 1_266_884)).is_empty());
    assert!(harness.state.pending_requests.is_empty());

    assert_eq!(
        requested_ranges(&harness.start(1_266_457)),
        vec![Height::new(1_266_457)..=Height::new(1_266_884)]
    );
}

#[test]
fn late_obsolete_responses_release_full_height_allowance() {
    let mut harness = Harness::new(500, 25);
    let peer = PeerId::random();
    let _ = harness.start(1);
    assert_eq!(requested_ranges(&harness.status(peer, 1_266_884)).len(), 25);
    let requests = requests_in_stage(&harness.state, RequestStage::Inflight);
    assert_eq!(requests.len(), 25);

    // All 12,500 heights are still on the wire when the restored height arrives.
    let restored_effects = harness.start(1_266_457);
    let mut forwarded_responses = 0;
    for (id, range, peer) in requests {
        let effects = harness.apply(Input::ValueResponse(
            id,
            peer,
            Some(response(range.start().as_u64(), range.end().as_u64())),
        ));
        forwarded_responses += forwarded(&effects).len();
    }

    // No timeout, decision, or restart is needed to request the actual missing range.
    assert_eq!(
        requested_ranges(&restored_effects),
        vec![Height::new(1_266_457)..=Height::new(1_266_884)]
    );
    assert_eq!(forwarded_responses, 0);
    assert!(requests_in_stage(&harness.state, RequestStage::PendingConsensus).is_empty());
}

#[test]
fn late_response_ending_at_tip_is_discarded() {
    let mut harness = Harness::new(5, 1);
    let peer = PeerId::random();
    let _ = harness.start(10);
    harness.status(peer, 14);
    let (id, _, _) = requests_in_stage(&harness.state, RequestStage::Inflight)
        .into_iter()
        .next()
        .unwrap();
    let _ = harness.start(13);

    let effects = harness.apply(Input::ValueResponse(id, peer, Some(response(10, 12))));
    assert!(forwarded(&effects).is_empty());
    assert_eq!(
        requested_ranges(&effects),
        vec![Height::new(13)..=Height::new(14)]
    );
    assert!(requests_in_stage(&harness.state, RequestStage::PendingConsensus).is_empty());
}

#[test]
fn late_full_and_partial_responses_forward_only_undecided_suffix() {
    for end in [13, 14] {
        let mut harness = Harness::new(5, 1);
        let peer = PeerId::random();
        let _ = harness.start(10);
        assert_eq!(
            requested_ranges(&harness.status(peer, 14)),
            vec![Height::new(10)..=Height::new(14)]
        );
        let (id, _, _) = requests_in_stage(&harness.state, RequestStage::Inflight)
            .into_iter()
            .next()
            .unwrap();
        let _ = harness.start(13);

        let effects = harness.apply(Input::ValueResponse(
            id.clone(),
            peer,
            Some(response(10, end)),
        ));
        assert_eq!(forwarded(&effects), vec![response(13, end)]);
        let pending = harness.state.pending_requests.get(&id).unwrap();
        assert_eq!(
            harness.state.request_stage(&id),
            Some(RequestStage::PendingConsensus)
        );
        assert_eq!(pending.range, Height::new(13)..=Height::new(end));
        assert_eq!(pending.peer, peer);
    }
}

#[test]
fn obsolete_prefix_still_requires_original_range_and_certificate_validation() {
    for wrong_start in [false, true] {
        let mut harness = Harness::new(5, 1);
        let peer = PeerId::random();
        let _ = harness.start(10);
        harness.status(peer, 14);
        let (id, _, _) = requests_in_stage(&harness.state, RequestStage::Inflight)
            .into_iter()
            .next()
            .unwrap();
        let _ = harness.start(13);
        let mut value = response(if wrong_start { 9 } else { 10 }, 13);
        if !wrong_start {
            value.values[0].certificate.height = Height::new(9);
        }

        let effects = harness.apply(Input::ValueResponse(id, peer, Some(value)));
        assert!(forwarded(&effects).is_empty());
        assert!(requests_in_stage(&harness.state, RequestStage::PendingConsensus).is_empty());
        assert_eq!(
            requested_ranges(&harness.status(peer, 14)),
            vec![Height::new(13)..=Height::new(14)]
        );
    }
}

#[test]
fn received_undecided_values_still_enforce_full_height_allowance() {
    let mut harness = Harness::new(500, 25);
    let peer = PeerId::random();
    let _ = harness.start(1);
    harness.status(peer, 13_000);
    let requests = requests_in_stage(&harness.state, RequestStage::Inflight);
    assert_eq!(requests.len(), 25);
    for (id, range, peer) in requests {
        harness.apply(Input::ValueResponse(
            id,
            peer,
            Some(response(range.start().as_u64(), range.end().as_u64())),
        ));
    }

    assert!(requests_in_stage(&harness.state, RequestStage::Inflight).is_empty());
    assert_eq!(
        requests_in_stage(&harness.state, RequestStage::PendingConsensus).len(),
        25
    );
    assert!(requested_ranges(&harness.status(peer, 13_000)).is_empty());
    harness.apply(Input::Decided(Height::new(500)));
    assert_eq!(
        requested_ranges(&harness.status(peer, 13_000)),
        vec![Height::new(12_501)..=Height::new(13_000)]
    );
}

#[test]
fn wrong_peer_response_keeps_original_request_and_capacity() {
    let mut harness = Harness::new(5, 1);
    let original_peer = PeerId::random();
    let wrong_peer = PeerId::random();
    let _ = harness.start(10);
    harness.status(original_peer, 14);
    let inflight_before = requests_in_stage(&harness.state, RequestStage::Inflight);
    let (request_id, range, stored_peer) = inflight_before.first().unwrap();

    let effects = harness.apply(Input::ValueResponse(
        request_id.clone(),
        wrong_peer,
        Some(response(range.start().as_u64(), range.end().as_u64())),
    ));

    assert!(forwarded(&effects).is_empty());
    assert_eq!(*stored_peer, original_peer);
    assert_eq!(
        requests_in_stage(&harness.state, RequestStage::Inflight),
        inflight_before
    );
    assert!(requested_ranges(&harness.status(original_peer, 14)).is_empty());
}

#[test]
fn unknown_response_does_not_consume_or_release_capacity() {
    let mut harness = Harness::new(5, 1);
    let peer = PeerId::random();
    let _ = harness.start(10);
    harness.status(peer, 14);
    let requests_before = requests_in_stage(&harness.state, RequestStage::Inflight);

    let effects = harness.apply(Input::ValueResponse(
        OutboundRequestId::new(999),
        peer,
        Some(response(10, 14)),
    ));

    assert!(forwarded(&effects).is_empty());
    assert_eq!(
        requests_in_stage(&harness.state, RequestStage::Inflight),
        requests_before
    );
    assert!(requests_in_stage(&harness.state, RequestStage::PendingConsensus).is_empty());
    assert!(requested_ranges(&harness.status(peer, 14)).is_empty());
}

#[test]
fn saturated_height_allowance_reports_inflight_and_pending_breakdown() {
    let mut harness = Harness::new(500, 25);
    let peer = PeerId::random();
    let _ = harness.start(1);
    harness.status(peer, 13_000);
    let (request_id, range, request_peer) =
        requests_in_stage(&harness.state, RequestStage::Inflight)
            .into_iter()
            .next()
            .unwrap();
    harness.apply(Input::ValueResponse(
        request_id,
        request_peer,
        Some(response(range.start().as_u64(), range.end().as_u64())),
    ));

    let snapshot = height_allowance_snapshot(&harness.state);
    assert_eq!(snapshot.inflight_heights, 12_000);
    assert_eq!(snapshot.pending_heights, 500);
    assert_eq!(snapshot.lowest_inflight_start, Some(Height::new(501)));
    assert_eq!(snapshot.lowest_pending_start, Some(Height::new(1)));
    assert_eq!(snapshot.tip_height, Height::new(0));
    assert!(requested_ranges(&harness.status(peer, 13_000)).is_empty());
}

#[test]
fn invalid_values_and_processing_errors_release_the_failed_height_for_retry() {
    for processing_error in [false, true] {
        let mut harness = Harness::new(5, 1);
        let peer = PeerId::random();
        let retry_peer = PeerId::random();
        let _ = harness.start(10);
        harness.status(peer, 14);
        harness.status(retry_peer, 14);
        let (id, _, request_peer) = requests_in_stage(&harness.state, RequestStage::Inflight)
            .into_iter()
            .next()
            .unwrap();
        harness.apply(Input::ValueResponse(
            id,
            request_peer,
            Some(response(10, 14)),
        ));

        let score_before = harness.state.peer_scorer.get_score(&request_peer);
        let effects = harness.apply(if processing_error {
            Input::ValueProcessingError(request_peer, Height::new(12))
        } else {
            Input::InvalidValue(request_peer, Height::new(12))
        });
        assert_eq!(
            requested_ranges(&effects),
            if processing_error {
                vec![Height::new(12)..=Height::new(12)]
            } else {
                vec![Height::new(10)..=Height::new(14)]
            }
        );
        let score_after = harness.state.peer_scorer.get_score(&request_peer);
        if processing_error {
            assert_eq!(score_after, score_before);
            let snapshot = height_allowance_snapshot(&harness.state);
            assert_eq!(snapshot.pending_heights, 4);
            assert_eq!(snapshot.inflight_heights, 1);
        } else {
            assert!(score_after < score_before);
        }
    }
}
