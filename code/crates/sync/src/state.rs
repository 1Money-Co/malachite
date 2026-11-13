use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::RangeInclusive;

use malachitebft_core_types::{Context, Height};
use malachitebft_peer::PeerId;

use crate::scoring::{ema, PeerScorer, Strategy};
use crate::{Config, OutboundRequestId, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStage {
    Inflight,
    PendingConsensus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeightAllowanceSnapshot<H> {
    pub inflight_heights: u64,
    pub pending_heights: u64,
    pub lowest_inflight_start: Option<H>,
    pub lowest_pending_start: Option<H>,
    pub tip_height: H,
}

/// The value stored for each pending request.
#[derive(Debug, Clone)]
pub struct PendingRequestEntry<H> {
    /// The requested height range.
    pub range: RangeInclusive<H>,
    /// The peer currently handling this request.
    pub peer: PeerId,
    /// Peers already tried and failed for this range, accumulated across retries.
    pub excluded_peers: BTreeSet<PeerId>,
}

pub struct State<Ctx>
where
    Ctx: Context,
{
    pub rng: Box<dyn rand::RngCore + Send>,

    /// Configuration for the sync state and behaviour.
    pub config: Config,

    /// Consensus has started
    pub started: bool,

    /// The height that consensus is at, but has not decided yet.
    pub consensus_height: Ctx::Height,

    /// Height of last decided value
    pub tip_height: Ctx::Height,

    /// Next height to send a sync request.
    /// Invariant: `sync_height > tip_height`
    pub sync_height: Ctx::Height,

    /// The requested range of heights, the peer handling the request, and
    /// the set of peers already tried (and failed) for this range.
    pub pending_requests: BTreeMap<OutboundRequestId, PendingRequestEntry<Ctx::Height>>,

    /// The exact response ranges waiting for consensus validation, keyed by
    /// their original request ID. The ranges can become disjoint when a local
    /// processing failure releases one height for retry.
    pending_consensus_ranges: BTreeMap<OutboundRequestId, Vec<RangeInclusive<Ctx::Height>>>,

    /// The set of peers we are connected to in order to get values, certificates and votes.
    pub peers: BTreeMap<PeerId, Status<Ctx>>,

    /// Peer scorer for scoring peers based on their performance.
    pub peer_scorer: PeerScorer,
}

impl<Ctx> State<Ctx>
where
    Ctx: Context,
{
    pub fn new(
        // Random number generator for selecting peers
        rng: Box<dyn rand::RngCore + Send>,
        // Sync configuration
        config: Config,
    ) -> Self {
        let peer_scorer = match config.scoring_strategy {
            Strategy::Ema => PeerScorer::new(ema::ExponentialMovingAverage::default()),
        };

        Self {
            rng,
            config,
            started: false,
            consensus_height: Ctx::Height::ZERO,
            tip_height: Ctx::Height::ZERO,
            sync_height: Ctx::Height::ZERO,
            pending_requests: BTreeMap::new(),
            pending_consensus_ranges: BTreeMap::new(),
            peers: BTreeMap::new(),
            peer_scorer,
        }
    }

    /// The maximum number of parallel requests that can be made to peers.
    /// If the configuration is set to 0, it defaults to 1.
    pub fn max_parallel_requests(&self) -> usize {
        max(1, self.config.parallel_requests)
    }

    pub fn update_status(&mut self, status: Status<Ctx>) {
        self.peers.insert(status.peer_id, status);
    }

    pub fn update_request(
        &mut self,
        request_id: OutboundRequestId,
        peer_id: PeerId,
        range: RangeInclusive<Ctx::Height>,
        excluded_peers: BTreeSet<PeerId>,
    ) {
        self.pending_consensus_ranges.remove(&request_id);
        self.pending_requests.insert(
            request_id,
            PendingRequestEntry {
                range,
                peer: peer_id,
                excluded_peers,
            },
        );
    }

    pub fn request_stage(&self, request_id: &OutboundRequestId) -> Option<RequestStage> {
        self.pending_requests.get(request_id).map(|_| {
            if self.pending_consensus_ranges.contains_key(request_id) {
                RequestStage::PendingConsensus
            } else {
                RequestStage::Inflight
            }
        })
    }

    pub fn mark_pending_consensus(
        &mut self,
        request_id: OutboundRequestId,
        range: RangeInclusive<Ctx::Height>,
    ) {
        assert!(self.pending_requests.contains_key(&request_id));
        self.pending_consensus_ranges
            .insert(request_id, vec![range]);
    }

    pub fn remove_request(
        &mut self,
        request_id: &OutboundRequestId,
    ) -> Option<PendingRequestEntry<Ctx::Height>> {
        self.pending_consensus_ranges.remove(request_id);
        self.pending_requests.remove(request_id)
    }

    pub fn clear_requests(&mut self) {
        self.pending_requests.clear();
        self.pending_consensus_ranges.clear();
    }

    /// Filter peers to only include those that can provide the given range of values, or at least a prefix of the range.
    ///
    /// If there is no peer with all requested values, select a peer that has a tip at or above the start of the range.
    /// Prefer peers that support batching (v2 sync protocol).
    /// Return the peer ID and the range of heights that the peer can provide.
    pub fn filter_peers_by_range(
        peers: &BTreeMap<PeerId, Status<Ctx>>,
        range: &RangeInclusive<Ctx::Height>,
        except: &BTreeSet<PeerId>,
    ) -> HashMap<PeerId, RangeInclusive<Ctx::Height>> {
        // Peers that can provide the whole range of values.
        let peers_with_whole_range = peers
            .iter()
            .filter(|(peer, status)| {
                status.history_min_height <= *range.start()
                    && *range.start() <= *range.end()
                    && *range.end() <= status.tip_height
                    && !except.contains(peer)
            })
            .map(|(peer, _)| (*peer, range.clone()))
            .collect::<HashMap<_, _>>();

        // Prefer peers that have the whole range of values in their history.
        if !peers_with_whole_range.is_empty() {
            peers_with_whole_range
        } else {
            // Otherwise, just get the peers that can provide a prefix of the range.
            peers
                .iter()
                .filter(|(peer, status)| {
                    status.history_min_height <= *range.start() && !except.contains(peer)
                })
                .map(|(peer, status)| (*peer, *range.start()..=status.tip_height))
                .filter(|(_, range)| !range.is_empty())
                .collect::<HashMap<_, _>>()
        }
    }

    /// Select at random a peer that can provide the given range of values,
    /// while excluding the given set of peers.
    pub fn random_peer_with_except(
        &mut self,
        range: &RangeInclusive<Ctx::Height>,
        except: &BTreeSet<PeerId>,
    ) -> Option<(PeerId, RangeInclusive<Ctx::Height>)> {
        // Filtered peers together with the range of heights they can provide.
        let peers_range = Self::filter_peers_by_range(&self.peers, range, except);

        // Select a peer at random.
        let peer_ids = peers_range.keys().cloned().collect::<Vec<_>>();
        self.peer_scorer
            .select_peer(&peer_ids, &mut self.rng)
            .map(|peer_id| (peer_id, peers_range.get(&peer_id).unwrap().clone()))
    }

    /// Same as [`Self::random_peer_with_except`] but without excluding any peer.
    pub fn random_peer_with(
        &mut self,
        range: &RangeInclusive<Ctx::Height>,
    ) -> Option<(PeerId, RangeInclusive<Ctx::Height>)>
    where
        Ctx: Context,
    {
        self.random_peer_with_except(range, &BTreeSet::new())
    }

    /// Get the request that contains the given height.
    ///
    /// Assumes a height cannot be in multiple pending requests.
    pub fn get_request_id_by(&self, height: Ctx::Height) -> Option<(OutboundRequestId, PeerId)> {
        self.pending_requests
            .iter()
            .find(|(request_id, _entry)| {
                self.pending_consensus_ranges
                    .get(*request_id)
                    .is_some_and(|ranges| ranges.iter().any(|range| range.contains(&height)))
            })
            .map(|(request_id, entry)| (request_id.clone(), entry.peer))
    }

    /// Release one locally rejected height from a response that is otherwise
    /// still pending consensus validation.
    pub fn release_pending_height(
        &mut self,
        height: Ctx::Height,
    ) -> Option<(OutboundRequestId, PeerId)> {
        let (request_id, peer) = self.get_request_id_by(height)?;
        let ranges = self
            .pending_consensus_ranges
            .get_mut(&request_id)
            .expect("located request must have pending consensus ranges");
        let range_index = ranges
            .iter()
            .position(|range| range.contains(&height))
            .expect("located request must contain the height");
        let range = ranges.remove(range_index);

        if *range.start() < height {
            ranges.push(
                *range.start()
                    ..=height
                        .decrement()
                        .expect("height above range start must be decrementable"),
            );
        }
        if height < *range.end() {
            ranges.push(height.increment()..=*range.end());
        }
        ranges.sort_by_key(|range| *range.start());

        if ranges.is_empty() {
            self.pending_consensus_ranges.remove(&request_id);
            self.pending_requests.remove(&request_id);
        }

        Some((request_id, peer))
    }

    /// Return the exact ranges currently covered by in-flight requests or by
    /// values waiting for consensus validation.
    pub fn covered_ranges(&self) -> Vec<RangeInclusive<Ctx::Height>> {
        self.pending_requests
            .iter()
            .flat_map(|(request_id, entry)| {
                self.pending_consensus_ranges
                    .get(request_id)
                    .cloned()
                    .unwrap_or_else(|| vec![entry.range.clone()])
            })
            .collect()
    }

    /// Return a new range of heights, trimming from the beginning any height
    /// that is validated by consensus.
    pub fn trim_validated_heights(
        &mut self,
        range: &RangeInclusive<Ctx::Height>,
    ) -> RangeInclusive<Ctx::Height> {
        let start = max(self.tip_height.increment(), *range.start());
        start..=*range.end()
    }

    /// Remove pending requests that are for heights that have already been validated by consensus.
    pub fn prune_pending_requests(&mut self) {
        for ranges in self.pending_consensus_ranges.values_mut() {
            ranges.retain_mut(|range| {
                if range.end() <= &self.tip_height {
                    return false;
                }

                if range.start() <= &self.tip_height {
                    *range = self.tip_height.increment()..=*range.end();
                }
                true
            });
        }

        let completed = self
            .pending_consensus_ranges
            .iter()
            .filter_map(|(request_id, ranges)| ranges.is_empty().then_some(request_id.clone()))
            .collect::<Vec<_>>();
        for request_id in completed {
            self.pending_consensus_ranges.remove(&request_id);
            self.pending_requests.remove(&request_id);
        }

        self.pending_requests.retain(|request_id, entry| {
            self.pending_consensus_ranges.contains_key(request_id)
                || entry.range.end() > &self.tip_height
        });
    }
}

pub fn height_allowance_snapshot<Ctx>(state: &State<Ctx>) -> HeightAllowanceSnapshot<Ctx::Height>
where
    Ctx: Context,
{
    let mut snapshot = HeightAllowanceSnapshot {
        inflight_heights: 0,
        pending_heights: 0,
        lowest_inflight_start: None,
        lowest_pending_start: None,
        tip_height: state.tip_height,
    };

    for (request_id, entry) in &state.pending_requests {
        match state.request_stage(request_id) {
            Some(RequestStage::Inflight) => {
                let height_count = entry.range.end().as_u64() - entry.range.start().as_u64() + 1;
                snapshot.inflight_heights += height_count;
                snapshot.lowest_inflight_start = Some(
                    snapshot
                        .lowest_inflight_start
                        .map_or(*entry.range.start(), |height| {
                            height.min(*entry.range.start())
                        }),
                );
            }
            Some(RequestStage::PendingConsensus) => {
                for range in state
                    .pending_consensus_ranges
                    .get(request_id)
                    .expect("pending request must have exact ranges")
                {
                    snapshot.pending_heights += range.end().as_u64() - range.start().as_u64() + 1;
                    snapshot.lowest_pending_start = Some(
                        snapshot
                            .lowest_pending_start
                            .map_or(*range.start(), |height| height.min(*range.start())),
                    );
                }
            }
            None => unreachable!("iterated request must have a stage"),
        }
    }

    snapshot
}
