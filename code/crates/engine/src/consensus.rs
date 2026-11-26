use core::fmt;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_recursion::async_recursion;
use async_trait::async_trait;
use derive_where::derive_where;
use eyre::eyre;
use itertools::Itertools;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::time::Instant;
use tracing::{debug, error, error_span, info, warn};

use malachitebft_codec as codec;
use malachitebft_config::ConsensusConfig;
use malachitebft_core_consensus::{
    Effect, LivenessMsg, PeerId, Resumable, Resume, SignedConsensusMsg, VoteExtensionError,
};
use malachitebft_core_types::{
    Context, Proposal, Round, Timeout, TimeoutKind, Timeouts, ValidatorProof, ValidatorSet, Value,
    ValueId, ValueOrigin, ValueResponse as CoreValueResponse, Vote,
};
use malachitebft_metrics::Metrics;
use malachitebft_signing::{Signer, Verifier, VerifierExt};
use malachitebft_sync::HeightStartType;

use crate::host::{HeightParams, HostMsg, HostRef, LocallyProposedValue, Next, ProposedValue};
use crate::network::{NetworkEvent, NetworkMsg, NetworkRef};
use crate::node::NodeRef;
use crate::sync::Msg as SyncMsg;
use crate::util::events::{Event, TxEvent};
use crate::util::failure::{hang_on_safety_failure, is_liveness_failure, stop_on_failure};
use crate::util::msg_buffer::MessageBuffer;
use crate::util::output_port::OutputPort;
use crate::util::ractor::cast_option_and_handle;
use crate::util::streaming::StreamMessage;
use crate::util::timers::{TimeoutElapsed, TimerScheduler};
use crate::wal::{Msg as WalMsg, WalAhead, WalEntry, WalRef};

pub use malachitebft_core_consensus::Error as ConsensusError;
pub use malachitebft_core_consensus::Params as ConsensusParams;
pub use malachitebft_core_consensus::State as ConsensusState;

mod replay_index;
pub mod state_dump;
use replay_index::ReplayIndex;
use state_dump::StateDump;

/// Codec for consensus messages.
///
/// This trait is automatically implemented for any type that implements:
/// - [`codec::Codec<Ctx::ProposalPart>`]
/// - [`codec::Codec<SignedConsensusMsg<Ctx>>`]
/// - [`codec::Codec<PolkaCertificate<Ctx>>`]
/// - [`codec::Codec<StreamMessage<Ctx::ProposalPart>>`]
/// - [`codec::Codec<ValidatorProof<Ctx>>`]
pub trait ConsensusCodec<Ctx>
where
    Ctx: Context,
    Self: codec::Codec<Ctx::ProposalPart>,
    Self: codec::Codec<SignedConsensusMsg<Ctx>>,
    Self: codec::Codec<LivenessMsg<Ctx>>,
    Self: codec::Codec<StreamMessage<Ctx::ProposalPart>>,
    Self: codec::Codec<ValidatorProof<Ctx>>,
{
}

impl<Ctx, Codec> ConsensusCodec<Ctx> for Codec
where
    Ctx: Context,
    Self: codec::Codec<Ctx::ProposalPart>,
    Self: codec::Codec<SignedConsensusMsg<Ctx>>,
    Self: codec::Codec<LivenessMsg<Ctx>>,
    Self: codec::Codec<StreamMessage<Ctx::ProposalPart>>,
    Self: codec::Codec<ValidatorProof<Ctx>>,
{
}

pub type ConsensusRef<Ctx> = ActorRef<Msg<Ctx>>;

pub struct Consensus<Ctx>
where
    Ctx: Context,
{
    ctx: Ctx,
    params: ConsensusParams<Ctx>,
    consensus_config: ConsensusConfig,
    verifier: Box<dyn Verifier<Ctx>>,
    signer: Option<Box<dyn Signer<Ctx>>>,
    network: NetworkRef<Ctx>,
    host: HostRef<Ctx>,
    wal: WalRef<Ctx>,
    node: NodeRef,
    sync: Arc<OutputPort<SyncMsg<Ctx>>>,
    metrics: Metrics,
    tx_event: TxEvent<Ctx>,
    span: tracing::Span,
}

pub type ConsensusMsg<Ctx> = Msg<Ctx>;

#[derive_where(Debug)]
pub enum Msg<Ctx: Context> {
    /// Start consensus for the given height and provided parameters.
    StartHeight(Ctx::Height, HeightParams<Ctx>),

    /// Received an event from the gossip layer
    NetworkEvent(NetworkEvent<Ctx>),

    /// A timeout has elapsed
    TimeoutElapsed(TimeoutElapsed<Timeout>),

    /// The proposal builder has built a value and can be used in a new proposal consensus message
    ProposeValue(LocallyProposedValue<Ctx>),

    /// Received and assembled the full value proposed by a validator
    ReceivedProposedValue(ProposedValue<Ctx>, ValueOrigin),

    /// Process a sync response
    ProcessSyncResponse(CoreValueResponse<Ctx>),

    /// Instructs consensus to restart at a given height with the provided parameters.
    ///
    /// On this input consensus resets the Write-Ahead Log.
    ///
    /// # Warning
    /// This operation should be used with extreme caution as it can lead to safety violations:
    /// 1. The application must clean all state associated with the height for which commit has failed
    /// 2. Since consensus resets its write-ahead log, the node may equivocate on proposals and votes
    ///    for the restarted height, potentially violating protocol safety
    RestartHeight(Ctx::Height, HeightParams<Ctx>),

    /// The application has confirmed that the decision has been committed.
    /// This triggers notifying the sync actor about the decided height.
    DecisionCommitted(Ctx::Height),

    /// Request to dump the current consensus state
    DumpState(RpcReplyPort<Option<StateDump<Ctx>>>),
}

impl<Ctx: Context> fmt::Display for Msg<Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Msg::StartHeight(height, params) => {
                write!(f, "StartHeight(height={height} params={params:?})")
            }
            Msg::NetworkEvent(event) => match event {
                NetworkEvent::Proposal(_, proposal) => write!(
                    f,
                    "NetworkEvent(Proposal height={} round={})",
                    proposal.height(),
                    proposal.round()
                ),
                NetworkEvent::ProposalPart(_, part) => {
                    write!(f, "NetworkEvent(ProposalPart sequence={})", part.sequence)
                }
                NetworkEvent::Vote(_, vote) => write!(
                    f,
                    "NetworkEvent(Vote height={} round={})",
                    vote.height(),
                    vote.round()
                ),
                _ => write!(f, "NetworkEvent"),
            },
            Msg::TimeoutElapsed(timeout) => write!(f, "TimeoutElapsed({})", timeout.display_key()),
            Msg::ProposeValue(value) => write!(
                f,
                "ProposeValue(height={} round={})",
                value.height, value.round
            ),
            Msg::ReceivedProposedValue(value, origin) => write!(
                f,
                "ReceivedProposedValue(height={} round={} origin={origin:?})",
                value.height, value.round
            ),
            Msg::ProcessSyncResponse(response) => {
                write!(
                    f,
                    "ProcessSyncResponse(peer={} height={} value={})",
                    response.peer, response.certificate.height, response.certificate.value_id
                )
            }
            Msg::RestartHeight(height, params) => {
                write!(f, "RestartHeight(height={height} params={params:?})")
            }
            Msg::DecisionCommitted(height) => write!(f, "DecisionCommitted(height={height})"),
            Msg::DumpState(_) => write!(f, "DumpState"),
        }
    }
}

impl<Ctx: Context> From<NetworkEvent<Ctx>> for Msg<Ctx> {
    fn from(event: NetworkEvent<Ctx>) -> Self {
        Self::NetworkEvent(event)
    }
}

type ConsensusInput<Ctx> = malachitebft_core_consensus::Input<Ctx>;

impl<Ctx: Context> From<TimeoutElapsed<Timeout>> for Msg<Ctx> {
    fn from(msg: TimeoutElapsed<Timeout>) -> Self {
        Msg::TimeoutElapsed(msg)
    }
}

type Timers = TimerScheduler<Timeout>;

enum WalFetch<Ctx: Context> {
    Entries(Vec<io::Result<WalEntry<Ctx>>>),
    Ahead(u64),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Phase {
    Unstarted,
    Ready,
    Running,
    Recovering,
}

/// Maximum number of messages to buffer while consensus is
/// not in the `Running` phase
const MAX_BUFFER_SIZE: usize = 1024;

pub struct State<Ctx: Context> {
    /// Scheduler for timers
    timers: Timers,

    /// Timeouts for various consensus steps
    timeouts: Ctx::Timeouts,

    /// The state of the consensus state machine,
    /// or `None` if consensus has not been started yet.
    consensus: Option<ConsensusState<Ctx>>,

    /// The set of peers we are connected to.
    connected_peers: BTreeSet<PeerId>,

    /// The current phase
    phase: Phase,

    /// Local signed messages are indexed before any recovery input drives the state machine.
    replay_index: Option<ReplayIndex<Ctx>>,

    /// Whether this node is in the validator set for the current height.
    /// Non-validators skip WAL writes since they have no equivocation risk.
    is_validator: bool,

    /// A buffer of messages that were received while
    /// consensus was not in the `Running` phase
    msg_buffer: MessageBuffer<Ctx>,
}

impl<Ctx> State<Ctx>
where
    Ctx: Context,
{
    pub fn height(&self) -> Ctx::Height {
        self.consensus
            .as_ref()
            .map(|c| c.height())
            .unwrap_or_default()
    }

    pub fn round(&self) -> Round {
        self.consensus
            .as_ref()
            .map(|c| c.round())
            .unwrap_or(Round::Nil)
    }

    fn set_phase(&mut self, phase: Phase) {
        if self.phase != phase {
            info!(prev = ?self.phase, new = ?phase, "Phase transition");
            self.phase = phase;
        }
    }
}

struct HandlerState<'a, Ctx: Context> {
    phase: Phase,
    replay_index: Option<&'a mut ReplayIndex<Ctx>>,
    is_validator: bool,
    timers: &'a mut Timers,
    timeouts: Ctx::Timeouts,
}

impl<Ctx> Consensus<Ctx>
where
    Ctx: Context,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        ctx: Ctx,
        params: ConsensusParams<Ctx>,
        consensus_config: ConsensusConfig,
        verifier: Box<dyn Verifier<Ctx>>,
        signer: Option<Box<dyn Signer<Ctx>>>,
        network: NetworkRef<Ctx>,
        host: HostRef<Ctx>,
        wal: WalRef<Ctx>,
        node: NodeRef,
        sync: Arc<OutputPort<SyncMsg<Ctx>>>,
        metrics: Metrics,
        tx_event: TxEvent<Ctx>,
        span: tracing::Span,
    ) -> Result<ActorRef<Msg<Ctx>>, ractor::SpawnErr> {
        let node = Self {
            ctx,
            params,
            consensus_config,
            verifier,
            signer,
            network,
            host,
            wal,
            node,
            sync,
            metrics,
            tx_event,
            span,
        };

        let (actor_ref, _) = Actor::spawn(None, node, ()).await?;
        Ok(actor_ref)
    }

    fn signer(&self) -> &dyn Signer<Ctx> {
        self.signer.as_deref().expect(
            "BUG: signing effect produced but no signer configured; \
             this node should not be a validator",
        )
    }

    async fn process_input(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
        input: ConsensusInput<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        malachitebft_core_consensus::process!(
            input: input,
            state: state.consensus.as_mut().expect("Consensus not started"),
            metrics: &self.metrics,
            try_with: effect => {
                let handler_state = HandlerState {
                    phase: state.phase,
                    replay_index: state.replay_index.as_mut(),
                    is_validator: state.is_validator,
                    timers: &mut state.timers,
                    timeouts: state.timeouts,
                };

                match self.handle_effect(myself, handler_state, effect).await {
                    Ok(resume) => Ok(resume),
                    Err(error) => {
                        error!("Error when processing effect: {error:?}");
                        Ok(Resume::Continue)
                    }
                }
            }
        )
    }

    #[async_recursion]
    async fn process_buffered_msgs(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
        is_restart: bool,
    ) -> Result<(), ActorProcessingErr> {
        if state.msg_buffer.is_empty() {
            return Ok(());
        }

        if is_restart {
            state.msg_buffer = MessageBuffer::new(MAX_BUFFER_SIZE);
        }

        info!(count = %state.msg_buffer.len(), "Replaying buffered messages");

        while let Some(msg) = state.msg_buffer.pop() {
            debug!("Replaying buffered message: {msg}");

            if let Err(error) = self.handle_msg(myself.clone(), state, msg).await {
                if is_liveness_failure(&error) {
                    return Err(error);
                }
                error!("Error when handling buffered message: {error:?}");
            }
        }

        Ok(())
    }

    async fn handle_msg(
        &self,
        myself: ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
        msg: Msg<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        let is_restart = matches!(msg, Msg::RestartHeight(_, _));

        match msg {
            Msg::StartHeight(height, params) | Msg::RestartHeight(height, params) => {
                let previous_height = state.consensus.as_ref().map(ConsensusState::height);
                // Check that the validator set is provided and that it is not empty
                if params.validator_set.count() == 0 {
                    return Err(eyre!("Validator set for height {height} is empty").into());
                }

                // Initialize consensus state if this is the first height we start
                if state.consensus.is_none() {
                    state.consensus = Some(ConsensusState::new(
                        self.ctx.clone(),
                        height,
                        params.validator_set.clone(),
                        self.params.clone(),
                        self.consensus_config.queue_capacity,
                        self.consensus_config.queue_per_height_capacity,
                    ));
                }

                self.tx_event
                    .send(|| Event::StartedHeight(height, is_restart));

                // Determine if this node is an active validator for this height.
                // Mirrors ConsensusState::is_active_validator(): both `enabled` and
                // validator set membership must hold.
                state.is_validator = self.params.enabled
                    && params
                        .validator_set
                        .get_by_address(&self.params.address)
                        .is_some();

                // Push validator set to network layer
                if let Err(e) = self
                    .network
                    .cast(NetworkMsg::UpdateValidatorSet(params.validator_set.clone()))
                {
                    error!(%height, "Error pushing validator set to network layer: {e}");
                }

                // Confirm the WAL height even when signing is disabled. A node can return to
                // validator mode at the same height and still needs its existing records.
                let mut signing_enabled = self.params.enabled;
                let wal_entries = if is_restart {
                    stop_on_failure(self.wal_reset(height), |e| {
                        format!("wal_reset at height {height} failed: {e}")
                    })
                    .await?;

                    vec![]
                } else {
                    let entries = stop_on_failure(self.wal_fetch(height), |e| {
                        format!("wal_fetch at height {height} failed: {e}")
                    })
                    .await?;
                    match entries {
                        WalFetch::Entries(entries) if state.is_validator => entries,
                        WalFetch::Entries(_) => Vec::new(),
                        WalFetch::Ahead(wal_height) => {
                            warn!(%height, wal_height, "Application is behind the WAL; catching up with signing disabled");
                            state.is_validator = false;
                            signing_enabled = false;
                            Vec::new()
                        }
                    }
                };
                state
                    .consensus
                    .as_mut()
                    .expect("Consensus was initialized")
                    .set_signing_enabled(signing_enabled);

                // Update the timeouts
                state.timeouts = params.timeouts;

                // Validate the entire replay batch and index local signing choices before even
                // StartHeight can run effects. The WAL worker has already sealed these bytes.
                let (wal_entries, index) = hang_on_safety_failure(
                    &self.node,
                    async { self.prepare_replay(wal_entries) },
                    |error| format!("WAL replay at height {height} failed: {error}"),
                )
                .await;
                if !wal_entries.is_empty() {
                    state.set_phase(Phase::Recovering);
                }
                if !wal_entries.is_empty()
                    || previous_height != Some(height)
                    || is_restart
                    || state.replay_index.is_none()
                {
                    state.replay_index = Some(index);
                }

                // Start consensus for the given height
                let result = self
                    .process_input(
                        &myself,
                        state,
                        ConsensusInput::StartHeight(
                            height,
                            params.validator_set,
                            is_restart,
                            params.target_time,
                        ),
                    )
                    .await;

                if state.phase == Phase::Recovering {
                    hang_on_safety_failure(&self.node, async { result }, |error| {
                        format!("Could not start WAL replay at height {height}: {error}")
                    })
                    .await;
                } else if let Err(e) = result {
                    error!(%height, "Error when starting height: {e}");
                }

                // A validator must reconstruct its recorded crash state before
                // sync can advance the height or apply a certificate.
                if !wal_entries.is_empty() {
                    hang_on_safety_failure(
                        &self.node,
                        self.wal_replay(&myself, state, height, wal_entries),
                        |e| format!("WAL replay at height {height} failed: {e}"),
                    )
                    .await;
                }

                // Set the phase to `Running` now that we have replayed the WAL
                state.set_phase(Phase::Running);

                // Notify the sync actor that we have started a new height.
                // NOTE: SyncMsg::Decided is sent separately via Msg::DecisionCommitted,
                // which fires when the app confirms the decision commit (after Effect::Decide).
                let start_type = HeightStartType::from_is_restart(is_restart);

                self.sync.send(SyncMsg::StartedHeight(height, start_type));

                // Process any buffered messages, now that we are in the `Running` phase
                self.process_buffered_msgs(&myself, state, is_restart)
                    .await?;

                Ok(())
            }

            Msg::ProposeValue(value) => {
                let result = self
                    .process_input(&myself, state, ConsensusInput::Propose(value.clone()))
                    .await;

                if let Err(e) = result {
                    error!(
                        height = %value.height, round = %value.round,
                        "Error when processing ProposeValue message: {e}"
                    );
                }

                self.tx_event.send(|| Event::ProposedValue(value));

                Ok(())
            }

            Msg::NetworkEvent(event) => {
                match event {
                    NetworkEvent::Listening(address) => {
                        info!(%address, "Listening");

                        if state.phase == Phase::Unstarted {
                            state.set_phase(Phase::Ready);

                            self.host.call_and_forward(
                                |reply_to| HostMsg::ConsensusReady { reply_to },
                                &myself,
                                |(height, params)| ConsensusMsg::StartHeight(height, params),
                                None,
                            )?;
                        }
                    }

                    NetworkEvent::PeerConnected(peer_id) => {
                        if !state.connected_peers.insert(peer_id) {
                            // We already saw that peer, ignoring...
                            return Ok(());
                        }

                        info!(%peer_id, total = %state.connected_peers.len(), "Connected to peer");

                        self.metrics.connected_peers.inc();
                    }

                    NetworkEvent::PeerDisconnected(peer_id) => {
                        info!(%peer_id, "Disconnected from peer");

                        if state.connected_peers.remove(&peer_id) {
                            self.metrics.connected_peers.dec();
                        }
                    }

                    NetworkEvent::Vote(from, vote) => {
                        self.tx_event
                            .send(|| Event::Received(SignedConsensusMsg::Vote(vote.clone())));

                        if let Err(e) = self
                            .process_input(&myself, state, ConsensusInput::Vote(vote))
                            .await
                        {
                            error!(%from, "Error when processing vote: {e}");
                        }
                    }

                    NetworkEvent::Proposal(from, proposal) => {
                        self.tx_event.send(|| {
                            Event::Received(SignedConsensusMsg::Proposal(proposal.clone()))
                        });

                        if self.params.value_payload.parts_only() {
                            error!(%from, "Properly configured peer should never send proposal messages in BlockPart mode");
                            return Ok(());
                        }

                        if let Err(e) = self
                            .process_input(&myself, state, ConsensusInput::Proposal(proposal))
                            .await
                        {
                            error!(%from, "Error when processing proposal: {e}");
                        }
                    }

                    NetworkEvent::PolkaCertificate(from, certificate) => {
                        if let Err(e) = self
                            .process_input(
                                &myself,
                                state,
                                ConsensusInput::PolkaCertificate(certificate),
                            )
                            .await
                        {
                            error!(%from, "Error when processing polka certificate: {e}");
                        }
                    }

                    NetworkEvent::RoundCertificate(from, certificate) => {
                        if let Err(e) = self
                            .process_input(
                                &myself,
                                state,
                                ConsensusInput::RoundCertificate(certificate),
                            )
                            .await
                        {
                            error!(%from, "Error when processing round certificate: {e}");
                        }
                    }

                    NetworkEvent::ProposalPart(from, part) => {
                        if self.params.value_payload.proposal_only() {
                            error!(%from, "Properly configured peer should never send proposal part messages in Proposal mode");
                            return Ok(());
                        }

                        self.host
                            .call_and_forward(
                                |reply_to| HostMsg::ReceivedProposalPart {
                                    from,
                                    part,
                                    reply_to,
                                },
                                &myself,
                                move |value| {
                                    Msg::ReceivedProposedValue(value, ValueOrigin::Consensus)
                                },
                                None,
                            )
                            .map_err(|e| {
                                eyre!("Error when forwarding proposal parts to host: {e}")
                            })?;
                    }

                    NetworkEvent::ValidatorProofReceived { peer_id, proof } => {
                        use malachitebft_network::validator_proof::ProofVerificationResult;

                        // Note: peer_id match is already verified in network layer

                        // Verify signature using public_key in proof
                        let verification = self.verifier.verify_validator_proof(&proof).await;

                        let (result, public_key_bytes) = match verification {
                            Ok(v) if v.is_valid() => {
                                debug!(
                                    %peer_id,
                                    public_key = %hex::encode(&proof.public_key),
                                    "Valid validator proof received"
                                );
                                (
                                    ProofVerificationResult::Valid,
                                    Some(proof.public_key.clone()),
                                )
                            }
                            Ok(_) => {
                                warn!(%peer_id, "Invalid validator proof signature");
                                (ProofVerificationResult::Invalid, None)
                            }
                            Err(e) => {
                                warn!(%peer_id, "Error verifying validator proof: {e}");
                                (ProofVerificationResult::Invalid, None)
                            }
                        };

                        // Send verification result to network layer
                        if let Err(e) = self.network.cast(NetworkMsg::ValidatorProofVerified {
                            peer_id,
                            result,
                            public_key: public_key_bytes,
                        }) {
                            error!(%peer_id, ?result, "Error sending validator proof result: {e}");
                        }
                    }

                    _ => {}
                }

                Ok(())
            }

            Msg::TimeoutElapsed(elapsed) => {
                let Some(timeout) = state.timers.intercept_timer_msg(elapsed) else {
                    // Timer was cancelled or already processed, ignore
                    return Ok(());
                };

                if let Err(e) = self.timeout_elapsed(&myself, state, timeout).await {
                    error!("Error when processing TimeoutElapsed message: {e:?}");
                }

                Ok(())
            }

            Msg::ReceivedProposedValue(value, origin) => {
                self.tx_event
                    .send(|| Event::ReceivedProposedValue(value.clone(), origin));

                let result = self
                    .process_input(&myself, state, ConsensusInput::ProposedValue(value, origin))
                    .await;

                if let Err(e) = result {
                    error!("Error when processing ReceivedProposedValue message: {e}");
                }

                Ok(())
            }

            Msg::ProcessSyncResponse(response) => {
                let height = response.certificate.height;
                let round = response.certificate.round;
                let value = response.certificate.value_id.clone();
                let peer = response.peer;

                debug!(
                    %height, %round, %value, %peer,
                    "Processing sync response"
                );

                if let Err(e) = self
                    .process_input(&myself, state, ConsensusInput::SyncValueResponse(response))
                    .await
                {
                    error!(
                        %height, %round, %value, %peer,
                        "Failed to process sync response: {e:?}"
                    );
                }

                Ok(())
            }

            Msg::DecisionCommitted(height) => {
                // The application has confirmed that the decision has been committed.
                // Notify the sync actor so it can advertise this height to peers.
                self.sync.send(SyncMsg::Decided(height));

                debug_assert_eq!(
                    height,
                    state.height(),
                    "DecisionCommitted height must match current state height"
                );

                Ok(())
            }

            Msg::DumpState(reply_to) => {
                let state_dump = if let Some(consensus) = &state.consensus {
                    info!(
                        height = %consensus.height(),
                        round  = %consensus.round(),
                        "Dumping consensus state"
                    );

                    Some(StateDump::new(consensus))
                } else {
                    info!("Dumping consensus state: not started");
                    None
                };

                if let Err(e) = reply_to.send(state_dump) {
                    error!("Failed to reply with state dump: {e}");
                }

                Ok(())
            }
        }
    }

    async fn timeout_elapsed(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
        timeout: Timeout,
    ) -> Result<(), ActorProcessingErr> {
        // Make sure the associated timer is cancelled
        state.timers.cancel(&timeout);

        // Print debug information if the timeout is for a prevote or precommit
        if matches!(
            timeout.kind,
            TimeoutKind::Prevote | TimeoutKind::Precommit | TimeoutKind::Rebroadcast
        ) {
            info!(step = ?timeout.kind, "Timeout elapsed");

            state.consensus.as_ref().inspect(|consensus| {
                consensus.print_state();
            });
        }

        // Process the timeout event
        self.process_input(myself, state, ConsensusInput::TimeoutElapsed(timeout))
            .await?;

        Ok(())
    }

    async fn wal_reset(&self, height: Ctx::Height) -> Result<(), ActorProcessingErr> {
        let result = ractor::call!(self.wal, WalMsg::Reset, height);

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                let message = format!("Failed to reset WAL for height {height}: {e}");
                Err(self.report_wal_reset_error(message))
            }
            Err(e) => {
                let message =
                    format!("Failed to send Reset command to WAL actor for height {height}: {e}");
                Err(self.report_wal_reset_error(message))
            }
        }
    }

    async fn wal_fetch(&self, height: Ctx::Height) -> Result<WalFetch<Ctx>, ActorProcessingErr> {
        let result = ractor::call!(self.wal, WalMsg::StartedHeight, height);

        match result {
            Ok(Ok(entries)) if entries.is_empty() => {
                debug!(%height, "No WAL entries to replay");

                // Nothing to replay
                Ok(WalFetch::Entries(Vec::new()))
            }

            Ok(Ok(entries)) => {
                info!("Found {} WAL entries", entries.len());

                Ok(WalFetch::Entries(entries))
            }

            Ok(Err(e)) => {
                if let Some(ahead) = e.downcast_ref::<WalAhead>() {
                    return Ok(WalFetch::Ahead(ahead.0));
                }
                let message = format!("Failed to fetch WAL entries for height {height}: {e}");
                Err(self.report_wal_reset_error(message))
            }
            Err(e) => {
                let message = format!(
                    "Failed to send StartedHeight command to WAL actor for height {height}: {e}"
                );
                Err(self.report_wal_reset_error(message))
            }
        }
    }

    fn report_wal_reset_error(&self, message: String) -> ActorProcessingErr {
        error!(%message, "WAL startup operation failed");
        let event_message = message.clone();
        self.tx_event
            .send(move || Event::WalResetError(Arc::new(eyre!(event_message))));
        eyre!(message).into()
    }

    fn prepare_replay(
        &self,
        entries: Vec<io::Result<WalEntry<Ctx>>>,
    ) -> io::Result<(Vec<WalEntry<Ctx>>, ReplayIndex<Ctx>)> {
        let result = entries
            .into_iter()
            .collect::<io::Result<Vec<_>>>()
            .and_then(|entries| {
                let index = ReplayIndex::new(&entries, &self.params.address)?;
                Ok((entries, index))
            });
        result.map_err(|error| {
            error!(%error, "Corrupted WAL entry encountered before replay");
            let reported = Arc::new(io::Error::new(error.kind(), error.to_string()));
            self.tx_event.send(|| Event::WalCorrupted(reported));
            error
        })
    }

    async fn wal_replay(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
        height: Ctx::Height,
        entries: Vec<WalEntry<Ctx>>,
    ) -> Result<(), ActorProcessingErr> {
        use SignedConsensusMsg::*;

        assert_eq!(state.phase, Phase::Recovering);

        if entries.is_empty() {
            return Ok(());
        }

        info!("Replaying {} WAL entries", entries.len());

        self.tx_event
            .send(|| Event::WalReplayBegin(height, entries.len()));

        for entry in entries {
            self.tx_event.send(|| Event::WalReplayEntry(entry.clone()));

            match entry {
                WalEntry::ConsensusMsg(Vote(vote)) => {
                    info!("Replaying vote: {vote:?}");

                    if let Err(e) = self
                        .process_input(myself, state, ConsensusInput::Vote(vote))
                        .await
                    {
                        error!("Error when replaying vote: {e}");

                        return Err(self.report_wal_replay_error(e));
                    }
                }

                WalEntry::ConsensusMsg(Proposal(proposal)) => {
                    info!("Replaying proposal: {proposal:?}");

                    if let Err(e) = self
                        .process_input(myself, state, ConsensusInput::Proposal(proposal))
                        .await
                    {
                        error!("Error when replaying Proposal: {e}");

                        return Err(self.report_wal_replay_error(e));
                    }
                }

                WalEntry::Timeout(timeout) => {
                    info!("Replaying timeout: {timeout:?}");

                    if let Err(e) = self.timeout_elapsed(myself, state, timeout).await {
                        error!("Error when replaying TimeoutElapsed: {e}");

                        return Err(self.report_wal_replay_error(e));
                    }
                }

                WalEntry::ProposedValue(value) => {
                    info!("Replaying proposed value: {value:?}");

                    if let Err(e) = self
                        .process_input(
                            myself,
                            state,
                            ConsensusInput::ProposedValue(value, ValueOrigin::Consensus),
                        )
                        .await
                    {
                        error!("Error when replaying LocallyProposedValue: {e}");

                        return Err(self.report_wal_replay_error(e));
                    }
                }

                WalEntry::PolkaCertificate(certificate) => {
                    info!("Replaying polka certificate: {certificate:?}");

                    if let Err(e) = self
                        .process_input(myself, state, ConsensusInput::PolkaCertificate(certificate))
                        .await
                    {
                        error!("Error when replaying PolkaCertificate: {e}");

                        return Err(self.report_wal_replay_error(e));
                    }
                }
            }
        }

        self.tx_event.send(|| Event::WalReplayDone(state.height()));

        Ok(())
    }

    fn report_wal_replay_error(&self, error: ActorProcessingErr) -> ActorProcessingErr {
        match error.downcast::<ConsensusError<Ctx>>() {
            Ok(error) => {
                let event_error = Arc::new(*error);
                self.tx_event.send({
                    let event_error = Arc::clone(&event_error);
                    || Event::WalReplayError(event_error)
                });
                eyre!(event_error.to_string()).into()
            }
            Err(error) => error,
        }
    }

    fn get_value(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        height: Ctx::Height,
        round: Round,
        timeout: Duration,
    ) -> Result<(), ActorProcessingErr> {
        // Call `GetValue` on the Host actor, and forward the reply
        // to the current actor, wrapping it in `Msg::ProposeValue`.
        self.host.call_and_forward(
            |reply_to| HostMsg::GetValue {
                height,
                round,
                timeout,
                reply_to,
            },
            myself,
            Msg::<Ctx>::ProposeValue,
            None,
        )?;

        Ok(())
    }

    async fn extend_vote(
        &self,
        height: Ctx::Height,
        round: Round,
        value_id: ValueId<Ctx>,
    ) -> Result<Option<Ctx::Extension>, ActorProcessingErr> {
        ractor::call!(self.host, |reply_to| HostMsg::ExtendVote {
            height,
            round,
            value_id,
            reply_to
        })
        .map_err(|e| eyre!("Failed to get earliest block height: {e:?}").into())
    }

    async fn verify_vote_extension(
        &self,
        height: Ctx::Height,
        round: Round,
        value_id: ValueId<Ctx>,
        extension: Ctx::Extension,
    ) -> Result<Result<(), VoteExtensionError>, ActorProcessingErr> {
        ractor::call!(self.host, |reply_to| HostMsg::VerifyVoteExtension {
            height,
            round,
            value_id,
            extension,
            reply_to
        })
        .map_err(|e| eyre!("Failed to verify vote extension: {e:?}").into())
    }

    async fn wal_append(
        &self,
        height: Ctx::Height,
        entry: WalEntry<Ctx>,
        phase: Phase,
        is_validator: bool,
    ) -> Result<(), ActorProcessingErr> {
        if phase == Phase::Recovering || !is_validator {
            return Ok(());
        }

        hang_on_safety_failure(
            &self.node,
            async {
                match ractor::call!(self.wal, WalMsg::Append, height, entry) {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(format!("Failed to append entry to WAL: {error}")),
                    Err(error) => Err(format!(
                        "Failed to send Append command to WAL actor: {error}"
                    )),
                }
            },
            |reason| reason,
        )
        .await;

        Ok(())
    }

    async fn wal_flush(&self, phase: Phase, is_validator: bool) -> Result<(), ActorProcessingErr> {
        if phase == Phase::Recovering || !is_validator {
            return Ok(());
        }

        hang_on_safety_failure(
            &self.node,
            async {
                match ractor::call!(self.wal, WalMsg::Flush) {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(format!("Failed to flush WAL to disk: {error}")),
                    Err(error) => Err(format!(
                        "Failed to send Flush command to WAL actor: {error}"
                    )),
                }
            },
            |reason| reason,
        )
        .await;

        Ok(())
    }

    async fn handle_effect(
        &self,
        myself: &ActorRef<Msg<Ctx>>,
        state: HandlerState<'_, Ctx>,
        effect: Effect<Ctx>,
    ) -> Result<Resume<Ctx>, ActorProcessingErr> {
        match effect {
            Effect::CancelAllTimeouts(r) => {
                state.timers.cancel_all();
                Ok(r.resume_with(()))
            }

            Effect::CancelTimeout(timeout, r) => {
                state.timers.cancel(&timeout);
                Ok(r.resume_with(()))
            }

            Effect::ScheduleTimeout(timeout, r) => {
                let duration = state.timeouts.duration_for(timeout);
                state.timers.start_timer(timeout, duration);

                Ok(r.resume_with(()))
            }

            Effect::StartRound(height, round, proposer, role, r) => {
                self.wal_flush(state.phase, state.is_validator).await?;

                let undecided_values =
                    ractor::call!(self.host, |reply_to| HostMsg::StartedRound {
                        height,
                        round,
                        proposer: proposer.clone(),
                        role,
                        reply_to,
                    })?;

                for value in undecided_values {
                    let _ = myself.cast(Msg::ReceivedProposedValue(value, ValueOrigin::Consensus));
                }

                self.tx_event
                    .send(|| Event::StartedRound(height, round, proposer, role));

                Ok(r.resume_with(()))
            }

            Effect::SignProposal(proposal, r) => {
                // PartsOnly also signs internal placeholders for other proposers. Those are
                // not this validator's signing choices and must stay out of its signing index.
                let local = proposal.validator_address() == &self.params.address;
                if local && !state.is_validator {
                    return Err(eyre!("Local proposal signing is disabled for this height").into());
                }
                let index = state.replay_index.filter(|_| local);
                if let Some(index) = index.as_ref() {
                    let recorded = hang_on_safety_failure(
                        &self.node,
                        async { index.proposal(&proposal) },
                        |error| error.to_string(),
                    )
                    .await;
                    if let Some(recorded) = recorded {
                        return Ok(r.resume_with(recorded));
                    }
                }
                let start = Instant::now();

                let signed_proposal = self.signer().sign_proposal(proposal).await?;

                self.metrics
                    .signature_signing_time
                    .observe(start.elapsed().as_secs_f64());

                // A reproposal can restream before core consensus reaches WalAppend, and in
                // PartsOnly mode it may never issue that append for the signed proposal.
                if local
                    && (state.phase == Phase::Recovering
                        || signed_proposal.pol_round().is_defined())
                {
                    self.wal_append(
                        signed_proposal.height(),
                        WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(
                            signed_proposal.clone(),
                        )),
                        Phase::Running,
                        state.is_validator,
                    )
                    .await?;
                    self.wal_flush(Phase::Running, state.is_validator).await?;
                }
                if let Some(index) = index {
                    index.insert_proposal(signed_proposal.clone());
                }

                Ok(r.resume_with(signed_proposal))
            }

            Effect::SignVote(vote, r) => {
                if !state.is_validator {
                    return Err(eyre!("Vote signing is disabled for this height").into());
                }
                if let Some(index) = state.replay_index.as_ref() {
                    let recorded =
                        hang_on_safety_failure(&self.node, async { index.vote(&vote) }, |error| {
                            error.to_string()
                        })
                        .await;
                    if let Some(recorded) = recorded {
                        return Ok(r.resume_with(recorded));
                    }
                }
                let start = Instant::now();

                let signed_vote = self.signer().sign_vote(vote).await?;

                self.metrics
                    .signature_signing_time
                    .observe(start.elapsed().as_secs_f64());

                if let Some(index) = state.replay_index {
                    if state.phase == Phase::Recovering {
                        self.wal_append(
                            signed_vote.height(),
                            WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(signed_vote.clone())),
                            Phase::Running,
                            state.is_validator,
                        )
                        .await?;
                        self.wal_flush(Phase::Running, state.is_validator).await?;
                    }
                    index.insert_vote(signed_vote.clone());
                }

                Ok(r.resume_with(signed_vote))
            }

            Effect::VerifySignature(msg, pk, r) => {
                use malachitebft_core_consensus::ConsensusMsg as Msg;

                let start = Instant::now();

                let result = match msg.message {
                    Msg::Vote(v) => {
                        self.verifier
                            .verify_signed_vote(&v, &msg.signature, &pk)
                            .await?
                    }
                    Msg::Proposal(p) => {
                        self.verifier
                            .verify_signed_proposal(&p, &msg.signature, &pk)
                            .await?
                    }
                };

                self.metrics
                    .signature_verification_time
                    .observe(start.elapsed().as_secs_f64());

                Ok(r.resume_with(result.is_valid()))
            }

            Effect::VerifyCommitCertificate(certificate, validator_set, thresholds, r) => {
                let result = self
                    .verifier
                    .verify_commit_certificate(&self.ctx, &certificate, &validator_set, thresholds)
                    .await;

                Ok(r.resume_with(result))
            }

            Effect::VerifyPolkaCertificate(certificate, validator_set, thresholds, r) => {
                let result = self
                    .verifier
                    .verify_polka_certificate(&self.ctx, &certificate, &validator_set, thresholds)
                    .await;

                Ok(r.resume_with(result))
            }

            Effect::VerifyRoundCertificate(certificate, validator_set, thresholds, r) => {
                let result = self
                    .verifier
                    .verify_round_certificate(&self.ctx, &certificate, &validator_set, thresholds)
                    .await;

                Ok(r.resume_with(result))
            }

            Effect::ExtendVote(height, round, value_id, r) => {
                if let Some(index) = state.replay_index.as_ref() {
                    let extension = hang_on_safety_failure(
                        &self.node,
                        async { index.extension(height, round, &value_id) },
                        |error| error.to_string(),
                    )
                    .await;
                    if let Some(extension) = extension {
                        return Ok(r.resume_with(extension));
                    }
                }
                if let Some(extension) = self.extend_vote(height, round, value_id).await? {
                    let signed_extension = self
                        .signer()
                        .sign_vote_extension(extension)
                        .await
                        .inspect_err(|e| {
                            error!("Failed to sign vote extension: {e}");
                        })
                        .ok(); // Discard the vote extension if signing fails

                    Ok(r.resume_with(signed_extension))
                } else {
                    Ok(r.resume_with(None))
                }
            }

            Effect::VerifyVoteExtension(height, round, value_id, signed_extension, pk, r) => {
                let result = self
                    .verifier
                    .verify_signed_vote_extension(
                        &signed_extension.message,
                        &signed_extension.signature,
                        &pk,
                    )
                    .await?;

                if result.is_invalid() {
                    return Ok(r.resume_with(Err(VoteExtensionError::InvalidSignature)));
                }

                let result = self
                    .verify_vote_extension(height, round, value_id, signed_extension.message)
                    .await?;

                Ok(r.resume_with(result))
            }

            Effect::PublishConsensusMsg(msg, r) => {
                // Sync the WAL to disk before we broadcast the message
                // NOTE: The message has already been append to the WAL by the `WalAppend` effect.
                self.wal_flush(state.phase, state.is_validator).await?;

                // Notify any subscribers that we are about to publish a message
                self.tx_event.send(|| Event::Published(msg.clone()));

                self.network
                    .cast(NetworkMsg::PublishConsensusMsg(msg))
                    .map_err(|e| eyre!("Error when broadcasting consensus message: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::PublishLivenessMsg(msg, r) => {
                self.wal_flush(state.phase, state.is_validator).await?;
                match msg {
                    LivenessMsg::Vote(ref msg) => {
                        self.tx_event.send(|| Event::RepublishVote(msg.clone()));
                    }
                    LivenessMsg::PolkaCertificate(ref certificate) => {
                        self.tx_event
                            .send(|| Event::PolkaCertificate(certificate.clone()));
                    }
                    LivenessMsg::SkipRoundCertificate(ref certificate) => {
                        self.tx_event
                            .send(|| Event::SkipRoundCertificate(certificate.clone()));
                    }
                }

                self.network
                    .cast(NetworkMsg::PublishLivenessMsg(msg))
                    .map_err(|e| eyre!("Error when broadcasting liveness message: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::RepublishVote(msg, r) => {
                self.wal_flush(state.phase, state.is_validator).await?;
                // Notify any subscribers that we are about to rebroadcast a vote
                self.tx_event.send(|| Event::RepublishVote(msg.clone()));

                self.network
                    .cast(NetworkMsg::PublishLivenessMsg(LivenessMsg::Vote(msg)))
                    .map_err(|e| eyre!("Error when rebroadcasting vote message: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::RepublishRoundCertificate(certificate, r) => {
                self.wal_flush(state.phase, state.is_validator).await?;
                // Notify any subscribers that we are about to rebroadcast a round certificate
                self.tx_event
                    .send(|| Event::RebroadcastRoundCertificate(certificate.clone()));

                self.network
                    .cast(NetworkMsg::PublishLivenessMsg(
                        LivenessMsg::SkipRoundCertificate(certificate),
                    ))
                    .map_err(|e| {
                        eyre!("Error when rebroadcasting round certificate message: {e:?}")
                    })?;

                Ok(r.resume_with(()))
            }

            Effect::GetValue(height, round, timeout, r) => {
                let timeout_duration = state.timeouts.duration_for(timeout);

                self.get_value(myself, height, round, timeout_duration)
                    .map_err(|e| {
                        eyre!("Error when asking application for value to propose: {e:?}")
                    })?;

                Ok(r.resume_with(()))
            }

            Effect::RestreamProposal(height, round, valid_round, address, value_id, r) => {
                self.wal_flush(state.phase, state.is_validator).await?;
                self.host
                    .cast(HostMsg::RestreamValue {
                        height,
                        round,
                        valid_round,
                        address,
                        value_id,
                    })
                    .map_err(|e| eyre!("Error when sending decided value to host: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::Decide(certificate, extensions, r) => {
                assert!(!certificate.commit_signatures.is_empty());

                // Sync the WAL to disk before we decide the value
                self.wal_flush(state.phase, state.is_validator).await?;

                // Notify any subscribers about the decided value
                self.tx_event.send(|| Event::Decided {
                    commit_certificate: certificate.clone(),
                });

                let height = certificate.height;

                // Notify the host about the decided value and wait for commit confirmation.
                // When the app replies, the forwarded DecisionCommitted message will notify
                // the sync actor, ensuring the decision is committed before we advertise it.
                self.host
                    .call_and_forward(
                        |reply_to| HostMsg::Decided {
                            certificate,
                            extensions,
                            reply_to,
                        },
                        myself,
                        move |()| Msg::<Ctx>::DecisionCommitted(height),
                        None,
                    )
                    .map_err(|e| eyre!("Error when sending decided value to host: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::Finalize(certificate, extensions, evidence, r) => {
                assert!(!certificate.commit_signatures.is_empty());
                self.wal_flush(state.phase, state.is_validator).await?;

                // Update metrics for equivocation evidence
                let proposal_evidence_count = evidence
                    .proposals
                    .iter()
                    .map(|(_, proposals)| proposals.len())
                    .sum::<usize>();
                let vote_evidence_count = evidence
                    .votes
                    .iter()
                    .map(|(_, votes)| votes.len())
                    .sum::<usize>();
                if proposal_evidence_count > 0 {
                    self.metrics
                        .equivocation_proposals
                        .inc_by(proposal_evidence_count as u64);
                }
                if vote_evidence_count > 0 {
                    self.metrics
                        .equivocation_votes
                        .inc_by(vote_evidence_count as u64);
                }

                if proposal_evidence_count > 0 || vote_evidence_count > 0 {
                    let validator_addresses = evidence
                        .proposals
                        .iter()
                        .map(|(addr, _)| addr.to_string())
                        .chain(evidence.votes.iter().map(|(addr, _)| addr.to_string()))
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .join(", ");

                    warn!(
                        height = %certificate.height,
                        round = %certificate.round,
                        proposal_evidence_count,
                        vote_evidence_count,
                        %validator_addresses,
                        "Equivocation evidence observed at finalization"
                    );
                }

                // Notify any subscribers about the finalized value
                self.tx_event.send(|| Event::Finalized {
                    commit_certificate: certificate.clone(),
                    evidence: evidence.clone(),
                });

                info!(
                    height = %certificate.height,
                    round = %certificate.round,
                    total_signatures = certificate.commit_signatures.len(),
                    "Height finalized with extended certificate"
                );

                // Notify the host about the finalized value
                self.host
                    .call_and_forward(
                        |reply_to| HostMsg::Finalized {
                            certificate,
                            extensions,
                            evidence,
                            reply_to,
                        },
                        myself,
                        |next| match next {
                            Next::Start(h, params) => Msg::StartHeight(h, params),
                            Next::Restart(h, params) => Msg::RestartHeight(h, params),
                        },
                        None,
                    )
                    .map_err(|e| eyre!("Error when sending finalized value to host: {e:?}"))?;

                Ok(r.resume_with(()))
            }

            Effect::InvalidSyncValue(peer, height, error, r) => {
                if let ConsensusError::InvalidCommitCertificate(certificate, e) = error {
                    error!(
                        %peer,
                        %certificate.height,
                        %certificate.round,
                        "Invalid certificate received: {e}"
                    );

                    self.sync
                        .send(SyncMsg::InvalidValue(peer, certificate.height));
                } else {
                    self.sync.send(SyncMsg::ValueProcessingError(peer, height));
                }

                Ok(r.resume_with(()))
            }

            Effect::ValidSyncValue(value, proposer, r) => {
                let certificate_height = value.certificate.height;
                let certificate_round = value.certificate.round;

                let sync = Arc::clone(&self.sync);
                let sync_on_none = Arc::clone(&self.sync);
                let myself = myself.clone();

                cast_option_and_handle(
                    &self.host,
                    |reply_to| HostMsg::ProcessSyncedValue {
                        height: certificate_height,
                        round: certificate_round,
                        proposer,
                        value_bytes: value.value_bytes,
                        reply_to,
                    },
                    move |proposed| {
                        if proposed.value.id() == value.certificate.value_id {
                            // Id matches the certificate — forward to consensus.
                            // A locally-invalid validity is still forwarded so the
                            // downstream `maybe_sync_decision` path can surface the
                            // version-skew diagnostic to operators.
                            let _ = myself.cast(Msg::<Ctx>::ReceivedProposedValue(
                                proposed,
                                ValueOrigin::Sync,
                            ));
                        } else {
                            warn!(
                                peer = %value.peer,
                                height = %certificate_height,
                                proposed.value_id = %proposed.value.id(),
                                certificate.value_id = %value.certificate.value_id,
                                "Synced value id does not match commit certificate, rejecting"
                            );
                            sync.send(SyncMsg::InvalidValue(value.peer, certificate_height));
                        }
                    },
                    move || {
                        sync_on_none.send(SyncMsg::ValueProcessingError(
                            value.peer,
                            certificate_height,
                        ));
                    },
                )?;

                Ok(r.resume_with(()))
            }

            Effect::WalAppend(height, entry, r) => {
                self.wal_append(height, entry, state.phase, state.is_validator)
                    .await?;
                Ok(r.resume_with(()))
            }
        }
    }
}

#[async_trait]
impl<Ctx> Actor for Consensus<Ctx>
where
    Ctx: Context,
{
    type Msg = Msg<Ctx>;
    type State = State<Ctx>;
    type Arguments = ();

    #[tracing::instrument(
        name = "consensus",
        parent = &self.span,
        skip_all,
    )]
    async fn pre_start(
        &self,
        myself: ActorRef<Msg<Ctx>>,
        _args: (),
    ) -> Result<State<Ctx>, ActorProcessingErr> {
        info!("Consensus is starting");

        self.network
            .cast(NetworkMsg::Subscribe(Box::new(myself.clone())))?;

        Ok(State {
            timers: Timers::new(Box::new(myself)),
            timeouts: Ctx::Timeouts::default(),
            consensus: None,
            connected_peers: BTreeSet::new(),
            phase: Phase::Unstarted,
            replay_index: None,
            is_validator: false,
            msg_buffer: MessageBuffer::new(MAX_BUFFER_SIZE),
        })
    }

    #[tracing::instrument(
        name = "consensus",
        parent = &self.span,
        skip_all,
        fields(height = %state.height(), round = %state.round())
    )]
    async fn post_start(
        &self,
        _myself: ActorRef<Msg<Ctx>>,
        state: &mut State<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        info!("Consensus has started");

        state.timers.cancel_all();
        Ok(())
    }

    #[tracing::instrument(
        name = "consensus",
        parent = &self.span,
        skip_all,
        fields(
            height = %span_height(state.height(), &msg),
            round = %span_round(state.round(), &msg)
        )
    )]
    async fn handle(
        &self,
        myself: ActorRef<Msg<Ctx>>,
        msg: Msg<Ctx>,
        state: &mut State<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        if state.phase != Phase::Running && should_buffer(&msg) {
            let _span = error_span!("buffer", phase = ?state.phase).entered();
            state.msg_buffer.buffer(msg);
            return Ok(());
        }

        if let Err(error) = self.handle_msg(myself.clone(), state, msg).await {
            if is_liveness_failure(&error) {
                return Err(error);
            }
            error!("Error when handling message: {error:?}");
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "consensus",
        parent = &self.span,
        skip_all,
        fields(
            height = %state.height(),
            round = %state.round()
        )
    )]
    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut State<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        info!("Consensus has stopped");
        state.timers.cancel_all();
        Ok(())
    }
}

fn should_buffer<Ctx: Context>(msg: &Msg<Ctx>) -> bool {
    !matches!(
        msg,
        Msg::StartHeight(..)
            | Msg::DecisionCommitted(..)
            | Msg::NetworkEvent(NetworkEvent::Listening(..))
            | Msg::NetworkEvent(NetworkEvent::PeerConnected(..))
            | Msg::NetworkEvent(NetworkEvent::PeerDisconnected(..))
            | Msg::NetworkEvent(NetworkEvent::PeerSyncSubscribed(..))
    )
}

/// Use the height we are about to start instead of the consensus state height
/// for the tracing span of the Consensus actor when starting a new height.
fn span_height<Ctx: Context>(height: Ctx::Height, msg: &Msg<Ctx>) -> Ctx::Height {
    if let Msg::StartHeight(h, _) = msg {
        *h
    } else {
        height
    }
}

/// Use round 0 instead of the consensus state round for the tracing span of
/// the Consensus actor when starting a new height.
fn span_round<Ctx: Context>(round: Round, msg: &Msg<Ctx>) -> Round {
    if let Msg::StartHeight(_, _) = msg {
        Round::new(0)
    } else {
        round
    }
}
