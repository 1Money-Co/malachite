use std::collections::BTreeMap;
use std::io;

use malachitebft_core_consensus::SignedConsensusMsg;
use malachitebft_core_types::{
    Context, NilOrVal, Proposal, Round, SignedExtension, SignedProposal, SignedVote, ValueId, Vote,
    VoteType,
};

use crate::wal::WalEntry;

/// Records the local signing choices already in the sealed WAL. The v0.8.2 consensus
/// layer provides the equivalent index over its WAL inputs.
pub(super) struct ReplayIndex<Ctx: Context> {
    votes: BTreeMap<(Ctx::Height, Round, VoteType), SignedVote<Ctx>>,
    proposals: BTreeMap<(Ctx::Height, Round), SignedProposal<Ctx>>,
}

impl<Ctx: Context> ReplayIndex<Ctx> {
    pub(super) fn new(entries: &[WalEntry<Ctx>], address: &Ctx::Address) -> io::Result<Self> {
        let mut index = Self {
            votes: BTreeMap::new(),
            proposals: BTreeMap::new(),
        };
        for entry in entries {
            match entry {
                WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(vote))
                    if vote.validator_address() == address
                        && index.vote(&vote.message)?.is_none() =>
                {
                    index.insert_vote(vote.clone());
                }
                WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(proposal))
                    if proposal.validator_address() == address
                        && index.proposal(&proposal.message)?.is_none() =>
                {
                    index.insert_proposal(proposal.clone());
                }
                _ => {}
            }
        }
        Ok(index)
    }

    pub(super) fn vote(&self, vote: &Ctx::Vote) -> io::Result<Option<SignedVote<Ctx>>> {
        let Some(recorded) = self
            .votes
            .get(&(vote.height(), vote.round(), vote.vote_type()))
        else {
            return Ok(None);
        };
        // Match v0.8.2: the vote extension can be nondeterministic and is signed separately.
        let mut original = recorded.message.clone();
        let mut derived = vote.clone();
        original.take_extension();
        derived.take_extension();
        if original != derived {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Re-derived {:?} at height {}, round {} differs from the recorded WAL vote",
                    vote.vote_type(),
                    vote.height(),
                    vote.round(),
                ),
            ));
        }
        Ok(Some(recorded.clone()))
    }

    pub(super) fn proposal(
        &self,
        proposal: &Ctx::Proposal,
    ) -> io::Result<Option<SignedProposal<Ctx>>> {
        let Some(recorded) = self.proposals.get(&(proposal.height(), proposal.round())) else {
            return Ok(None);
        };
        if &recorded.message != proposal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                "Re-derived proposal at height {}, round {} differs from the recorded WAL proposal",
                proposal.height(), proposal.round(),
            ),
            ));
        }
        Ok(Some(recorded.clone()))
    }

    pub(super) fn insert_vote(&mut self, vote: SignedVote<Ctx>) {
        self.votes
            .insert((vote.height(), vote.round(), vote.vote_type()), vote);
    }

    pub(super) fn extension(
        &self,
        height: Ctx::Height,
        round: Round,
        value: &ValueId<Ctx>,
    ) -> io::Result<Option<Option<SignedExtension<Ctx>>>> {
        let Some(recorded) = self.votes.get(&(height, round, VoteType::Precommit)) else {
            return Ok(None);
        };
        if !matches!(recorded.value(), NilOrVal::Val(recorded) if recorded == value) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                "Re-derived precommit extension for height {height}, round {round} differs from the recorded vote"
            )));
        }
        Ok(Some(recorded.extension().cloned()))
    }

    pub(super) fn insert_proposal(&mut self, proposal: SignedProposal<Ctx>) {
        self.proposals
            .insert((proposal.height(), proposal.round()), proposal);
    }
}
