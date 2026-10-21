//! Pure decision circuit, with resource admission owned by the async host.

mod rate;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, Ordering};

use behavior_machines::{Node, Status};
use behavior_machines_circuit::{Lowered, State};

use crate::config::RateLimitConfig;
use rate::Limiter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Invalid,
    Unavailable,
    Suppressed,
    RateLimited,
}

#[derive(Clone, Copy)]
// Independent binary circuit inputs, not mutually exclusive lifecycle states.
#[allow(clippy::struct_excessive_bools)]
pub struct Snapshot {
    pub valid: bool,
    pub service_ready: bool,
    pub storage_ready: bool,
    pub recipients_allowed: bool,
    pub rate_allowed: bool,
}

#[derive(Clone, Copy)]
enum Signal {
    Invalid,
    Unavailable,
    Suppressed,
    Throttled,
    Outcome(Decision),
}

const LEAVES: [Signal; 9] = [
    Signal::Invalid,
    Signal::Outcome(Decision::Invalid),
    Signal::Unavailable,
    Signal::Outcome(Decision::Unavailable),
    Signal::Suppressed,
    Signal::Outcome(Decision::Suppressed),
    Signal::Throttled,
    Signal::Outcome(Decision::RateLimited),
    Signal::Outcome(Decision::Allow),
];

impl Signal {
    const fn status(self, snapshot: Snapshot) -> Status {
        let success = match self {
            Self::Invalid => !snapshot.valid,
            Self::Unavailable => !snapshot.service_ready || !snapshot.storage_ready,
            Self::Suppressed => !snapshot.recipients_allowed,
            Self::Throttled => !snapshot.rate_allowed,
            Self::Outcome(_) => true,
        };
        if success { Status::Success } else { Status::Failure }
    }
}

pub struct Policy {
    lowered: Lowered,
}

impl Policy {
    pub fn new() -> anyhow::Result<Self> {
        let branches = LEAVES[..8]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Node::sequence([Node::leaf(pair[0]), Node::leaf(pair[1])]))
            .chain(std::iter::once(Node::leaf(LEAVES[8])));
        let lowered = Lowered::new(&Node::reactive_fallback(branches))?;
        anyhow::ensure!(lowered.plan().leaf_count() == LEAVES.len(), "invalid admission circuit");
        Ok(Self { lowered })
    }

    pub fn decide(&self, snapshot: Snapshot) -> anyhow::Result<Decision> {
        // Leaves are pure: no AWS, SQL, resource reservations, or clocks.
        let statuses: Vec<_> = LEAVES.iter().map(|signal| signal.status(snapshot)).collect();
        let schedule = self.lowered.evaluate(&mut State::initial(&self.lowered), &statuses)?;
        let decisions: Vec<_> = LEAVES
            .iter()
            .enumerate()
            .filter_map(|(slot, signal)| {
                if let Signal::Outcome(decision) = signal {
                    self.lowered
                        .plan()
                        .leaf_node(slot)
                        .filter(|node| schedule.reached(*node))
                        .map(|_| *decision)
                } else {
                    None
                }
            })
            .collect();
        anyhow::ensure!(decisions.len() == 1, "admission circuit must choose exactly one outcome");
        Ok(decisions[0])
    }
}

pub struct Control {
    policy: Policy,
    accepting: AtomicBool,
    bootstrapped: AtomicBool,
    limiter: Option<Limiter>,
}

impl Control {
    pub fn new(rate: Option<&RateLimitConfig>, bootstrapped: bool) -> anyhow::Result<Self> {
        Ok(Self {
            policy: Policy::new()?,
            accepting: AtomicBool::new(true),
            bootstrapped: AtomicBool::new(bootstrapped),
            limiter: rate.map(Limiter::new),
        })
    }

    pub fn ready(&self) -> bool {
        self.accepting.load(Ordering::Acquire) && self.bootstrapped.load(Ordering::Acquire)
    }

    pub fn draining(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
    }

    pub fn bootstrap_done(&self) {
        self.bootstrapped.store(true, Ordering::Release);
    }

    pub fn drain(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    pub fn decide(&self, mut snapshot: Snapshot) -> Decision {
        snapshot.service_ready &= self.ready();
        self.policy.decide(snapshot).unwrap_or(Decision::Unavailable)
    }

    pub fn rate_admit(&self, recipients: usize) -> bool {
        self.ready() && self.limiter.as_ref().is_none_or(|limiter| limiter.admit(recipients))
    }
}
