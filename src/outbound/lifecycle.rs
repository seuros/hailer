//! Pure lifecycle restoration and transition validation. No callbacks/effects.

use anyhow::ensure;
use serde::{Deserialize, Serialize};
use state_machines::state_machine;

state_machine! {
    name: MessageLifecycle,
    dynamic: true,
    snapshot: true,
    initial: Queued,
    states: [
        Queued, Claimed, Dispatching, RetryWaiting, HeldUnknown,
        Accepted, Rejected, Suppressed, Cancelled, Expired
    ],
    events {
        claim { transition: { from: [Queued, RetryWaiting], to: Claimed } }
        dispatch { transition: { from: Claimed, to: Dispatching } }
        reclaim { transition: { from: Claimed, to: Queued } }
        retry { transition: { from: [Claimed, Dispatching, HeldUnknown], to: RetryWaiting } }
        uncertain { transition: { from: Dispatching, to: HeldUnknown } }
        accept { transition: { from: [Queued, Claimed, RetryWaiting, Dispatching, HeldUnknown], to: Accepted } }
        reject { transition: { from: [Claimed, Dispatching, HeldUnknown], to: Rejected } }
        suppress { transition: { from: Claimed, to: Suppressed } }
        cancel { transition: { from: [Queued, RetryWaiting, Claimed], to: Cancelled } }
        expire { transition: { from: [Queued, RetryWaiting, Claimed], to: Expired } }
        authorize_retry { transition: { from: HeldUnknown, to: RetryWaiting } }
    }
}

#[derive(Serialize, Deserialize)]
struct Persisted {
    schema_version: u32,
    snapshot: MessageLifecycleSnapshot<()>,
}

pub(super) struct Lifecycle(DynamicMessageLifecycle<()>);

impl Lifecycle {
    pub fn queued() -> Self {
        Self(DynamicMessageLifecycle::new(()))
    }

    pub fn restore(value: serde_json::Value, indexed_status: &str) -> anyhow::Result<Self> {
        let saved: Persisted = serde_json::from_value(value)?;
        ensure!(saved.schema_version == 1, "unsupported message lifecycle schema");
        let machine = DynamicMessageLifecycle::from_snapshot(saved.snapshot)
            .map_err(|_| anyhow::anyhow!("invalid message lifecycle snapshot"))?;
        let lifecycle = Self(machine);
        ensure!(lifecycle.status() == indexed_status, "message lifecycle index mismatch");
        Ok(lifecycle)
    }

    pub fn status(&self) -> &'static str {
        match self.0.current_state() {
            MessageLifecycleState::Queued => "queued",
            MessageLifecycleState::Claimed => "claimed",
            MessageLifecycleState::Dispatching => "dispatching",
            MessageLifecycleState::RetryWaiting => "retry_waiting",
            MessageLifecycleState::HeldUnknown => "held_unknown",
            MessageLifecycleState::Accepted => "accepted",
            MessageLifecycleState::Rejected => "rejected",
            MessageLifecycleState::Suppressed => "suppressed",
            MessageLifecycleState::Cancelled => "cancelled",
            MessageLifecycleState::Expired => "expired",
        }
    }

    pub fn apply(&mut self, event: MessageLifecycleEvent) -> anyhow::Result<()> {
        self.0.handle(event).map_err(|_| anyhow::anyhow!("invalid message transition"))
    }

    pub fn persist(self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(Persisted { schema_version: 1, snapshot: self.0.into_snapshot() })?)
    }
}
