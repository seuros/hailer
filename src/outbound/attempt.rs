use anyhow::ensure;
use serde::{Deserialize, Serialize};
use state_machines::state_machine;

state_machine! {
    name: AttemptLifecycle,
    dynamic: true,
    snapshot: true,
    initial: Reserved,
    states: [Reserved, Dispatching, Accepted, NotAccepted, Unknown, Abandoned],
    events {
        dispatch { transition: { from: Reserved, to: Dispatching } }
        accept { transition: { from: [Dispatching, Unknown, NotAccepted], to: Accepted } }
        refuse { transition: { from: [Dispatching, Unknown], to: NotAccepted } }
        uncertain { transition: { from: Dispatching, to: Unknown } }
        abandon { transition: { from: Reserved, to: Abandoned } }
    }
}

#[derive(Serialize, Deserialize)]
struct Persisted {
    schema_version: u32,
    snapshot: AttemptLifecycleSnapshot<()>,
}

pub(super) fn initial() -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::to_value(Persisted {
        schema_version: 1,
        snapshot: DynamicAttemptLifecycle::new(()).into_snapshot(),
    })?)
}

pub(super) fn advance(
    value: serde_json::Value,
    indexed_phase: &str,
    event: AttemptLifecycleEvent,
) -> anyhow::Result<(&'static str, serde_json::Value)> {
    let saved: Persisted = serde_json::from_value(value)?;
    ensure!(saved.schema_version == 1, "unsupported attempt lifecycle schema");
    let mut machine = DynamicAttemptLifecycle::from_snapshot(saved.snapshot)
        .map_err(|_| anyhow::anyhow!("invalid attempt lifecycle snapshot"))?;
    ensure!(phase(machine.current_state()) == indexed_phase, "attempt lifecycle index mismatch");
    machine.handle(event).map_err(|_| anyhow::anyhow!("invalid attempt transition"))?;
    let phase = phase(machine.current_state());
    let value =
        serde_json::to_value(Persisted { schema_version: 1, snapshot: machine.into_snapshot() })?;
    Ok((phase, value))
}

const fn phase(state: AttemptLifecycleState) -> &'static str {
    match state {
        AttemptLifecycleState::Reserved => "reserved",
        AttemptLifecycleState::Dispatching => "dispatching",
        AttemptLifecycleState::Accepted => "accepted",
        AttemptLifecycleState::NotAccepted => "not_accepted",
        AttemptLifecycleState::Unknown => "unknown",
        AttemptLifecycleState::Abandoned => "abandoned",
    }
}
