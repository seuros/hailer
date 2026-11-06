//! Fault injection, armed by an operator through `hailer.rogue_experiments`
//! and disarmed by default. Each target is one slot of a [`Rogue`]; a call
//! with nothing armed costs a read lock and one relaxed load.
//!
//! Expiries are computed by PostgreSQL as time remaining, then counted down
//! on [`crate::budget::clock`], so no two hosts' wall clocks are compared.

#[cfg(test)]
mod tests;

use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use deadline_machines::Clock;
use rogue_machines::{Decision, Experiment, Fault, Fraction, PER_MILLION, Rogue, Slot};
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff::pause;
use crate::state::Store;

/// The longest an experiment may stay armed.
pub const MAX_EXPIRY: Duration = Duration::from_hours(24);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Provider,
    Feedback,
    Verifier,
}

const fn slot(index: usize) -> Slot {
    match Slot::new(index) {
        Some(slot) => slot,
        None => panic!("a target needs a slot"),
    }
}

const SLOTS: [Slot; 3] = [slot(0), slot(1), slot(2)];

impl Target {
    pub const ALL: [Self; 3] = [Self::Provider, Self::Feedback, Self::Verifier];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Feedback => "feedback",
            Self::Verifier => "verifier",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|target| target.name() == name)
    }

    const fn slot(self) -> Slot {
        SLOTS[self as usize]
    }
}

pub const fn fault_name(fault: Fault) -> &'static str {
    match fault {
        Fault::Raise => "raise",
        Fault::Delay { .. } => "delay",
        Fault::Garble => "garble",
    }
}

fn fault(name: &str, delay_ms: u64) -> anyhow::Result<Fault> {
    Ok(match name {
        "raise" => Fault::Raise,
        "delay" => Fault::Delay { ns: crate::budget::nanos(Duration::from_millis(delay_ms)) },
        "garble" => Fault::Garble,
        other => anyhow::bail!("unknown rogue fault {other}"),
    })
}

/// One armed experiment as stored, with the time it has left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Armed {
    pub target: Target,
    pub fault: Fault,
    pub fraction: Fraction,
    pub remaining: Duration,
}

impl Armed {
    pub fn to_json(self) -> serde_json::Value {
        let delay_ms = match self.fault {
            Fault::Delay { ns } => Duration::from_nanos(ns).as_millis(),
            Fault::Raise | Fault::Garble => 0,
        };
        serde_json::json!({
            "target": self.target.name(),
            "fault": fault_name(self.fault),
            "delay_ms": delay_ms,
            "fraction_ppm": self.fraction.millionths(),
            "remaining_seconds": self.remaining.as_secs(),
        })
    }
}

/// What an armed fault leaves of a call allowed `timeout`.
#[derive(Debug, PartialEq, Eq)]
pub enum Injected {
    Pass(Duration),
    Garble(Duration),
    Raise,
    TimedOut,
}

#[derive(Debug, Default)]
pub struct Experiments(RwLock<Rogue>);

impl Experiments {
    pub fn fault(&self, target: Target) -> Option<Fault> {
        let rogue = self.0.read().unwrap_or_else(PoisonError::into_inner);
        if !rogue.any_armed() {
            return None;
        }
        match rogue.decide(target.slot(), crate::budget::clock().now_ns(), draw()) {
            Decision::Pass => None,
            Decision::Inject(fault) => Some(fault),
        }
    }

    /// A delay is served inside `timeout`; one at least that long times the
    /// call out.
    pub async fn inject(&self, target: Target, timeout: Duration) -> Injected {
        match self.fault(target) {
            None => Injected::Pass(timeout),
            Some(Fault::Raise) => Injected::Raise,
            Some(Fault::Garble) => Injected::Garble(timeout),
            Some(Fault::Delay { ns }) => {
                let delay = Duration::from_nanos(ns);
                tokio::time::sleep(delay.min(timeout)).await;
                timeout
                    .checked_sub(delay)
                    .filter(|left| !left.is_zero())
                    .map_or(Injected::TimedOut, Injected::Pass)
            }
        }
    }

    /// Arms exactly `armed` and disarms every other target.
    pub fn apply(&self, armed: &[Armed]) {
        let now = crate::budget::clock().now_ns();
        let mut rogue = self.0.write().unwrap_or_else(PoisonError::into_inner);
        for target in Target::ALL {
            match armed.iter().find(|experiment| experiment.target == target) {
                Some(experiment) => rogue.arm(
                    target.slot(),
                    Experiment::new(
                        experiment.fault,
                        experiment.fraction,
                        now.saturating_add(crate::budget::nanos(experiment.remaining)),
                    ),
                ),
                None => rogue.disarm(target.slot()),
            }
        }
    }
}

/// A draw below [`PER_MILLION`].
fn draw() -> u32 {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let hash = RandomState::new().hash_one(SEQUENCE.fetch_add(1, Ordering::Relaxed));
    u32::try_from(hash % u64::from(PER_MILLION)).unwrap_or_default()
}

impl Store {
    pub async fn arm_experiment(
        &self,
        target: Target,
        fault: Fault,
        fraction: Fraction,
        expires_in: Duration,
    ) -> anyhow::Result<()> {
        let delay_ms = match fault {
            Fault::Delay { ns } => i64::try_from(Duration::from_nanos(ns).as_millis())?,
            Fault::Raise | Fault::Garble => 0,
        };
        sqlx::query(
            "INSERT INTO hailer.rogue_experiments (target, fault, delay_ms, fraction_ppm, expires_at) \
             VALUES ($1, $2, $3, $4, clock_timestamp() + $5 * interval '1 millisecond') \
             ON CONFLICT (target) DO UPDATE SET fault = EXCLUDED.fault, delay_ms = EXCLUDED.delay_ms, \
             fraction_ppm = EXCLUDED.fraction_ppm, armed_at = EXCLUDED.armed_at, \
             expires_at = EXCLUDED.expires_at",
        )
        .bind(target.name())
        .bind(fault_name(fault))
        .bind(delay_ms)
        .bind(i32::try_from(fraction.millionths())?)
        .bind(i64::try_from(expires_in.as_millis())?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Disarms `target`, or every target; returns how many were armed.
    pub async fn disarm_experiments(&self, target: Option<Target>) -> anyhow::Result<u64> {
        Ok(sqlx::query(
            "DELETE FROM hailer.rogue_experiments WHERE ($1::text IS NULL OR target = $1) \
             AND expires_at > clock_timestamp()",
        )
        .bind(target.map(Target::name))
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn experiments(&self) -> anyhow::Result<Vec<Armed>> {
        sqlx::query(
            "SELECT target, fault, delay_ms, fraction_ppm, \
             (extract(epoch from expires_at - clock_timestamp()) * 1000)::bigint AS remaining_ms \
             FROM hailer.rogue_experiments WHERE expires_at > clock_timestamp() ORDER BY target",
        )
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(|row| {
            let target: String = row.try_get("target")?;
            let name: String = row.try_get("fault")?;
            let millionths = u32::try_from(row.try_get::<i32, _>("fraction_ppm")?)?;
            Ok(Armed {
                target: Target::parse(&target)
                    .ok_or_else(|| anyhow::anyhow!("unknown rogue target {target}"))?,
                fault: fault(&name, u64::try_from(row.try_get::<i64, _>("delay_ms")?)?)?,
                fraction: Fraction::new(millionths)
                    .ok_or_else(|| anyhow::anyhow!("rogue fraction above a million"))?,
                remaining: Duration::from_millis(u64::try_from(
                    row.try_get::<i64, _>("remaining_ms")?.max(0),
                )?),
            })
        })
        .collect()
    }
}

/// Polls the armed set into `experiments` until `stop`. A failed read keeps
/// whatever is armed, which still expires on its own.
pub async fn watch(
    store: Store,
    experiments: Arc<Experiments>,
    every: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let mut previous: Vec<(Target, Fault, Fraction)> = Vec::new();
    loop {
        match store.experiments().await {
            Ok(armed) => {
                experiments.apply(&armed);
                let current: Vec<_> =
                    armed.iter().map(|each| (each.target, each.fault, each.fraction)).collect();
                if current != previous {
                    let summary: Vec<_> = armed.iter().copied().map(Armed::to_json).collect();
                    info!(experiments = %serde_json::Value::from(summary), "Rogue experiments changed");
                    previous = current;
                }
            }
            Err(error) => {
                warn!(%error, "Rogue experiments unavailable; armed set kept until expiry");
            }
        }
        if !pause(&stop, every).await {
            return Ok(());
        }
    }
}
