//! The pace every background worker keeps: its own interval while cycles
//! succeed, an exponential hold-off while they fail.
//!
//! One policy for every worker, so a PostgreSQL or AWS outage slows the
//! dispatcher, the feedback consumer, rate discovery and suppression
//! reconciliation at the same rate instead of four hand-tuned ones.

#[cfg(test)]
mod tests;

use std::time::Duration;

use chrono_machines::ExponentialBackoff;
use tokio_util::sync::CancellationToken;

/// One second doubling to thirty, up to a quarter off each delay so restarted
/// replicas do not poll in step. Never spent: a worker keeps trying for as
/// long as it is supervised.
const STRATEGY: ExponentialBackoff = ExponentialBackoff::new()
    .max_attempts(u8::MAX)
    .base_delay_ms(1_000)
    .multiplier(2.0)
    .max_delay_ms(30_000)
    .jitter_factor(0.25);

/// Consecutive failures of one worker's cycle, and the hold-off they earn.
#[derive(Debug)]
pub struct Backoff(chrono_machines::Backoff<ExponentialBackoff>);

impl Backoff {
    pub const fn new() -> Self {
        Self(chrono_machines::Backoff::new(STRATEGY))
    }

    /// A cycle completed; the next one runs at the worker's own interval.
    pub const fn succeeded(&mut self) {
        self.0.succeeded();
    }

    /// A cycle failed; how long to hold off before the next one.
    pub fn failed(&mut self) -> Duration {
        Duration::from_millis(self.0.failed().unwrap_or(STRATEGY.max_delay_ms))
    }
}

/// Waits `delay` unless `stop` fires first; `true` when the whole wait elapsed.
pub async fn pause(stop: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = stop.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}
