//! Request budgets on one monotonic clock, so every deadline compares.

#[cfg(test)]
mod tests;

use std::sync::LazyLock;
use std::time::Duration;

use deadline_machines::{Clock, Deadline, Instant, SystemClock};

static CLOCK: LazyLock<SystemClock> = LazyLock::new(SystemClock::new);

pub fn clock() -> SystemClock {
    *CLOCK
}

pub fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub fn after(timeout: Duration) -> Deadline {
    Deadline::after(CLOCK.now(), nanos(timeout))
}

/// `deadline` brought forward by `reserve`, so work under it leaves `reserve`
/// to whatever follows.
pub fn leaving(deadline: Deadline, reserve: Duration) -> Deadline {
    deadline
        .expires_at()
        .map_or(deadline, |at| Deadline::at(Instant(at.0.saturating_sub(nanos(reserve)))))
}

/// The wait `deadline` still allows, at most `cap`; `None` below `floor`.
pub fn within(deadline: Deadline, floor: Duration, cap: Duration) -> Option<Duration> {
    let now = CLOCK.now();
    let wait = Duration::from_nanos(deadline.clamp_sleep_ns(now, nanos(cap)));
    (wait >= floor && !deadline.is_expired(now)).then_some(wait)
}
