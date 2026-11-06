use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{Backoff, pause};

/// A quarter of jitter leaves 75% to 100% of the nominal delay.
fn jittered(nominal_ms: u64) -> std::ops::RangeInclusive<Duration> {
    Duration::from_millis(nominal_ms * 3 / 4)..=Duration::from_millis(nominal_ms)
}

#[test]
fn hold_off_doubles_per_failure_and_resets_on_success() {
    let mut backoff = Backoff::new();
    assert!(jittered(1_000).contains(&backoff.failed()));
    assert!(jittered(2_000).contains(&backoff.failed()));
    assert!(jittered(4_000).contains(&backoff.failed()));
    backoff.succeeded();
    assert!(jittered(1_000).contains(&backoff.failed()));
}

#[test]
fn hold_off_is_capped_and_never_exhausts() {
    let mut backoff = Backoff::new();
    for _ in 0..300 {
        assert!(backoff.failed() <= Duration::from_secs(30));
    }
    assert!(jittered(30_000).contains(&backoff.failed()));
}

#[tokio::test]
async fn pause_reports_whether_the_wait_completed() {
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(!pause(&stop, Duration::from_secs(60)).await);
    assert!(pause(&CancellationToken::new(), Duration::ZERO).await);
}
