use super::*;

#[test]
fn circuit_covers_every_combination_and_fails_closed_in_priority_order() {
    let policy = Policy::new().expect("pure circuit");
    for bits in 0..32 {
        let snapshot = Snapshot {
            valid: bits & 1 != 0,
            service_ready: bits & 2 != 0,
            storage_ready: bits & 4 != 0,
            recipients_allowed: bits & 8 != 0,
            rate_allowed: bits & 16 != 0,
        };
        let expected = if !snapshot.valid {
            Decision::Invalid
        } else if !snapshot.service_ready || !snapshot.storage_ready {
            Decision::Unavailable
        } else if !snapshot.recipients_allowed {
            Decision::Suppressed
        } else if !snapshot.rate_allowed {
            Decision::RateLimited
        } else {
            Decision::Allow
        };
        assert_eq!(policy.decide(snapshot).expect("one pure result"), expected);
    }
}

#[test]
fn bootstrap_and_drain_cannot_be_overridden_by_a_request_snapshot() {
    let control = Control::new(None, false).expect("controller");
    let snapshot = Snapshot {
        valid: true,
        service_ready: true,
        storage_ready: true,
        recipients_allowed: true,
        rate_allowed: true,
    };
    assert_eq!(control.decide(snapshot), Decision::Unavailable);
    control.bootstrap_done();
    assert_eq!(control.decide(snapshot), Decision::Allow);
    control.drain();
    assert_eq!(control.decide(snapshot), Decision::Unavailable);
    control.bootstrap_done();
    assert_eq!(control.decide(snapshot), Decision::Unavailable);
}

#[test]
fn rate_reservations_are_atomic_and_refill_on_host_owned_monotonic_time() {
    let limiter = rate::Limiter::new(&RateLimitConfig { recipients_per_second: 2.0, burst: 2 });
    assert!(!limiter.admit_at(3, 0.0));
    assert!(limiter.admit_at(2, 0.0));
    assert!(!limiter.admit_at(1, 0.0));
    assert!(limiter.admit_at(1, 0.5));
    assert!(!limiter.admit_at(1, 0.5));
    assert!(limiter.admit_at(2, 1.5));
}
