use std::time::Duration;

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

async fn pool() -> (crate::test_support::Database, sqlx::PgPool) {
    let database = crate::test_support::database().await;
    let store = crate::state::Store::connect(database.url(), 5).await.expect("store");
    (database, store.pool)
}

#[tokio::test]
async fn bootstrap_and_drain_cannot_be_overridden_by_a_request_snapshot() {
    let (_database, pool) = pool().await;
    let control = Control::new(pool, None, false).await.expect("controller");
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

fn rate_config() -> RateLimitConfig {
    RateLimitConfig {
        recipients_per_second: 2.0,
        burst: 2,
        discover_from_aws: false,
        discovery_interval_seconds: 900,
    }
}

#[tokio::test]
async fn rate_reservations_are_atomic_and_refill_over_real_time() {
    let (_database, pool) = pool().await;
    let limiter = rate::Limiter::new(pool, &rate_config()).await.expect("limiter");
    assert!(!limiter.admit(3).await.expect("admit"));
    assert!(limiter.admit(2).await.expect("admit"));
    assert!(!limiter.admit(1).await.expect("admit"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(limiter.admit(1).await.expect("admit"));
    assert!(!limiter.admit(1).await.expect("admit"));
}

#[tokio::test]
async fn two_replicas_share_one_account_wide_budget() {
    let (_database, pool) = pool().await;
    // Two independent Limiter handles, as two Hailer processes would each
    // build their own - the shared row, not an in-process Mutex, is what
    // must stop a second replica from ever doubling the admitted rate.
    let replica_a = rate::Limiter::new(pool.clone(), &rate_config()).await.expect("limiter a");
    let replica_b = rate::Limiter::new(pool, &rate_config()).await.expect("limiter b");
    assert!(replica_a.admit(2).await.expect("admit"));
    assert!(!replica_b.admit(1).await.expect("admit"));
}

#[tokio::test]
async fn discovery_takes_effect_on_the_shared_row_for_every_replica() {
    let (_database, pool) = pool().await;
    let slow = RateLimitConfig {
        recipients_per_second: 1.0,
        burst: 1,
        discover_from_aws: false,
        discovery_interval_seconds: 900,
    };
    let replica_a = rate::Limiter::new(pool.clone(), &slow).await.expect("limiter a");
    let replica_b = rate::Limiter::new(pool, &slow).await.expect("limiter b");
    assert!(replica_a.admit(1).await.expect("admit")); // drains the single starting token
    // At the original 1/sec, 150ms could never refill a whole token again.
    replica_b.set_rate(20.0).await.expect("set_rate");
    tokio::time::sleep(Duration::from_millis(150)).await;
    // 150ms at the discovered 20/sec refills ~3 tokens - replica_a observes
    // the rate replica_b discovered, not the config it started with.
    assert!(replica_a.admit(1).await.expect("admit under the discovered rate"));
}
