use super::*;
use crate::verify::{State, Verdict};

fn verdict() -> Verdict {
    Verdict { state: State::Deliverable, reason: "test".into(), disposable: false, role: false }
}

#[tokio::test]
async fn exact_duplicate_lines_are_counted_once() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let (to_submit, report) = dedupe_against_cache(
        &store,
        vec!["a@example.dev".into(), "a@example.dev".into(), "a@example.dev".into()],
    )
    .await;
    assert_eq!(to_submit, ["a@example.dev"]);
    assert_eq!(report.duplicate_lines, 2);
    assert_eq!(report.already_cached, 0);
    assert_eq!(report.invalid_lines, 0);
}

#[tokio::test]
async fn plus_alias_variants_normalize_to_the_same_duplicate() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let (to_submit, report) = dedupe_against_cache(
        &store,
        vec!["user@example.dev".into(), "USER+newsletter@example.dev".into()],
    )
    .await;
    assert_eq!(to_submit.len(), 1);
    assert_eq!(report.duplicate_lines, 1);
}

#[tokio::test]
async fn an_unparseable_line_is_counted_as_invalid_not_silently_dropped() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let (to_submit, report) =
        dedupe_against_cache(&store, vec!["not-an-address".into(), "ok@example.dev".into()]).await;
    assert_eq!(to_submit, ["ok@example.dev"]);
    assert_eq!(report.invalid_lines, 1);
}

#[tokio::test]
async fn an_address_with_a_fresh_cached_verdict_is_skipped() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    store
        .cache_verdict("cached@example.dev", &verdict(), Duration::from_secs(3600))
        .await
        .expect("seed cache");
    let (to_submit, report) =
        dedupe_against_cache(&store, vec!["cached@example.dev".into(), "fresh@example.dev".into()])
            .await;
    assert_eq!(to_submit, ["fresh@example.dev"]);
    assert_eq!(report.already_cached, 1);
}

#[tokio::test]
async fn an_expired_cached_verdict_is_resubmitted_not_skipped() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    // A TTL of zero expires immediately, so this must not count as a hit.
    store
        .cache_verdict("expired@example.dev", &verdict(), Duration::from_secs(0))
        .await
        .expect("seed cache");
    let (to_submit, report) =
        dedupe_against_cache(&store, vec!["expired@example.dev".into()]).await;
    assert_eq!(to_submit, ["expired@example.dev"]);
    assert_eq!(report.already_cached, 0);
}
