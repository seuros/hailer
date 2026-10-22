use super::*;
use crate::feedback::test_support::{ADDRESS, hard_bounce};

#[tokio::test]
async fn suppressions_events_and_deduplication_survive_pool_restart() {
    let database = crate::test_support::database().await;
    let event = hard_bounce("notification-1");
    let store = Store::connect(database.url(), 5).await.expect("state");
    assert!(store.record(&event).await.expect("commit"));
    store.pool.close().await;
    let store = Store::connect(database.url(), 5).await.expect("reopened state");
    assert!(!store.record(&event).await.expect("duplicate"));
    assert_eq!(store.events(0, 100).await.expect("events").events.len(), 1);
    assert_eq!(
        store
            .blocked(&[ADDRESS.into(), "USER@example.dev".into()])
            .await
            .expect("suppressed")
            .len(),
        1
    );
}

#[tokio::test]
async fn invalid_suppression_never_commits_event_or_deduplication_key() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let mut event = hard_bounce("notification-1");
    event.recipients.push("invalid-address".into());
    assert!(store.record(&event).await.is_err());
    assert!(store.events(0, 100).await.expect("events").events.is_empty());
    assert!(store.blocked(&[ADDRESS.into()]).await.expect("suppressions").is_empty());
    event.recipients.pop();
    assert!(store.record(&event).await.expect("retry valid event"));
}

#[tokio::test]
async fn independent_consumers_can_page_events_without_destructive_reads() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    for id in ["first", "second", "third"] {
        store.record(&hard_bounce(id)).await.expect("commit");
    }
    let first = store.events(0, 2).await.expect("first page");
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.next_cursor, 2);
    let second = store.events(first.next_cursor, 2).await.expect("second page");
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.next_cursor, 3);
    let empty = store.events(3, 2).await.expect("empty page");
    assert!(empty.events.is_empty());
    assert_eq!(empty.next_cursor, 3);
    assert_eq!(store.events(0, 100).await.expect("other consumer").events.len(), 3);
    assert!(store.events(0, 0).await.is_err());
    assert!(store.events(0, 101).await.is_err());
    assert!(store.events(u64::MAX, 1).await.is_err());
}

fn imported(email: &str, reason: SuppressionReason) -> Suppression {
    Suppression {
        email_address: email.into(),
        reason,
        suppressed_at: "2026-10-19T12:00:00Z".into(),
        originating_message_id: Some("old-message".into()),
        source_event_id: None,
        metadata: serde_json::json!({"imported": true}),
    }
}

#[tokio::test]
async fn import_is_atomic_and_preserves_commonshare_matching() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    assert!(
        store
            .import(vec![
                imported("USER+old@example.dev", SuppressionReason::Bounce),
                imported("invalid", SuppressionReason::Complaint),
            ])
            .await
            .is_err()
    );
    assert!(store.blocked(&[ADDRESS.into()]).await.expect("state").is_empty());
    assert_eq!(
        store
            .import(vec![imported("USER+old@example.dev", SuppressionReason::Bounce)])
            .await
            .expect("import"),
        1
    );
    assert!(store.blocked(&[ADDRESS.into()]).await.expect("lookup").contains("user@example.dev"));
    assert!(store.events(0, 100).await.expect("no artificial feedback").events.is_empty());
}

#[tokio::test]
async fn imports_cannot_downgrade_complaints() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.import(vec![imported(ADDRESS, SuppressionReason::Complaint)]).await.expect("complaint");
    store.record(&hard_bounce("later-bounce")).await.expect("bounce");
    store.import(vec![imported(ADDRESS, SuppressionReason::Bounce)]).await.expect("old import");
    let row = sqlx::query("SELECT reason, metadata FROM hailer.suppressions")
        .fetch_one(&store.pool)
        .await
        .expect("suppression");
    assert_eq!(row.get::<String, _>("reason"), "complaint");
    assert_eq!(row.get::<Json<serde_json::Value>, _>("metadata").0["imported"], true);
}

#[tokio::test]
async fn concurrent_redeliveries_commit_once_and_other_pools_see_the_result() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let other = Store::connect(database.url(), 5).await.expect("second process");
    let tasks: Vec<_> = (0..10)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.record(&hard_bounce("same")).await.expect("record") })
        })
        .collect();
    let mut inserted = 0;
    for task in tasks {
        inserted += usize::from(task.await.expect("writer"));
    }
    assert_eq!(inserted, 1);
    assert_eq!(other.events(0, 100).await.expect("shared events").next_cursor, 1);
    assert!(
        other
            .blocked(&[ADDRESS.into()])
            .await
            .expect("shared suppression")
            .contains("user@example.dev")
    );
}

#[tokio::test]
async fn sequence_allocation_is_locked_until_commit_so_cursors_never_skip_events() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let mut first = store.write().await.expect("first writer");
    sqlx::query("UPDATE hailer.event_counter SET value = 1 WHERE id = 1")
        .execute(&mut *first)
        .await
        .expect("allocate");
    sqlx::query("INSERT INTO hailer.events VALUES (1, 'first', $1, clock_timestamp())")
        .bind(Json(hard_bounce("first")))
        .execute(&mut *first)
        .await
        .expect("pending first");
    let second = store.clone();
    let mut task = tokio::spawn(async move { second.record(&hard_bounce("second")).await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut task).await.is_err());
    assert!(store.events(0, 100).await.expect("no premature cursor").events.is_empty());
    first.commit().await.expect("commit first");
    assert!(task.await.expect("second task").expect("commit second"));
    let page = store.events(0, 100).await.expect("ordered committed events");
    assert_eq!(page.events.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(page.next_cursor, 2);
}

#[tokio::test]
async fn database_error_rolls_back_counter_event_and_suppression() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    sqlx::query("ALTER TABLE hailer.suppressions ADD CONSTRAINT reject_fixture CHECK (email_address <> 'user@example.dev')")
        .execute(&store.pool).await.expect("inject database failure");
    assert!(store.record(&hard_bounce("retry")).await.is_err());
    assert!(store.events(0, 100).await.expect("rollback").events.is_empty());
    sqlx::query("ALTER TABLE hailer.suppressions DROP CONSTRAINT reject_fixture")
        .execute(&store.pool)
        .await
        .expect("remove failure");
    assert!(store.record(&hard_bounce("retry")).await.expect("retry"));
    assert_eq!(store.events(0, 100).await.expect("counter rollback").next_cursor, 1);
}

#[test]
fn quoted_local_parts_are_not_mistaken_for_plus_aliases() {
    assert_eq!(normalize(r#""a+b"@example.dev"#).expect("quoted mailbox"), r#""a+b"@example.dev"#);
    assert_eq!(normalize(r#""a@b"@example.dev"#).expect("quoted at"), r#""a@b"@example.dev"#);
}
