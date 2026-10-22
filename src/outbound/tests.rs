use std::assert_matches;

use sqlx::Row;
use sqlx::types::Json;

use super::lifecycle::{Lifecycle, MessageLifecycleEvent};
use super::*;
use crate::config::QueuedConfig;
use crate::state::Store;

pub(super) fn budget() -> QueuedConfig {
    QueuedConfig { max_bytes: 10_000_000, max_messages: 100, unknown_retention_seconds: 604_800 }
}

pub(super) fn submission(key: &str) -> Submission {
    serde_json::from_value(serde_json::json!({
        "submission_key": key,
        "raw_mime": "From: from@example.com\r\nMessage-ID: <intent@example.com>\r\n\r\nHello",
        "to_addresses": ["to@example.com"],
        "bcc_addresses": ["blind@example.com"]
    }))
    .expect("submission")
}

#[tokio::test]
async fn intake_commits_payload_receipt_lifecycle_and_feed_together() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let receipt = store.submit(submission("intent"), &budget()).await.expect("accept");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "queued");
    assert_eq!(status.version, 0);
    assert!(status.body_present);
    assert!(status.provider_message_id.is_none());
    assert_eq!(status.rfc_message_id.as_deref(), Some("<intent@example.com>"));
    assert_eq!(status.receipt, receipt);
    let start = receipt.accepted_at.parse::<jiff::Timestamp>().expect("start");
    let expiry = receipt.expires_at.parse::<jiff::Timestamp>().expect("expiry");
    assert_eq!(expiry.as_millisecond() - start.as_millisecond(), SEND_WINDOW_MS);
    let page = store.message_events(0, 100).await.expect("feed");
    assert_eq!(page.next_cursor, 1);
    assert_eq!(page.events[0].event.kind, "queued");
    assert_eq!(page.events[0].event.hailer_message_id, receipt.hailer_message_id);
    assert!(store.events(0, 100).await.expect("legacy feed").events.is_empty());
    let row = sqlx::query("SELECT payload, envelope FROM hailer.messages WHERE id = $1")
        .bind(&receipt.hailer_message_id)
        .fetch_one(&store.pool)
        .await
        .expect("body");
    let payload: Vec<u8> = row.try_get("payload").expect("payload");
    assert_eq!(payload, submission("intent").raw_mime.as_bytes());
    let Json(envelope): Json<serde_json::Value> = row.try_get("envelope").expect("envelope");
    assert!(envelope.get("raw_mime").is_none());
    assert_eq!(envelope["bcc_addresses"], serde_json::json!(["blind@example.com"]));
}

#[tokio::test]
async fn spool_preserves_rendered_mime_bytes_including_json_escaped_nuls() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let mut command = submission("binary");
    command.raw_mime.push_str("\0non-ASCII: \u{2603}");
    let expected = command.raw_mime.as_bytes().to_vec();
    let receipt = store.submit(command, &budget()).await.expect("accept");
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT payload FROM hailer.messages WHERE id = $1")
        .bind(&receipt.hailer_message_id)
        .fetch_one(&store.pool)
        .await
        .expect("payload");
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn concurrent_identical_commands_get_one_receipt_and_conflicts_never_overwrite() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let another = Store::connect(database.url(), 5).await.expect("independent pool");
    let limits = budget();
    let (first, second) = tokio::join!(
        store.submit(submission("same"), &limits),
        another.submit(submission("same"), &limits)
    );
    assert_eq!(first.expect("first"), second.expect("replay"));
    for change in ["body", "recipient", "expiry"] {
        let mut command = submission("same");
        match change {
            "body" => command.raw_mime.push('!'),
            "recipient" => command.bcc_addresses.push("different@example.com".into()),
            _ => command.expires_at = Some("2030-01-01T00:00:00Z".into()),
        }
        assert_matches!(store.submit(command, &budget()).await, Err(IntakeError::Conflict));
    }
    assert_eq!(store.message_events(0, 100).await.expect("feed").events.len(), 1);
}

#[tokio::test]
async fn concurrent_conflicting_commands_have_exactly_one_winner() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let mut other = submission("same");
    other.raw_mime.push('!');
    let limits = budget();
    let (first, second) =
        tokio::join!(store.submit(submission("same"), &limits), store.submit(other, &limits));
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(store.message_events(0, 100).await.expect("feed").events.len(), 1);
}

#[tokio::test]
async fn replay_survives_payload_purge_deadline_and_pool_restart() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let receipt = store.submit(submission("same"), &budget()).await.expect("accept");
    // Simulate terminal retention while preserving the immutable receipt.
    let mut life = Lifecycle::queued();
    life.apply(MessageLifecycleEvent::Expire).expect("expire");
    sqlx::query(
        "UPDATE hailer.messages SET payload = NULL, status = 'expired', lifecycle = $1, \
                 accepted_at = clock_timestamp() - interval '2 days', \
                 expires_at = clock_timestamp() - interval '1 day' WHERE id = $2",
    )
    .bind(Json(life.persist().expect("snapshot")))
    .bind(&receipt.hailer_message_id)
    .execute(&store.pool)
    .await
    .expect("simulate retention");
    store.pool.close().await;
    let store = Store::connect(database.url(), 5).await.expect("restart");
    assert_eq!(store.submit(submission("same"), &budget()).await.expect("replay"), receipt);
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "expired");
    assert!(!status.body_present);
}

#[tokio::test]
async fn capacity_is_atomic_and_replays_do_not_reserve_again() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let one = QueuedConfig { max_messages: 1, ..budget() };
    let receipt = store.submit(submission("first"), &one).await.expect("accept");
    assert!(!store.intake_ready(&one).await);
    assert_matches!(store.submit(submission("second"), &one).await, Err(IntakeError::Capacity));
    assert_eq!(store.submit(submission("first"), &one).await.expect("replay"), receipt);
    let row =
        sqlx::query("SELECT live_messages, live_bytes FROM hailer.outbound_counter WHERE id = 1")
            .fetch_one(&store.pool)
            .await
            .expect("counters");
    assert_eq!(row.get::<i64, _>("live_messages"), 1);
    assert_eq!(
        row.get::<i64, _>("live_bytes"),
        Prepared::new(submission("first")).expect("prepared").bytes
    );
    let tiny = QueuedConfig { max_bytes: 1, ..budget() };
    assert_matches!(store.submit(submission("third"), &tiny).await, Err(IntakeError::Capacity));
}

#[tokio::test]
async fn concurrent_intake_cannot_overbook_the_last_spool_slot() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let limits = QueuedConfig { max_messages: 1, ..budget() };
    let (first, second) = tokio::join!(
        store.submit(submission("first"), &limits),
        store.submit(submission("second"), &limits)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_matches!(
        (first, second),
        (Ok(_), Err(IntakeError::Capacity)) | (Err(IntakeError::Capacity), Ok(_))
    );
    assert_eq!(store.message_events(0, 100).await.expect("feed").events.len(), 1);
}

#[tokio::test]
async fn read_only_storage_never_reports_intake_ready() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 1).await.expect("store");
    assert!(store.intake_ready(&budget()).await);
    sqlx::query("SET default_transaction_read_only = on")
        .execute(&store.pool)
        .await
        .expect("read-only fault");
    assert!(!store.intake_ready(&budget()).await);
    sqlx::query("SET default_transaction_read_only = off")
        .execute(&store.pool)
        .await
        .expect("restore");
    assert!(store.intake_ready(&budget()).await);
}

#[tokio::test]
async fn lifecycle_cursor_never_skips_an_uncommitted_earlier_event() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let receipt = store.submit(submission("first"), &budget()).await.expect("accept");
    let mut tx = store.outbound_write().await.expect("writer");
    super::store::event(
        &mut tx,
        &receipt.hailer_message_id,
        "test_observation",
        serde_json::json!({}),
    )
    .await
    .expect("pending event");
    let mut later = tokio::spawn({
        let store = store.clone();
        async move { store.submit(submission("later"), &budget()).await }
    });
    assert!(tokio::time::timeout(std::time::Duration::from_millis(100), &mut later).await.is_err());
    assert_eq!(store.message_events(0, 100).await.expect("visible feed").next_cursor, 1);
    tx.commit().await.expect("commit earlier event");
    later.await.expect("writer task").expect("later acceptance");
    let page = store.message_events(1, 100).await.expect("committed feed");
    assert_eq!(page.events.iter().map(|entry| entry.sequence).collect::<Vec<_>>(), [2, 3]);
}

#[tokio::test]
async fn failed_event_insert_rolls_back_ownership_and_capacity() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    sqlx::raw_sql(
        "CREATE FUNCTION hailer.fail_queue_event() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'injected'; END $$; \
         CREATE TRIGGER fail_queue_event BEFORE INSERT ON hailer.message_events \
         FOR EACH ROW EXECUTE FUNCTION hailer.fail_queue_event()",
    )
    .execute(&store.pool)
    .await
    .expect("fault");
    assert_matches!(
        store.submit(submission("rollback"), &budget()).await,
        Err(IntakeError::Unavailable)
    );
    let messages: i64 = sqlx::query_scalar("SELECT count(*) FROM hailer.messages")
        .fetch_one(&store.pool)
        .await
        .expect("messages");
    assert_eq!(messages, 0);
    let row = sqlx::query(
        "SELECT sequence, live_bytes, live_messages FROM hailer.outbound_counter WHERE id = 1",
    )
    .fetch_one(&store.pool)
    .await
    .expect("counters");
    for column in ["sequence", "live_bytes", "live_messages"] {
        assert_eq!(row.get::<i64, _>(column), 0);
    }
}

#[test]
fn fingerprints_are_typed_canonical_and_exclude_server_defaults() {
    let first = Prepared::new(submission("first")).expect("first");
    let mut other = submission("other-key");
    other.cc_addresses = Vec::new();
    let second = Prepared::new(other).expect("second");
    assert_eq!(first.fingerprint, second.fingerprint);
    let mut utc = submission("key");
    utc.expires_at = Some("2026-10-22T00:00:00Z".into());
    let mut offset = submission("key");
    offset.expires_at = Some("2026-10-22T01:00:00+01:00".into());
    assert_eq!(
        Prepared::new(utc).expect("utc").fingerprint,
        Prepared::new(offset).expect("offset").fingerprint
    );
}

#[test]
fn lifecycle_restore_rejects_schema_or_index_drift_and_unknown_never_expires() {
    let mut life = Lifecycle::queued();
    life.apply(MessageLifecycleEvent::Claim).expect("claim");
    life.apply(MessageLifecycleEvent::Dispatch).expect("dispatch");
    life.apply(MessageLifecycleEvent::Uncertain).expect("unknown");
    let value = life.persist().expect("persist");
    let mut restored = Lifecycle::restore(value.clone(), "held_unknown").expect("restore");
    assert!(restored.apply(MessageLifecycleEvent::Expire).is_err());
    assert!(restored.apply(MessageLifecycleEvent::Claim).is_err());
    assert!(Lifecycle::restore(value.clone(), "queued").is_err());
    let mut corrupt = value;
    corrupt["schema_version"] = serde_json::json!(2);
    assert!(Lifecycle::restore(corrupt, "held_unknown").is_err());
}

#[test]
fn message_id_metadata_is_bounded_unambiguous_and_never_guessed_from_body() {
    assert_eq!(
        original_message_id(
            "From: from@example.com\r\nmEsSaGe-Id: <original@example.com>\r\n\r\nbody"
        ),
        Some("<original@example.com>".into())
    );
    assert_eq!(original_message_id("From: from@example.com\r\n\r\nMessage-ID: <body>"), None);
    assert_eq!(original_message_id("Message-ID: <one>\r\nMessage-ID: <two>\r\n\r\nbody"), None);
    assert_eq!(original_message_id("Message-ID: <one>"), None);
    assert_eq!(
        original_message_id(&format!(
            "Message-ID: <one>\r\nX-Large: {}\r\n\r\nbody",
            "x".repeat(65_536)
        )),
        None
    );
}

#[tokio::test]
async fn invalid_input_and_invalid_new_deadlines_never_commit() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    for expiry in ["2000-01-01T00:00:00Z", "2099-01-01T00:00:00Z", "invalid"] {
        let mut command = submission("invalid");
        command.expires_at = Some(expiry.into());
        assert_matches!(store.submit(command, &budget()).await, Err(IntakeError::Invalid));
    }
    let mut command = submission("invalid");
    command.to_addresses = vec!["bad address".into()];
    assert_matches!(store.submit(command, &budget()).await, Err(IntakeError::Invalid));
    assert_eq!(store.message_events(0, 100).await.expect("feed").next_cursor, 0);
}
