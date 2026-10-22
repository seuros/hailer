use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sqlx::types::Json;
use tokio_util::sync::CancellationToken;

use super::dispatch::Ticket;
use super::tests::{budget, submission};
use super::*;
use crate::control::Control;
use crate::feedback::FeedbackEvent;
use crate::feedback::test_support::{TOPIC, notification, ses};
use crate::provider::queued::{DispatchOutcome, QueuedProvider};
use crate::provider::{MailProvider, ProviderError};
use crate::runtime::Runtime;
use crate::state::{Store, Suppression};

fn control() -> Control {
    Control::new(None, true).expect("control")
}

async fn ticket(store: &Store) -> Ticket {
    let claim = store.claim_message().await.expect("claim").expect("work");
    store.prepare_dispatch(&claim, &control()).await.expect("prepare").expect("ticket")
}

async fn expire_lease(store: &Store, id: &str) {
    sqlx::query("UPDATE hailer.attempts SET lease_until = clock_timestamp() - interval '1 second' WHERE message_id = $1")
        .bind(id).execute(&store.pool).await.expect("lease fault");
    sqlx::query("UPDATE hailer.messages SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1")
        .bind(id).execute(&store.pool).await.expect("message lease fault");
}

async fn due(store: &Store, id: &str) {
    sqlx::query("UPDATE hailer.messages SET next_attempt_at = clock_timestamp() WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await
        .expect("due");
}

async fn block(store: &Store, address: &str) {
    let record: Suppression = serde_json::from_value(serde_json::json!({
        "email_address": address, "reason": "bounce", "suppressed_at": "2026-01-01T00:00:00Z"
    }))
    .expect("suppression");
    store.import(vec![record]).await.expect("block");
}

fn feedback(ticket: &Ticket, id: &str, kind: &str, address: &str) -> FeedbackEvent {
    let mut document = ses(kind);
    document["mail"]["tags"] = serde_json::json!({
        "hailer_message_id": [ticket.correlation.hailer_message_id],
        "hailer_attempt_id": [ticket.correlation.attempt_id],
    });
    document["mail"]["messageId"] = serde_json::json!(id);
    document["mail"]["destination"] = serde_json::json!([address]);
    if kind == "Delivery" {
        document["delivery"]["recipients"] = serde_json::json!([address]);
    }
    if kind == "Bounce" {
        document["bounce"]["bouncedRecipients"] = serde_json::json!([{"emailAddress": address}]);
    }
    FeedbackEvent::from_sns(&notification(&format!("{id}-{kind}"), &document), TOPIC)
        .expect("feedback")
}

#[tokio::test]
async fn claims_are_exclusive_and_pre_dispatch_worker_loss_is_reclaimable() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("claim"), &budget()).await.expect("accept");
    let (one, two) = tokio::join!(store.claim_message(), store.claim_message());
    let one = one.expect("one");
    let two = two.expect("two");
    assert_eq!(usize::from(one.is_some()) + usize::from(two.is_some()), 1);
    let old = one.or(two).expect("claim");
    expire_lease(&store, &receipt.hailer_message_id).await;
    store.maintain_messages(604_800).await.expect("recover");
    let new = store.claim_message().await.expect("new claim").expect("work");
    assert_ne!(old.correlation.attempt_id, new.correlation.attempt_id);
    assert!(store.prepare_dispatch(&old, &control()).await.expect("stale fence").is_none());
    assert!(store.prepare_dispatch(&new, &control()).await.expect("new fence").is_some());
}

#[tokio::test]
async fn crossing_dispatch_boundary_changes_crash_recovery_to_held_unknown() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("unknown"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    assert!(store.dispatch_fence(&dispatch).await.expect("fence"));
    expire_lease(&store, &receipt.hailer_message_id).await;
    store.maintain_messages(604_800).await.expect("recover");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "held_unknown");
    assert_eq!(status.attempts[0]["phase"], "unknown");
    assert!(status.body_present);
    due(&store, &receipt.hailer_message_id).await;
    assert!(store.claim_message().await.expect("no replay").is_none());
    assert!(!store.dispatch_fence(&dispatch).await.expect("stale fence"));
    store
        .finish_dispatch(&dispatch.correlation, &DispatchOutcome::Accepted("late-accepted".into()))
        .await
        .expect("late response");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "accepted");
    assert!(!status.body_present);
}

#[tokio::test]
async fn durable_allowed_subset_matches_the_transport_and_original_body_is_unchanged() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("partial"), &budget()).await.expect("accept");
    block(&store, "to@example.com").await;
    let dispatch = ticket(&store).await;
    assert_eq!(dispatch.request.to_addresses, [] as [String; 0]);
    assert_eq!(dispatch.request.bcc_addresses, ["blind@example.com"]);
    assert_eq!(dispatch.request.raw_mime, submission("partial").raw_mime);
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.attempts[0]["envelope"]["to_addresses"], serde_json::json!([]));
    assert_eq!(status.attempts[0]["excluded"][0]["recipient"], "to@example.com");
    let Json(original): Json<serde_json::Value> =
        sqlx::query_scalar("SELECT envelope FROM hailer.messages WHERE id = $1")
            .bind(&receipt.hailer_message_id)
            .fetch_one(&store.pool)
            .await
            .expect("original envelope");
    assert_eq!(original["to_addresses"], serde_json::json!(["to@example.com"]));
    store
        .finish_dispatch(&dispatch.correlation, &DispatchOutcome::Accepted("ses-partial".into()))
        .await
        .expect("finish");
    assert_eq!(store.submit(submission("partial"), &budget()).await.expect("replay"), receipt);
    assert!(store.claim_message().await.expect("no resend").is_none());
    let count: i64 =
        sqlx::query_scalar("SELECT live_messages FROM hailer.outbound_counter WHERE id = 1")
            .fetch_one(&store.pool)
            .await
            .expect("count");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn all_suppressed_and_expired_queued_work_purge_without_dispatch() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("blocked"), &budget()).await.expect("accept");
    block(&store, "to@example.com").await;
    block(&store, "blind@example.com").await;
    let claim = store.claim_message().await.expect("claim").expect("work");
    assert!(store.prepare_dispatch(&claim, &control()).await.expect("suppressed").is_none());
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "suppressed");
    assert!(!status.body_present);
    let expired = store.submit(submission("expired"), &budget()).await.expect("accept");
    sqlx::query(
        "UPDATE hailer.messages SET accepted_at = clock_timestamp() - interval '2 days', \
        expires_at = clock_timestamp() - interval '1 day' WHERE id = $1",
    )
    .bind(&expired.hailer_message_id)
    .execute(&store.pool)
    .await
    .expect("expiry");
    store.maintain_messages(604_800).await.expect("maintain");
    let status =
        store.message_status(&expired.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "expired");
    assert!(!status.body_present);
}

#[tokio::test]
async fn only_definite_failures_retry_and_manual_unknown_retry_keeps_history() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("retry"), &budget()).await.expect("accept");
    let first = ticket(&store).await;
    store
        .finish_dispatch(
            &first.correlation,
            &DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true },
        )
        .await
        .expect("safe failure");
    due(&store, &receipt.hailer_message_id).await;
    let second = ticket(&store).await;
    store.finish_dispatch(&second.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    assert!(
        store
            .resolve_message(
                &receipt.hailer_message_id,
                Resolution::Retry { acknowledge_duplicate_risk: false },
                "investigated"
            )
            .await
            .is_err()
    );
    assert!(store.claim_message().await.expect("held").is_none());
    store
        .resolve_message(
            &receipt.hailer_message_id,
            Resolution::Retry { acknowledge_duplicate_risk: true },
            "explicit duplicate risk",
        )
        .await
        .expect("authorize");
    let third = ticket(&store).await;
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.attempts[0]["phase"], "not_accepted");
    assert_eq!(status.attempts[1]["phase"], "unknown");
    assert_eq!(status.attempts[2]["phase"], "dispatching");
    // A late historical acceptance cannot overwrite this newer attempt's journal.
    store
        .finish_dispatch(&second.correlation, &DispatchOutcome::Accepted("ses-historical".into()))
        .await
        .expect("late evidence");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "dispatching");
    assert_eq!(status.attempts[2]["phase"], "dispatching");
    assert!(!status.body_present);
    store
        .finish_dispatch(&third.correlation, &DispatchOutcome::Unknown)
        .await
        .expect("new unknown");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "accepted");
    assert_eq!(status.attempts[2]["phase"], "unknown");
    assert!(store.claim_message().await.expect("no further send").is_none());
    assert!(
        store
            .resolve_attempt(
                &receipt.hailer_message_id,
                &third.correlation.attempt_id,
                Resolution::Retry { acknowledge_duplicate_risk: true },
                "cannot retry accepted intent",
            )
            .await
            .is_err()
    );
    store
        .resolve_attempt(
            &receipt.hailer_message_id,
            &third.correlation.attempt_id,
            Resolution::NotAccepted,
            "confirmed newer attempt did not send",
        )
        .await
        .expect("resolve remaining audit uncertainty");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "accepted");
    assert_eq!(status.attempts[2]["phase"], "not_accepted");
}

#[tokio::test]
async fn cancellation_is_fenced_and_unknown_retention_never_labels_mail_unsent() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let cancelled = store.submit(submission("cancel"), &budget()).await.expect("accept");
    let claim = store.claim_message().await.expect("claim").expect("work");
    store.cancel_message(&cancelled.hailer_message_id, "business withdrawn").await.expect("cancel");
    assert!(store.prepare_dispatch(&claim, &control()).await.expect("fenced").is_none());
    let held = store.submit(submission("retain"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    assert!(store.cancel_message(&held.hailer_message_id, "too late").await.is_err());
    store.finish_dispatch(&dispatch.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    sqlx::query(
        "UPDATE hailer.messages SET accepted_at = clock_timestamp() - interval '8 days', \
        expires_at = clock_timestamp() - interval '7 days' WHERE id = $1",
    )
    .bind(&held.hailer_message_id)
    .execute(&store.pool)
    .await
    .expect("retention age");
    store.maintain_messages(604_800).await.expect("purge");
    store.maintain_messages(604_800).await.expect("idempotent purge");
    let status =
        store.message_status(&held.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "held_unknown");
    assert_eq!(status.attempts[0]["phase"], "unknown");
    assert!(!status.body_present);
    assert!(
        store
            .resolve_message(
                &held.hailer_message_id,
                Resolution::Retry { acknowledge_duplicate_risk: true },
                "too old"
            )
            .await
            .is_err()
    );
    assert_eq!(store.submit(submission("retain"), &budget()).await.expect("replay"), held);
    store
        .resolve_message(&held.hailer_message_id, Resolution::NotAccepted, "confirmed not accepted")
        .await
        .expect("resolve");
    let count: i64 =
        sqlx::query_scalar("SELECT live_bytes FROM hailer.outbound_counter WHERE id = 1")
            .fetch_one(&store.pool)
            .await
            .expect("budget");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn trusted_unique_feedback_resolves_unknown_and_later_bounces_do_not_reopen_it() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("feedback"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    store.finish_dispatch(&dispatch.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    let mut unrelated = feedback(&dispatch, "ses-related", "Delivery", "to@example.com");
    unrelated.correlation = None; // Similar address/Message-ID is not evidence.
    store.record(&unrelated).await.expect("legacy event");
    assert_eq!(
        store
            .message_status(&receipt.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "held_unknown"
    );
    let delivered = feedback(&dispatch, "ses-confirmed", "Delivery", "to@example.com");
    store.record(&delivered).await.expect("correlate");
    assert!(!store.record(&delivered).await.expect("dedup"));
    let mut bounce = feedback(&dispatch, "ses-confirmed", "Bounce", "to@example.com");
    bounce.correlation = None; // A previously captured provider ID is unique evidence.
    store.record(&bounce).await.expect("late untagged bounce");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "accepted");
    assert!(!status.body_present);
    assert_eq!(status.recipient_outcomes.as_array().expect("facts").len(), 2);
    assert!(
        store
            .blocked(&["to@example.com".into()])
            .await
            .expect("suppression")
            .contains("to@example.com")
    );
    assert!(store.claim_message().await.expect("no resend").is_none());
}

#[tokio::test]
async fn result_commit_failure_recovers_unknown_not_retryable() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("commit-loss"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    sqlx::raw_sql("CREATE FUNCTION hailer.fail_result() RETURNS trigger LANGUAGE plpgsql AS $$ \
        BEGIN IF NEW.kind = 'provider_accepted' THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$; \
        CREATE TRIGGER fail_result BEFORE INSERT ON hailer.message_events FOR EACH ROW EXECUTE FUNCTION hailer.fail_result()")
        .execute(&store.pool).await.expect("fault");
    assert!(
        store
            .finish_dispatch(
                &dispatch.correlation,
                &DispatchOutcome::Accepted("lost-result".into())
            )
            .await
            .is_err()
    );
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "dispatching");
    assert!(status.body_present);
    expire_lease(&store, &receipt.hailer_message_id).await;
    store.maintain_messages(604_800).await.expect("recover");
    assert_eq!(
        store
            .message_status(&receipt.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "held_unknown"
    );
    assert!(store.claim_message().await.expect("no replay").is_none());
}

#[tokio::test]
async fn invalid_feedback_correlation_cannot_resolve_another_attempt() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let first = store.submit(submission("one"), &budget()).await.expect("accept");
    let one = ticket(&store).await;
    store.finish_dispatch(&one.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    let second = store.submit(submission("two"), &budget()).await.expect("accept");
    let two = ticket(&store).await;
    store.finish_dispatch(&two.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    let wrong_address =
        feedback(&one, "wrong-recipient", "Delivery", "not-in-envelope@example.com");
    store.record(&wrong_address).await.expect("legacy observation only");
    assert_eq!(
        store
            .message_status(&first.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "held_unknown"
    );
    store
        .record(&feedback(&one, "shared-provider-id", "Delivery", "to@example.com"))
        .await
        .expect("positive evidence");
    let mut collision = feedback(&two, "shared-provider-id", "Delivery", "to@example.com");
    collision.event_id.push_str("-different-notification");
    store.record(&collision).await.expect("ambiguous mapping remains legacy-only");
    assert_eq!(
        store
            .message_status(&second.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "held_unknown"
    );
}

#[tokio::test]
async fn correlated_feedback_failure_rolls_back_feedback_suppression_and_spool_together() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("atomic-feedback"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    store.finish_dispatch(&dispatch.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    sqlx::raw_sql("CREATE FUNCTION hailer.fail_correlated() RETURNS trigger LANGUAGE plpgsql AS $$ \
        BEGIN IF NEW.kind = 'recipient_observed' THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$; \
        CREATE TRIGGER fail_correlated BEFORE INSERT ON hailer.message_events FOR EACH ROW EXECUTE FUNCTION hailer.fail_correlated()")
        .execute(&store.pool).await.expect("fault");
    let event = feedback(&dispatch, "ses-atomic", "Bounce", "to@example.com");
    assert!(store.record(&event).await.is_err());
    assert!(store.events(0, 100).await.expect("feedback").events.is_empty());
    assert!(store.blocked(&["to@example.com".into()]).await.expect("suppression").is_empty());
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "held_unknown");
    assert!(status.body_present);
    assert_eq!(status.recipient_outcomes, serde_json::json!([]));
    sqlx::query("DROP TRIGGER fail_correlated ON hailer.message_events")
        .execute(&store.pool)
        .await
        .expect("recover");
    store.record(&event).await.expect("redelivery");
    assert_eq!(
        store
            .message_status(&receipt.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "accepted"
    );
}

#[tokio::test]
async fn status_observation_windows_are_bounded_and_explicitly_truncated() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("observations"), &budget()).await.expect("accept");
    let dispatch = ticket(&store).await;
    store
        .finish_dispatch(&dispatch.correlation, &DispatchOutcome::Accepted("ses-opens".into()))
        .await
        .expect("accept");
    for index in 0..101 {
        let mut event = feedback(&dispatch, "ses-opens", "Open", "to@example.com");
        event.event_id = format!("observation-{index}");
        store.record(&event).await.expect("observation");
    }
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.recipient_outcomes.as_array().expect("bounded facts").len(), 100);
    assert!(status.recipient_outcomes_truncated);
    assert!(!status.attempts_truncated);
    let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM hailer.recipient_facts")
        .fetch_one(&store.pool)
        .await
        .expect("retained history");
    assert_eq!(facts, 101);
}

#[tokio::test]
async fn local_policy_denials_are_safe_and_operator_acceptance_is_audited() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("policy"), &budget()).await.expect("accept");
    let claim = store.claim_message().await.expect("claim").expect("work");
    assert!(
        store
            .prepare_dispatch(&claim, &Control::new(None, false).expect("unready"))
            .await
            .expect("denied")
            .is_none()
    );
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "retry_waiting");
    assert_eq!(status.attempts[0]["phase"], "abandoned");
    due(&store, &receipt.hailer_message_id).await;
    let dispatch = ticket(&store).await;
    store.finish_dispatch(&dispatch.correlation, &DispatchOutcome::Unknown).await.expect("unknown");
    store
        .resolve_message(
            &receipt.hailer_message_id,
            Resolution::Accepted("operator-confirmed".into()),
            "provider investigation confirmed",
        )
        .await
        .expect("operator confirmation");
    let status =
        store.message_status(&receipt.hailer_message_id).await.expect("status").expect("message");
    assert_eq!(status.status, "accepted");
    assert!(!status.body_present);
    assert!(
        store
            .message_events(0, 100)
            .await
            .expect("audit")
            .events
            .iter()
            .any(|entry| entry.event.kind == "operator_resolved")
    );
}

struct Probe {
    store: Store,
    calls: AtomicUsize,
    started: tokio::sync::Notify,
    finish: Option<tokio::sync::Notify>,
}

impl MailProvider for Probe {
    fn send(
        &self,
        _: &SendRequest,
    ) -> impl std::future::Future<Output = Result<String, ProviderError>> + Send {
        std::future::ready(Err(ProviderError::InvalidRequest))
    }
}

impl QueuedProvider for Probe {
    async fn dispatch(&self, request: &SendRequest, correlation: &Correlation) -> DispatchOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let status = self
            .store
            .message_status(&correlation.hailer_message_id)
            .await
            .expect("durable status")
            .expect("message");
        assert_eq!(status.status, "dispatching");
        assert_eq!(status.attempts[0]["phase"], "dispatching");
        assert_eq!(request.raw_mime, submission("probe").raw_mime);
        // Proves no spool transaction is held across the external effect.
        let tx = self.store.outbound_write().await.expect("external concurrent transaction");
        tx.rollback().await.expect("rollback");
        self.started.notify_one();
        if let Some(finish) = &self.finish {
            finish.notified().await;
        }
        DispatchOutcome::Accepted("probe-ses-id".into())
    }
}

#[tokio::test]
async fn supervised_worker_drains_one_effect_but_leaves_the_queue_tail_durable() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("probe"), &budget()).await.expect("accept");
    let tail = store.submit(submission("tail"), &budget()).await.expect("accept");
    sqlx::query("UPDATE hailer.messages SET next_attempt_at = clock_timestamp() + interval '1 hour' WHERE id = $1")
        .bind(&tail.hailer_message_id).execute(&store.pool).await.expect("defer tail");
    let runtime = Runtime::new(Arc::new(control()), false, false, Some(budget()));
    let stop = CancellationToken::new();
    let provider = Arc::new(Probe {
        store: store.clone(),
        calls: AtomicUsize::new(0),
        started: tokio::sync::Notify::new(),
        finish: Some(tokio::sync::Notify::new()),
    });
    let worker = tokio::spawn(run(provider.clone(), store.clone(), runtime.clone(), stop.clone()));
    tokio::time::timeout(Duration::from_secs(3), provider.started.notified())
        .await
        .expect("started");
    runtime.control.drain();
    stop.cancel();
    assert!(!worker.is_finished());
    provider.finish.as_ref().expect("finish").notify_one();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .expect("drain")
        .expect("task")
        .expect("worker");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .message_status(&receipt.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "accepted"
    );
    assert_eq!(
        store.message_status(&tail.hailer_message_id).await.expect("tail").expect("message").status,
        "queued"
    );
}

#[tokio::test]
async fn forced_worker_loss_and_restart_never_repeat_an_uncertain_effect() {
    let db = crate::test_support::database().await;
    let store = Store::connect(db.url(), 5).await.expect("store");
    let receipt = store.submit(submission("probe"), &budget()).await.expect("accept");
    let runtime = Runtime::new(Arc::new(control()), false, false, Some(budget()));
    let provider = Arc::new(Probe {
        store: store.clone(),
        calls: AtomicUsize::new(0),
        started: tokio::sync::Notify::new(),
        finish: Some(tokio::sync::Notify::new()),
    });
    let first = tokio::spawn(run(
        provider.clone(),
        store.clone(),
        runtime.clone(),
        CancellationToken::new(),
    ));
    tokio::time::timeout(Duration::from_secs(3), provider.started.notified())
        .await
        .expect("started");
    first.abort();
    assert!(first.await.expect_err("aborted").is_cancelled());
    expire_lease(&store, &receipt.hailer_message_id).await;
    let stop = CancellationToken::new();
    let second = tokio::spawn(run(provider.clone(), store.clone(), runtime, stop.clone()));
    tokio::time::sleep(Duration::from_millis(600)).await;
    stop.cancel();
    second.await.expect("task").expect("worker");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .message_status(&receipt.hailer_message_id)
            .await
            .expect("status")
            .expect("message")
            .status,
        "held_unknown"
    );
}
