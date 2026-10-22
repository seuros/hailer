use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use serde_json::{Value, json};

use super::test_support::{ADDRESS, TOPIC, notification, ses};
use super::{
    event::EventKind,
    queue::{FeedbackQueue, QueuedMessage, process},
    *,
};
use crate::state::Store;

#[test]
fn hard_bounces_and_complaints_become_provider_neutral_events() {
    for (event_type, expected, suppression) in [
        ("Bounce", EventKind::Bounced, Some(SuppressionReason::Bounce)),
        ("Complaint", EventKind::Complained, Some(SuppressionReason::Complaint)),
        ("Delivery", EventKind::Delivered, None),
        ("DeliveryDelay", EventKind::Delayed, None),
        ("Reject", EventKind::Rejected, None),
        ("Rendering Failure", EventKind::RenderingFailed, None),
        ("Send", EventKind::Sent, None),
        ("Open", EventKind::Opened, None),
        ("Click", EventKind::Clicked, None),
    ] {
        let event =
            FeedbackEvent::from_sns(&notification("notification-1", &ses(event_type)), TOPIC)
                .expect("valid feedback");
        assert_eq!(event.kind, expected);
        assert_eq!(event.suppression, suppression);
        assert_eq!(event.message_id.as_deref(), Some("<rails-message@example.dev>"));
        assert_eq!(event.provider_message_id, "ses-message-1");
        assert_eq!(event.recipients, [ADDRESS]);
        let serialized = serde_json::to_value(&event).expect("normalized event");
        assert!(serialized.get("mail").is_none());
        assert!(serialized.get("TopicArn").is_none());
        assert!(serialized.get("bounce").is_none());
    }
}

#[test]
fn transient_and_undetermined_bounces_do_not_permanently_suppress() {
    for category in ["Transient", "Undetermined"] {
        let mut payload = ses("Bounce");
        payload["bounce"]["bounceType"] = json!(category);
        let event = FeedbackEvent::from_sns(&notification("soft-bounce", &payload), TOPIC)
            .expect("soft bounce");
        assert_eq!(event.suppression, None);
    }
    let mut payload = ses("Complaint");
    payload["complaint"]["complaintFeedbackType"] = json!("not-spam");
    let event =
        FeedbackEvent::from_sns(&notification("not-spam", &payload), TOPIC).expect("not spam");
    assert_eq!(event.suppression, None);
}

#[test]
fn legacy_ses_notification_type_and_case_insensitive_message_id_are_supported() {
    let mut payload = ses("Bounce");
    payload.as_object_mut().expect("object").remove("eventType");
    payload["notificationType"] = json!("Bounce");
    payload["mail"]["headers"][0]["name"] = json!("message-id");
    let event =
        FeedbackEvent::from_sns(&notification("legacy", &payload), TOPIC).expect("legacy SES");
    assert_eq!(event.message_id.as_deref(), Some("<rails-message@example.dev>"));
}

#[test]
fn untrusted_malformed_and_unsupported_notifications_are_rejected() {
    let valid = notification("id", &ses("Bounce"));
    let mut forged: Value = serde_json::from_str(&valid).expect("notification");
    forged["TopicArn"] = json!("arn:aws:sns:us-east-1:999999999999:forged");
    let mut confirmation = forged.clone();
    confirmation["Type"] = json!("SubscriptionConfirmation");
    for body in [
        "not json".into(),
        ses("Bounce").to_string(),
        forged.to_string(),
        confirmation.to_string(),
        notification("", &ses("Bounce")),
        notification("id", &ses("Unknown")),
    ] {
        assert!(FeedbackEvent::from_sns(&body, TOPIC).is_err());
    }
    for field in ["messageId", "timestamp", "destination"] {
        let mut payload = ses("Send");
        payload["mail"].as_object_mut().expect("mail").remove(field);
        assert!(FeedbackEvent::from_sns(&notification("id", &payload), TOPIC).is_err());
    }
    let mut payload = ses("Bounce");
    payload["bounce"]["bounceType"] = Value::Null;
    assert!(FeedbackEvent::from_sns(&notification("id", &payload), TOPIC).is_err());
}

struct FakeQueue {
    store: Store,
    fail_delete: AtomicBool,
    acknowledged: Mutex<Vec<String>>,
}

impl FeedbackQueue for FakeQueue {
    fn receive(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<QueuedMessage>>> + Send {
        std::future::ready(Ok(Vec::new()))
    }

    async fn delete(&self, receipt: &str) -> anyhow::Result<()> {
        // Acknowledge only after BOTH the event and suppression are readable.
        assert_eq!(self.store.events(0, 100).await?.events.len(), 1);
        assert!(self.store.blocked(&[ADDRESS.into()]).await?.contains("user@example.dev"));
        anyhow::ensure!(!self.fail_delete.load(Ordering::SeqCst), "simulated SQS delete failure");
        self.acknowledged.lock().expect("acknowledgements").push(receipt.into());
        Ok(())
    }
}

fn message() -> QueuedMessage {
    QueuedMessage {
        receipt: "receipt-1".into(),
        body: notification("notification-1", &ses("Bounce")),
    }
}

#[tokio::test]
async fn acknowledgement_failure_and_redelivery_do_not_duplicate_events() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let queue = FakeQueue {
        store: store.clone(),
        fail_delete: AtomicBool::new(true),
        acknowledged: Mutex::new(Vec::new()),
    };
    assert!(process(&queue, &store, TOPIC, message()).await.is_err());
    assert!(queue.acknowledged.lock().expect("acks").is_empty());
    queue.fail_delete.store(false, Ordering::SeqCst);
    process(&queue, &store, TOPIC, message()).await.expect("redelivery");
    assert_eq!(store.events(0, 100).await.expect("events").events.len(), 1);
    assert_eq!(*queue.acknowledged.lock().expect("acks"), ["receipt-1"]);
}

#[tokio::test]
async fn rejected_feedback_is_never_acknowledged_or_committed() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let queue = FakeQueue {
        store: store.clone(),
        fail_delete: AtomicBool::new(false),
        acknowledged: Mutex::new(Vec::new()),
    };
    assert!(process(&queue, &store, "wrong-topic", message()).await.is_err());
    assert!(queue.acknowledged.lock().expect("acks").is_empty());
    assert!(store.events(0, 100).await.expect("events").events.is_empty());
}

struct DrainingQueue {
    received: AtomicBool,
    deleting: tokio::sync::Notify,
    finish: tokio::sync::Notify,
    acknowledged: AtomicBool,
}

impl FeedbackQueue for DrainingQueue {
    async fn receive(&self) -> anyhow::Result<Vec<QueuedMessage>> {
        if self.received.swap(true, Ordering::SeqCst) {
            std::future::pending().await
        } else {
            Ok(vec![message(), message()])
        }
    }

    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        self.deleting.notify_one();
        self.finish.notified().await;
        self.acknowledged.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_finishes_the_current_commit_and_ack_but_leaves_the_batch_tail() {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let queue = Arc::new(DrainingQueue {
        received: AtomicBool::new(false),
        deleting: tokio::sync::Notify::new(),
        finish: tokio::sync::Notify::new(),
        acknowledged: AtomicBool::new(false),
    });
    let runtime = crate::runtime::Runtime::new(
        Arc::new(crate::control::Control::new(None, true).expect("control")),
        true,
        false,
        None,
        Arc::new(crate::domain_intel::Intel::Disabled),
        None,
    );
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let (queue, store, runtime, stop) =
            (queue.clone(), store.clone(), runtime.clone(), stop.clone());
        async move {
            queue::run(
                &*queue,
                store,
                &crate::config::FeedbackConfig {
                    queue_url: "https://sqs.example.test/queue".into(),
                    topic_arn: TOPIC.into(),
                },
                &runtime,
                stop,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), queue.deleting.notified())
        .await
        .expect("commit reached");
    assert_eq!(store.events(0, 100).await.expect("committed").events.len(), 1);
    stop.cancel();
    assert!(!task.is_finished());
    queue.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("drained")
        .expect("task")
        .expect("worker");
    assert!(queue.acknowledged.load(Ordering::SeqCst));
    assert_eq!(runtime.health(true, "closed").feedback_acknowledged, 1);
}
