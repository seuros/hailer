use std::assert_matches;
use std::sync::Mutex;

use super::*;
use crate::feedback::test_support::{ADDRESS, hard_bounce};

#[derive(Default)]
struct FakeSender(Mutex<Vec<SendRequest>>);

impl MailProvider for FakeSender {
    fn send(
        &self,
        request: &SendRequest,
    ) -> impl std::future::Future<Output = Result<String, ProviderError>> + Send {
        self.0.lock().expect("requests").push(request.clone());
        std::future::ready(Ok("ses-id".into()))
    }
}

fn request() -> SendRequest {
    SendRequest {
        raw_mime: "From: Alice <alice@example.dev>\r\nTo: user+alias@example.dev\r\n\r\nHello"
            .into(),
        from_email_address: Some("bounce@example.dev".into()),
        to_addresses: vec![ADDRESS.into()],
        cc_addresses: vec!["allowed@example.dev".into()],
        bcc_addresses: vec!["USER+other@example.dev".into()],
    }
}

#[tokio::test]
async fn only_suppressed_envelope_recipients_are_removed_and_mime_is_unchanged() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.record(&hard_bounce("feedback")).await.expect("bounce");
    let sender = Arc::new(FakeSender::default());
    let guard = SuppressionGuard {
        inner: sender.clone(),
        store,
        control: Arc::new(Control::new(None, true).expect("control")),
    };
    let original = request();
    assert_eq!(guard.send(&original).await.expect("send"), "ses-id");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].to_addresses, Vec::<String>::new());
    assert_eq!(sent[0].cc_addresses, ["allowed@example.dev"]);
    assert_eq!(sent[0].bcc_addresses, Vec::<String>::new());
    assert_eq!(sent[0].raw_mime, original.raw_mime);
    assert_eq!(sent[0].from_email_address, original.from_email_address);
}

#[tokio::test]
async fn all_suppressed_recipients_never_reach_the_provider() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.record(&hard_bounce("feedback")).await.expect("bounce");
    let sender = Arc::new(FakeSender::default());
    let guard = SuppressionGuard {
        inner: sender.clone(),
        store,
        control: Arc::new(Control::new(None, true).expect("control")),
    };
    let mut message = request();
    message.cc_addresses.clear();
    assert_matches!(guard.send(&message).await, Err(ProviderError::Suppressed));
    assert!(sender.0.lock().expect("requests").is_empty());
}

#[tokio::test]
async fn unsuppressed_messages_keep_their_original_envelopes() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let sender = Arc::new(FakeSender::default());
    let guard = SuppressionGuard {
        inner: sender.clone(),
        store,
        control: Arc::new(Control::new(None, true).expect("control")),
    };
    let message = request();
    guard.send(&message).await.expect("send");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent[0].to_addresses, message.to_addresses);
    assert_eq!(sent[0].cc_addresses, message.cc_addresses);
    assert_eq!(sent[0].bcc_addresses, message.bcc_addresses);
}

#[tokio::test]
async fn bootstrap_drain_storage_loss_and_rate_denials_never_call_the_sender() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let sender = Arc::new(FakeSender::default());
    let control = Arc::new(
        Control::new(
            Some(&crate::config::RateLimitConfig { recipients_per_second: 0.001, burst: 3 }),
            false,
        )
        .expect("control"),
    );
    let guard =
        SuppressionGuard { inner: sender.clone(), store: store.clone(), control: control.clone() };
    assert_matches!(guard.send(&request()).await, Err(ProviderError::Transport(_)));
    control.bootstrap_done();
    guard.send(&request()).await.expect("one admitted message");
    assert_matches!(guard.send(&request()).await, Err(ProviderError::RateLimited));
    control.drain();
    assert_matches!(guard.send(&request()).await, Err(ProviderError::Transport(_)));
    assert_eq!(sender.0.lock().expect("calls").len(), 1);

    let guard = SuppressionGuard {
        inner: sender.clone(),
        store: store.clone(),
        control: Arc::new(Control::new(None, true).expect("control")),
    };
    store.pool.close().await;
    assert_matches!(guard.send(&request()).await, Err(ProviderError::Transport(_)));
    assert_eq!(sender.0.lock().expect("calls").len(), 1);
}

#[tokio::test]
async fn invalid_input_is_rejected_before_a_database_lookup() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.pool.close().await;
    let sender = Arc::new(FakeSender::default());
    let guard = SuppressionGuard {
        inner: sender.clone(),
        store,
        control: Arc::new(Control::new(None, true).expect("control")),
    };
    let mut invalid = request();
    invalid.to_addresses = vec!["invalid".into()];
    assert_matches!(guard.send(&invalid).await, Err(ProviderError::InvalidAddress(_)));
    assert!(sender.0.lock().expect("calls").is_empty());
}
