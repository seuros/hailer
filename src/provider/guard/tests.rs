use std::assert_matches;
use std::sync::Mutex;

use super::*;
use crate::feedback::test_support::{ADDRESS, hard_bounce};

#[derive(Default)]
struct FakeSender(Mutex<Vec<SendRequest>>);

#[async_trait::async_trait]
impl MailProvider for FakeSender {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        self.0.lock().expect("requests").push(request.clone());
        Ok("ses-id".into())
    }
}

fn request() -> SendRequest {
    SendRequest {
        raw_mime: "From: Alice <alice@example.com>\r\nTo: user+alias@example.com\r\n\r\nHello"
            .into(),
        from_email_address: Some("bounce@example.com".into()),
        to_addresses: vec![ADDRESS.into()],
        cc_addresses: vec!["allowed@example.com".into()],
        bcc_addresses: vec!["USER+other@example.com".into()],
    }
}

#[tokio::test]
async fn only_suppressed_envelope_recipients_are_removed_and_mime_is_unchanged() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.record(&hard_bounce("feedback")).await.expect("bounce");
    let sender = Arc::new(FakeSender::default());
    let guard = SuppressionGuard { inner: sender.clone(), store };
    let original = request();
    assert_eq!(guard.send(&original).await.expect("send"), "ses-id");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].to_addresses, Vec::<String>::new());
    assert_eq!(sent[0].cc_addresses, ["allowed@example.com"]);
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
    let guard = SuppressionGuard { inner: sender.clone(), store };
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
    let guard = SuppressionGuard { inner: sender.clone(), store };
    let message = request();
    guard.send(&message).await.expect("send");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent[0].to_addresses, message.to_addresses);
    assert_eq!(sent[0].cc_addresses, message.cc_addresses);
    assert_eq!(sent[0].bcc_addresses, message.bcc_addresses);
}
