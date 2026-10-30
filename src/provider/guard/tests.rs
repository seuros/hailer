use std::assert_matches;

use super::*;
use crate::feedback::test_support::{ADDRESS, hard_bounce};
use crate::provider::test_support::FakeSender;

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

async fn open_control(store: &Store) -> Arc<Control> {
    Arc::new(Control::new(store.pool.clone(), None, true).await.expect("control"))
}

#[tokio::test]
async fn only_suppressed_envelope_recipients_are_removed_and_mime_is_unchanged() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.record(&hard_bounce("feedback")).await.expect("bounce");
    let sender = Arc::new(FakeSender::default());
    let control = open_control(&store).await;
    let guard = SuppressionGuard { inner: sender.clone(), store, control };
    let original = request();
    assert_eq!(guard.send(&original).await.expect("send"), "ses-id");
    let sent = sender.sent();
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
    let control = open_control(&store).await;
    let guard = SuppressionGuard { inner: sender.clone(), store, control };
    let mut message = request();
    message.cc_addresses.clear();
    assert_matches!(guard.send(&message).await, Err(ProviderError::Suppressed));
    assert!(sender.sent().is_empty());
}

#[tokio::test]
async fn unsuppressed_messages_keep_their_original_envelopes() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let sender = Arc::new(FakeSender::default());
    let control = open_control(&store).await;
    let guard = SuppressionGuard { inner: sender.clone(), store, control };
    let message = request();
    guard.send(&message).await.expect("send");
    let sent = sender.sent();
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
            store.pool.clone(),
            Some(&crate::config::RateLimitConfig {
                recipients_per_second: 0.001,
                burst: 3,
                discover_from_aws: false,
                discovery_interval_seconds: 900,
            }),
            false,
        )
        .await
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
    assert_eq!(sender.sent().len(), 1);

    let guard = SuppressionGuard {
        inner: sender.clone(),
        store: store.clone(),
        control: open_control(&store).await,
    };
    store.pool.close().await;
    assert_matches!(guard.send(&request()).await, Err(ProviderError::Transport(_)));
    assert_eq!(sender.sent().len(), 1);
}

#[tokio::test]
async fn invalid_input_is_rejected_before_a_database_lookup() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.pool.close().await;
    let sender = Arc::new(FakeSender::default());
    let control = open_control(&store).await;
    let guard = SuppressionGuard { inner: sender.clone(), store, control };
    let mut invalid = request();
    invalid.to_addresses = vec!["invalid".into()];
    assert_matches!(guard.send(&invalid).await, Err(ProviderError::InvalidAddress(_)));
    assert!(sender.sent().is_empty());
}
