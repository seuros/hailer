use std::sync::Mutex;

use super::*;

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

fn request(to: &[&str]) -> SendRequest {
    SendRequest {
        raw_mime: "From: Alice <alice@example.com>\r\nTo: bob@example.com\r\n\r\nHello".into(),
        from_email_address: None,
        to_addresses: to.iter().map(ToString::to_string).collect(),
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

#[tokio::test]
async fn an_operator_banned_domain_is_dropped_and_mime_is_unchanged() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.ban_domain("guerrillamail.com", Some("disposable")).await.expect("ban");
    let sender = Arc::new(FakeSender::default());
    let guard = DomainBlockGuard { inner: sender.clone(), store };
    let original = request(&["good@example.com", "spammer@guerrillamail.com"]);
    guard.send(&original).await.expect("send");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent[0].to_addresses, ["good@example.com"]);
    assert_eq!(sent[0].raw_mime, original.raw_mime);
}

#[tokio::test]
async fn a_suffix_glob_blocks_the_whole_tld() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.ban_domain("*.kp", Some("sanctioned")).await.expect("ban");
    let sender = Arc::new(FakeSender::default());
    let guard = DomainBlockGuard { inner: sender.clone(), store };
    let error =
        guard.send(&request(&["someone@mail.example.kp"])).await.expect_err("should reject");
    assert!(matches!(error, ProviderError::Suppressed));
    assert!(sender.0.lock().expect("requests").is_empty());
}

#[tokio::test]
async fn unbanning_an_operator_pattern_lets_mail_through_again() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    store.ban_domain("example.com", None).await.expect("ban");
    assert!(store.unban_domain("example.com").await.expect("unban"));
    let sender = Arc::new(FakeSender::default());
    let guard = DomainBlockGuard { inner: sender.clone(), store };
    guard.send(&request(&["alice@example.com"])).await.expect("send");
    assert_eq!(sender.0.lock().expect("requests")[0].to_addresses, ["alice@example.com"]);
}

#[tokio::test]
async fn unbanning_a_curated_pattern_is_a_no_op() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    // Seeded by the 05_domain_blocks migration; only the gem upgrade path
    // changes the curated list, never the CLI.
    assert!(!store.unban_domain("guerrillamail.com").await.expect("unban attempt"));
    let sender = Arc::new(FakeSender::default());
    let guard = DomainBlockGuard { inner: sender.clone(), store };
    let error =
        guard.send(&request(&["spammer@guerrillamail.com"])).await.expect_err("still blocked");
    assert!(matches!(error, ProviderError::Suppressed));
}

#[tokio::test]
async fn an_unlisted_domain_passes_through() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let sender = Arc::new(FakeSender::default());
    let guard = DomainBlockGuard { inner: sender.clone(), store };
    guard.send(&request(&["alice@example.com"])).await.expect("send");
    assert_eq!(sender.0.lock().expect("requests").len(), 1);
}
