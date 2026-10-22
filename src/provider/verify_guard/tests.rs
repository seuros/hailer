use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::verify::VerifyError;

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

/// `verdicts` maps an address to a canned [`Verdict`]; anything missing
/// returns a transport error, so a test can also exercise fail-open.
struct FakeVerifier {
    verdicts: HashMap<String, Verdict>,
    calls: AtomicUsize,
}

impl FakeVerifier {
    fn new(verdicts: HashMap<String, Verdict>) -> Self {
        Self { verdicts, calls: AtomicUsize::new(0) }
    }
}

impl AddressVerifier for FakeVerifier {
    fn verify(
        &self,
        address: &str,
    ) -> impl std::future::Future<Output = Result<Verdict, VerifyError>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(
            self.verdicts
                .get(address)
                .cloned()
                .ok_or_else(|| VerifyError::Transport("no canned verdict".into())),
        )
    }
}

fn verdict(state: State) -> Verdict {
    Verdict { state, reason: "test".into(), disposable: false, role: false }
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

/// The returned [`crate::test_support::Database`] must stay bound for as
/// long as the guard is used - its `Drop` force-drops the real PostgreSQL
/// database, which previously happened the instant this function returned
/// (the bug behind an earlier flaky "verifier called twice" failure here).
async fn guard(
    verifier: FakeVerifier,
    reject_states: Vec<State>,
) -> (
    EmailVerificationGuard<FakeSender, FakeVerifier>,
    Arc<FakeSender>,
    crate::test_support::Database,
) {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let sender = Arc::new(FakeSender::default());
    let guard = EmailVerificationGuard {
        inner: sender.clone(),
        verifier: Arc::new(verifier),
        store,
        reject_states,
        cache_ttl: Duration::from_secs(3600),
    };
    (guard, sender, database)
}

#[tokio::test]
async fn deliverable_recipients_pass_through_unfiltered() {
    let verifier = FakeVerifier::new(HashMap::from([(
        "bob@example.com".to_string(),
        verdict(State::Deliverable),
    )]));
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let original = request(&["bob@example.com"]);
    assert_eq!(guard.send(&original).await.expect("send"), "ses-id");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent[0].to_addresses, ["bob@example.com"]);
    assert_eq!(sent[0].raw_mime, original.raw_mime);
}

#[tokio::test]
async fn undeliverable_recipients_are_dropped_and_mime_is_unchanged() {
    let verifier = FakeVerifier::new(HashMap::from([
        ("good@example.com".to_string(), verdict(State::Deliverable)),
        ("bad@example.com".to_string(), verdict(State::Undeliverable)),
    ]));
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let original = request(&["good@example.com", "bad@example.com"]);
    guard.send(&original).await.expect("send");
    let sent = sender.0.lock().expect("requests").clone();
    assert_eq!(sent[0].to_addresses, ["good@example.com"]);
    assert_eq!(sent[0].raw_mime, original.raw_mime);
}

#[tokio::test]
async fn all_undeliverable_recipients_never_reach_the_provider() {
    let verifier = FakeVerifier::new(HashMap::from([(
        "bad@example.com".to_string(),
        verdict(State::Undeliverable),
    )]));
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let error = guard.send(&request(&["bad@example.com"])).await.expect_err("should reject");
    assert!(matches!(error, ProviderError::Suppressed));
    assert!(sender.0.lock().expect("requests").is_empty());
}

#[tokio::test]
async fn a_verifier_call_failure_fails_open() {
    // No canned verdict for this address - FakeVerifier returns a transport
    // error, which the guard must treat as Unknown and allow through.
    let (guard, sender, _database) =
        guard(FakeVerifier::new(HashMap::new()), vec![State::Undeliverable]).await;
    let sent_ok = guard.send(&request(&["anyone@example.com"])).await;
    assert!(sent_ok.is_ok());
    assert_eq!(sender.0.lock().expect("requests").len(), 1);
}

#[tokio::test]
async fn risky_is_allowed_through_when_not_in_reject_states() {
    let verifier = FakeVerifier::new(HashMap::from([(
        "maybe@example.com".to_string(),
        verdict(State::Risky),
    )]));
    // Default-shaped policy: only Undeliverable is rejected, Risky passes.
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    guard.send(&request(&["maybe@example.com"])).await.expect("send");
    assert_eq!(sender.0.lock().expect("requests")[0].to_addresses, ["maybe@example.com"]);
}

#[tokio::test]
async fn a_cached_verdict_skips_a_second_verifier_call() {
    let verifier = FakeVerifier::new(HashMap::from([(
        "bob@example.com".to_string(),
        verdict(State::Deliverable),
    )]));
    let (guard, _sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    guard.send(&request(&["bob@example.com"])).await.expect("first send");
    guard.send(&request(&["bob@example.com"])).await.expect("second send");
    assert_eq!(guard.verifier.calls.load(Ordering::SeqCst), 1);
}
