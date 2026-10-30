use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::provider::test_support::{FakeSender, request_to};
use crate::verify::VerifyError;

/// `verdicts` maps an address to a canned [`Verdict`]; anything missing
/// returns a transport error, so a test can also exercise fail-open.
struct FakeVerifier {
    verdicts: HashMap<String, Verdict>,
    calls: AtomicUsize,
}

impl FakeVerifier {
    fn new(verdicts: &[(&str, State)]) -> Self {
        let verdicts = verdicts
            .iter()
            .map(|(address, state)| ((*address).to_owned(), verdict(*state)))
            .collect();
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
    let verifier = FakeVerifier::new(&[("bob@example.com", State::Deliverable)]);
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let original = request_to(&["bob@example.com"]);
    assert_eq!(guard.send(&original).await.expect("send"), "ses-id");
    let sent = sender.sent();
    assert_eq!(sent[0].to_addresses, ["bob@example.com"]);
    assert_eq!(sent[0].raw_mime, original.raw_mime);
}

#[tokio::test]
async fn undeliverable_recipients_are_dropped_and_mime_is_unchanged() {
    let verifier = FakeVerifier::new(&[
        ("good@example.com", State::Deliverable),
        ("bad@example.com", State::Undeliverable),
    ]);
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let original = request_to(&["good@example.com", "bad@example.com"]);
    guard.send(&original).await.expect("send");
    let sent = sender.sent();
    assert_eq!(sent[0].to_addresses, ["good@example.com"]);
    assert_eq!(sent[0].raw_mime, original.raw_mime);
}

#[tokio::test]
async fn all_undeliverable_recipients_never_reach_the_provider() {
    let verifier = FakeVerifier::new(&[("bad@example.com", State::Undeliverable)]);
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    let error = guard.send(&request_to(&["bad@example.com"])).await.expect_err("should reject");
    assert!(matches!(error, ProviderError::Suppressed));
    assert!(sender.sent().is_empty());
}

#[tokio::test]
async fn a_verifier_call_failure_fails_open() {
    // No canned verdict for this address - FakeVerifier returns a transport
    // error, which the guard must treat as Unknown and allow through.
    let (guard, sender, _database) =
        guard(FakeVerifier::new(&[]), vec![State::Undeliverable]).await;
    let sent_ok = guard.send(&request_to(&["anyone@example.com"])).await;
    assert!(sent_ok.is_ok());
    assert_eq!(sender.sent().len(), 1);
}

#[tokio::test]
async fn risky_is_allowed_through_when_not_in_reject_states() {
    let verifier = FakeVerifier::new(&[("maybe@example.com", State::Risky)]);
    // Default-shaped policy: only Undeliverable is rejected, Risky passes.
    let (guard, sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    guard.send(&request_to(&["maybe@example.com"])).await.expect("send");
    assert_eq!(sender.sent()[0].to_addresses, ["maybe@example.com"]);
}

#[tokio::test]
async fn a_cached_verdict_skips_a_second_verifier_call() {
    let verifier = FakeVerifier::new(&[("bob@example.com", State::Deliverable)]);
    let (guard, _sender, _database) = guard(verifier, vec![State::Undeliverable]).await;
    guard.send(&request_to(&["bob@example.com"])).await.expect("first send");
    guard.send(&request_to(&["bob@example.com"])).await.expect("second send");
    assert_eq!(guard.verifier.calls.load(Ordering::SeqCst), 1);
}
