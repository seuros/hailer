use std::assert_matches;
use std::sync::Mutex;
use std::time::Duration;

use mothership_docking_protocol::{HailFrame, ReplyBodyRef, decode_ready, decode_reply};
use serde_json::Value;
use tokio::io::{DuplexStream, duplex};

use super::*;
use crate::provider::ProviderError;

const RUBY_REQUEST: &[u8] = include_bytes!("../../../test/fixtures/send_request.json");

#[derive(Clone, Copy)]
enum Outcome {
    Accepted,
    InvalidAddress,
    NoRecipients,
    Suppressed,
    Rejected,
    Unavailable,
    Ambiguous,
    RateLimited,
}

struct FakeProvider {
    outcome: Outcome,
    requests: Mutex<Vec<SendRequest>>,
}

impl FakeProvider {
    fn new(outcome: Outcome) -> Arc<Self> {
        Arc::new(Self { outcome, requests: Mutex::new(Vec::new()) })
    }
}

#[async_trait::async_trait]
impl MailProvider for FakeProvider {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        self.requests.lock().expect("request lock").push(request.clone());
        match self.outcome {
            Outcome::Accepted => Ok("ses-test-id".into()),
            Outcome::InvalidAddress => Err(ProviderError::InvalidAddress("bad-address".into())),
            Outcome::NoRecipients => Err(ProviderError::NoRecipients),
            Outcome::Suppressed => Err(ProviderError::Suppressed),
            Outcome::Rejected => Err(ProviderError::Rejected("message rejected".into())),
            Outcome::Unavailable => Err(ProviderError::Transport("circuit open".into())),
            Outcome::Ambiguous => Err(ProviderError::Ambiguous),
            Outcome::RateLimited => Err(ProviderError::RateLimited),
        }
    }
}

#[tokio::test]
async fn ruby_request_reaches_the_provider_without_rewriting_mail_or_envelopes() {
    let fake = FakeProvider::new(Outcome::Accepted);
    let provider: Arc<dyn MailProvider> = fake.clone();
    let (status, body) = handle(b"POST", SEND_PATH.as_bytes(), RUBY_REQUEST, &provider).await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("success JSON"),
        serde_json::json!({"message_id": "ses-test-id"})
    );

    let fixture: Value = serde_json::from_slice(RUBY_REQUEST).expect("Ruby fixture");
    let requests = fake.requests.lock().expect("request lock").clone();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.raw_mime, fixture["raw_mime"].as_str().expect("MIME"));
    assert_eq!(request.from_email_address.as_deref(), Some("bounce@example.com"));
    assert_eq!(request.to_addresses, ["override@example.com"]);
    assert_eq!(request.cc_addresses, ["cc@example.com"]);
    assert_eq!(request.bcc_addresses, ["bcc@example.com"]);
}

#[tokio::test]
async fn malformed_requests_never_call_the_provider() {
    let fake = FakeProvider::new(Outcome::Accepted);
    let provider: Arc<dyn MailProvider> = fake.clone();
    for body in [b"not json".as_slice(), b"{}", br#"{"raw_mime":7}"#] {
        let (status, response) = handle(b"POST", SEND_PATH.as_bytes(), body, &provider).await;
        assert_eq!(status, 400);
        let error: Value = serde_json::from_slice(&response).expect("error JSON");
        assert!(error["error"].is_string());
    }
    assert!(fake.requests.lock().expect("request lock").is_empty());
}

#[tokio::test]
async fn unrelated_routes_never_send_mail() {
    let fake = FakeProvider::new(Outcome::Accepted);
    let provider: Arc<dyn MailProvider> = fake.clone();
    for (method, path) in [(b"GET".as_slice(), SEND_PATH.as_bytes()), (b"POST", b"/other")] {
        let (status, _) = handle(method, path, RUBY_REQUEST, &provider).await;
        assert_eq!(status, 404);
    }
    assert!(fake.requests.lock().expect("request lock").is_empty());
}

#[tokio::test]
async fn provider_failures_use_the_statuses_the_gem_understands() {
    for (outcome, expected_status) in [
        (Outcome::InvalidAddress, 422),
        (Outcome::NoRecipients, 422),
        (Outcome::Suppressed, 422),
        (Outcome::Rejected, 502),
        (Outcome::Unavailable, 503),
        (Outcome::Ambiguous, 503),
        (Outcome::RateLimited, 429),
    ] {
        let provider: Arc<dyn MailProvider> = FakeProvider::new(outcome);
        let (status, body) = handle(b"POST", SEND_PATH.as_bytes(), RUBY_REQUEST, &provider).await;
        assert_eq!(status, expected_status);
        let error: Value = serde_json::from_slice(&body).expect("error JSON");
        assert_ne!(error["error"].as_str().expect("error string"), "");
        assert!(error.get("message_id").is_none());
    }
}

#[tokio::test]
async fn feedback_feed_is_paginated_without_acknowledging_or_erasing_events() {
    let database = crate::test_support::database().await;
    let store = crate::state::Store::connect(database.url(), 5).await.expect("state");
    for id in ["first", "second"] {
        store.record(&crate::feedback::test_support::hard_bounce(id)).await.expect("event");
    }
    let (status, body) = event_page(br#"{"after":0,"limit":1}"#, &store).await;
    assert_eq!(status, 200);
    let page: Value = serde_json::from_slice(&body).expect("event page");
    assert_eq!(page["next_cursor"], 1);
    assert_eq!(page["events"][0]["event"]["kind"], "bounced");
    assert_eq!(page["events"][0]["event"]["message_id"], "<rails-message@example.com>");
    let (status, body) = event_page(br#"{"after":1,"limit":1}"#, &store).await;
    assert_eq!(status, 200);
    let page: Value = serde_json::from_slice(&body).expect("event page");
    assert_eq!(page["next_cursor"], 2);
    assert_eq!(store.events(0, 100).await.expect("events").events.len(), 2);
    for body in [br#"{"limit":0}"#.as_slice(), br#"{"limit":101}"#, br#"{"after":-1}"#, b"invalid"]
    {
        assert_eq!(event_page(body, &store).await.0, 400);
    }
}

#[tokio::test]
async fn body_reader_rejects_oversized_or_inconsistent_lengths_before_delivery() {
    for (length, chunk) in [(1, b"too long".as_slice()), (9, b"short".as_slice())] {
        let (mut writer, mut reader) = duplex(1024);
        let mut bytes = Vec::new();
        push_haul(&mut bytes, 1, true, chunk).expect("HAUL");
        writer.write_all(&bytes).await.expect("body");
        assert!(read_body(&mut reader, &mut BytesMut::new(), 1, length).await.is_err());
    }
    let (_writer, mut reader) = duplex(1024);
    assert!(read_body(&mut reader, &mut BytesMut::new(), 1, u64::MAX).await.is_err());
}

#[tokio::test]
async fn a_separate_empty_fin_is_consumed_before_the_next_request() {
    let (mut writer, mut reader) = duplex(1024);
    let mut bytes = Vec::new();
    push_haul(&mut bytes, 1, false, b"{}").expect("body");
    push_haul(&mut bytes, 1, true, b"").expect("empty FIN");
    writer.write_all(&bytes).await.expect("chunks");
    let body = read_body(&mut reader, &mut BytesMut::new(), 1, 2).await.expect("complete body");
    assert_eq!(body, b"{}");
}

#[test]
fn absent_optional_envelopes_deserialize_as_empty_not_null_recipients() {
    let request: SendRequest =
        serde_json::from_str(r#"{"raw_mime":"From: alice@example.com\r\n\r\nHello"}"#)
            .expect("minimal request");
    assert!(request.from_email_address.is_none());
    assert_eq!(request.recipients().count(), 0);
}

fn request_frames(request_id: u32) -> Vec<u8> {
    let mut hail = HailFrame::default();
    hail.param(b"REQUEST_METHOD", b"POST");
    hail.param(b"PATH_INFO", SEND_PATH.as_bytes());
    let mut frames = hail
        .seal(request_id, RUBY_REQUEST.len().try_into().expect("request length"))
        .expect("HAIL")
        .to_vec();
    let chunks: Vec<_> = RUBY_REQUEST.chunks(31).collect();
    for (index, chunk) in chunks.iter().enumerate() {
        push_haul(&mut frames, request_id, index + 1 == chunks.len(), chunk).expect("HAUL");
    }
    frames
}

async fn accepted_reply(stream: &mut DuplexStream, pending: &mut BytesMut, request_id: u32) {
    let (kind, payload) = next_frame(stream, pending).await.expect("REPLY frame");
    assert_eq!(kind, MessageType::Reply);
    let reply = decode_reply(&payload).expect("REPLY");
    assert_eq!(reply.request_id, request_id);
    assert_eq!(reply.status, 200);
    assert_matches!(reply.body, ReplyBodyRef::Stream);
    assert!(
        reply
            .headers
            .iter()
            .any(|(name, value)| { *name == b"content-type" && *value == b"application/json" })
    );

    let (kind, payload) = next_frame(stream, pending).await.expect("response HAUL frame");
    assert_eq!(kind, MessageType::Haul);
    let body = decode_haul(&payload).expect("response HAUL");
    assert_eq!(body.request_id, request_id);
    assert!(body.fin);
    let response: Value = serde_json::from_slice(body.data).expect("response JSON");
    assert_eq!(response["message_id"], "ses-test-id");

    let (kind, payload) = next_frame(stream, pending).await.expect("READY frame");
    assert_eq!(kind, MessageType::Ready);
    assert_eq!(decode_ready(&payload).expect("READY"), request_id);
}

#[tokio::test]
async fn chunked_docking_requests_return_acceptance_and_reuse_the_link() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let fake = FakeProvider::new(Outcome::Accepted);
        let provider: Arc<dyn MailProvider> = fake.clone();
        let (mut mothership, ship) = duplex(16_384);
        // Exercise bytes left over by the docking handshake as well as
        // subsequent reads from the same link.
        let pending = BytesMut::from(request_frames(1).as_slice());
        let database = crate::test_support::database().await;
        let store = crate::state::Store::connect(database.url(), 5).await.expect("state");
        let task = tokio::spawn(serve_link(ship, pending, provider, store));
        let mut replies = BytesMut::new();
        accepted_reply(&mut mothership, &mut replies, 1).await;
        mothership.write_all(&request_frames(2)).await.expect("second request");
        accepted_reply(&mut mothership, &mut replies, 2).await;
        assert_eq!(fake.requests.lock().expect("request lock").len(), 2);
        drop(mothership);
        task.await.expect("link task").expect_err("closed link");
    })
    .await
    .expect("exchange deadline");
}

#[tokio::test]
async fn mismatched_body_request_id_closes_the_link_without_sending() {
    let fake = FakeProvider::new(Outcome::Accepted);
    let provider: Arc<dyn MailProvider> = fake.clone();
    let (mut mothership, ship) = duplex(4096);
    let mut hail = HailFrame::default();
    hail.param(b"REQUEST_METHOD", b"POST");
    hail.param(b"PATH_INFO", SEND_PATH.as_bytes());
    let mut frames = hail.seal(1, 2).expect("HAIL").to_vec();
    push_haul(&mut frames, 99, true, b"{}").expect("mismatched HAUL");
    mothership.write_all(&frames).await.expect("request");
    let database = crate::test_support::database().await;
    let store = crate::state::Store::connect(database.url(), 5).await.expect("state");
    let error =
        serve_link(ship, BytesMut::new(), provider, store).await.expect_err("wrong request id");
    assert!(error.to_string().contains("does not match"));
    assert!(fake.requests.lock().expect("request lock").is_empty());
}
