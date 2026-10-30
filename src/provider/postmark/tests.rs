use std::assert_matches;

use super::*;
use crate::provider::test_support::{Endpoint, correlation, endpoint, request};

const ACCEPTED: &str = r#"{"To":"bob@example.dev","SubmittedAt":"2026-10-29T10:00:00Z","MessageID":"0a129aee-e1cd-480d-b08d-4f48548ff48d","ErrorCode":0,"Message":"OK"}"#;

fn provider(api: String) -> PostmarkProvider {
    PostmarkProvider::with_api("test-token".into(), &PostmarkConfig::default(), api)
}

async fn fixture(
    status: u16,
    body: &'static str,
    disconnect: bool,
) -> (PostmarkProvider, Endpoint) {
    let server = endpoint(status, body, disconnect).await;
    (provider(server.url.clone()), server)
}

#[tokio::test]
async fn an_accepted_send_returns_postmarks_message_id() {
    let (provider, server) = fixture(200, ACCEPTED, false).await;
    assert_eq!(
        provider.send(&request()).await.expect("accepted"),
        "0a129aee-e1cd-480d-b08d-4f48548ff48d"
    );
    assert_eq!(server.calls(), 1);
    let (headers, body) = {
        let recorded = server.requests();
        (recorded[0].headers.to_ascii_lowercase(), recorded[0].body.clone())
    };
    assert!(headers.starts_with("post /email "), "{headers}");
    assert!(headers.contains("x-postmark-server-token: test-token"), "{headers}");
    assert!(headers.contains("accept: application/json"), "{headers}");
    assert!(
        headers.contains(concat!("user-agent: hailer/", env!("CARGO_PKG_VERSION"))),
        "{headers}"
    );
    assert_eq!(body["MessageStream"], "outbound");
    assert_eq!(body["To"], "bob@example.dev");
    assert!(body.get("Metadata").is_none());
    server.stop().await;
}

#[tokio::test]
async fn a_queued_dispatch_carries_its_attempt_as_metadata() {
    let (provider, server) = fixture(200, ACCEPTED, false).await;
    assert_eq!(
        provider.dispatch(&request(), &correlation()).await,
        DispatchOutcome::Accepted("0a129aee-e1cd-480d-b08d-4f48548ff48d".into())
    );
    assert_eq!(
        server.requests()[0].body["Metadata"],
        serde_json::json!({
            "hailer_message_id": correlation().hailer_message_id,
            "hailer_attempt_id": correlation().attempt_id,
        })
    );
    server.stop().await;
}

#[tokio::test]
async fn postmarks_answers_are_read_from_the_wire() {
    for (status, body, disconnect, answer) in [
        (200, ACCEPTED, false, Answer::Accepted("0a129aee-e1cd-480d-b08d-4f48548ff48d".into())),
        (
            200,
            r#"{"MessageID":"x","ErrorCode":0,"Message":"Message OK, but will not deliver to these inactive addresses: a@example.dev"}"#,
            false,
            Answer::Accepted("x".into()),
        ),
        (200, r#"{"MessageID":"x","ErrorCode":300}"#, false, Answer::Unknown),
        (200, r#"{"MessageID":" ","ErrorCode":0}"#, false, Answer::Unknown),
        (200, "", false, Answer::Unknown),
        (200, "", true, Answer::Unknown),
        (
            422,
            r#"{"ErrorCode":406,"Message":"Found inactive addresses: bob@example.dev"}"#,
            false,
            Answer::Refused { status: 422, code: Some(406) },
        ),
        (500, "oops", false, Answer::Refused { status: 500, code: None }),
    ] {
        let (provider, server) = fixture(status, body, disconnect).await;
        assert_eq!(provider.post(b"{}".to_vec()).await, answer, "{status} {body:?} {disconnect}");
        server.stop().await;
    }

    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let url = format!("http://{}", closed.local_addr().expect("address"));
    drop(closed);
    assert_eq!(provider(url).post(b"{}".to_vec()).await, Answer::Unsent);
}

#[test]
fn answers_map_to_send_and_dispatch_outcomes() {
    let refused = |status, code| Answer::Refused { status, code };
    let rejected = DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false };
    let cases = [
        (Answer::Unsent, "transport", DispatchOutcome::NotDispatched("request_not_dispatched")),
        (Answer::Unknown, "ambiguous", DispatchOutcome::Unknown),
        (refused(422, Some(406)), "suppressed", rejected.clone()),
        (
            refused(401, Some(10)),
            "transport",
            DispatchOutcome::NotAccepted { code: "provider_unauthorized", retryable: true },
        ),
        (
            refused(429, None),
            "transport",
            DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true },
        ),
        (refused(500, Some(101)), "transport", DispatchOutcome::Unknown),
        (refused(503, Some(100)), "transport", DispatchOutcome::Unknown),
        (refused(413, None), "invalid", rejected.clone()),
        (refused(422, Some(300)), "rejected", rejected.clone()),
        (refused(404, None), "rejected", rejected),
    ];
    for (answer, send, dispatch) in cases {
        let label = format!("{answer:?}");
        let error = match send_result(answer.clone()) {
            Err(error) => error,
            Ok(id) => panic!("{label} accepted as {id}"),
        };
        let kind = match error {
            ProviderError::Transport(_) => "transport",
            ProviderError::Ambiguous => "ambiguous",
            ProviderError::Suppressed => "suppressed",
            ProviderError::InvalidRequest => "invalid",
            ProviderError::Rejected(ref message) => {
                assert!(!message.contains('@'), "{label}: {message}");
                "rejected"
            }
            ref other => panic!("{label}: {other}"),
        };
        assert_eq!(kind, send, "{label}");
        let outcome = match dispatch_result(answer) {
            Ok(id) => DispatchOutcome::Accepted(id),
            Err(failure) => failure.outcome(),
        };
        assert_eq!(outcome, dispatch, "{label}");
    }
}

#[tokio::test]
async fn invalid_messages_never_reach_postmark() {
    let (provider, server) = fixture(200, ACCEPTED, false).await;
    let mut invalid_address = request();
    invalid_address.to_addresses = vec!["invalid".into()];
    let mut no_body = request();
    no_body.raw_mime =
        "From: alice@example.dev\r\nTo: bob@example.dev\r\nContent-Type: image/png\r\n\r\nx".into();
    for invalid in [invalid_address, no_body] {
        assert!(provider.send(&invalid).await.is_err());
        assert_eq!(
            provider.dispatch(&invalid, &correlation()).await,
            DispatchOutcome::NotAccepted { code: "invalid_message", retryable: false }
        );
    }
    let bad_correlation = Correlation { attempt_id: "not-a-uuid".into(), ..correlation() };
    assert_eq!(
        provider.dispatch(&request(), &bad_correlation).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );
    assert_eq!(server.calls(), 0);
    server.stop().await;
}

#[tokio::test]
async fn rejections_leave_the_circuit_closed_and_outages_open_it() {
    let (provider, server) = fixture(
        422,
        r#"{"ErrorCode":300,"Message":"Invalid 'To' address: bob@example.dev"}"#,
        false,
    )
    .await;
    for _ in 0..8 {
        assert_matches!(provider.send(&request()).await, Err(ProviderError::Rejected(_)));
    }
    assert_eq!(server.calls(), 8);
    assert_eq!(provider.circuit_state(), "closed");
    server.stop().await;

    let (provider, server) = fixture(503, r#"{"ErrorCode":100}"#, false).await;
    for _ in 0..8 {
        assert!(provider.send(&request()).await.is_err());
    }
    assert_eq!(server.calls(), 5);
    assert_eq!(provider.circuit_state(), "open");
    server.stop().await;
}

#[tokio::test]
async fn the_server_check_never_carries_the_api_tokens() {
    let (provider, server) = fixture(
        200,
        r#"{"ID":42,"Name":"hailer","ApiTokens":["secret-token"],"DeliveryType":"Live","Color":"blue"}"#,
        false,
    )
    .await;
    let found = provider.server().await.expect("server");
    assert_eq!((found.id, found.name.as_str()), (42, "hailer"));
    let printed = serde_json::to_string(&found).expect("JSON");
    assert!(!printed.contains("secret-token"), "{printed}");
    server.stop().await;
}

const BLACK_HOLE: &str = "test@blackhole.postmarkapp.com";
const HARD_BOUNCE: &str = "hardbounce@bounce-testing.postmarkapp.com";

fn live() -> (String, String) {
    crate::install_crypto_provider().expect("TLS crypto provider");
    (
        std::env::var("POSTMARK_SERVER_TOKEN").expect("POSTMARK_SERVER_TOKEN"),
        std::env::var("HAILER_POSTMARK_FROM").expect("HAILER_POSTMARK_FROM"),
    )
}

fn envelope(raw_mime: String, to: &[&str], bcc: &[&str]) -> SendRequest {
    SendRequest {
        raw_mime,
        from_email_address: None,
        to_addresses: to.iter().map(|address| (*address).to_owned()).collect(),
        cc_addresses: vec![],
        bcc_addresses: bcc.iter().map(|address| (*address).to_owned()).collect(),
    }
}

fn plain(from: &str, to: &[&str], subject: &str) -> SendRequest {
    let raw = format!(
        "From: {from}\r\nTo: {}\r\nSubject: {subject}\r\n\r\nhailer live test\r\n",
        to.join(", ")
    );
    envelope(raw, to, &[])
}

/// What Rails renders for a mailer with an inline image and an attachment.
fn rails_shaped(from: &str, to: &str, stamp: i64) -> String {
    use base64::Engine;
    let subject =
        base64::engine::general_purpose::STANDARD.encode("Hailer → Postmark: rich message ✓");
    let domain = from.rsplit_once('@').map_or("hailer.invalid", |(_, domain)| domain);
    [
        format!("From: \"Hailer\" <{from}>"),
        format!("To: \"Testing\" <{to}>"),
        format!("Reply-To: \"Hailer Support\" <{from}>"),
        format!("Subject: =?UTF-8?B?{subject}?="),
        format!("Message-ID: <hailer-postmark-{stamp}@{domain}>"),
        format!("List-Unsubscribe: <mailto:{from}?subject=unsubscribe>"),
        format!("X-Hailer-Test: {stamp}"),
        "MIME-Version: 1.0".into(),
        "Content-Type: multipart/mixed; boundary=\"mixed\"".into(),
        String::new(),
        "--mixed".into(),
        "Content-Type: multipart/related; boundary=\"related\"".into(),
        String::new(),
        "--related".into(),
        "Content-Type: multipart/alternative; boundary=\"alt\"".into(),
        String::new(),
        "--alt".into(),
        "Content-Type: text/plain; charset=UTF-8".into(),
        "Content-Transfer-Encoding: quoted-printable".into(),
        String::new(),
        "Hello from hailer =E2=86=92 Postmark.".into(),
        "Parsed MIME, an inline image by cid, and a CSV attachment.".into(),
        "--alt".into(),
        "Content-Type: text/html; charset=UTF-8".into(),
        String::new(),
        "<p>Hello from <strong>hailer</strong> &rarr; Postmark.</p>".into(),
        "<p><img src=\"cid:dot@hailer\" alt=\"red dot\" width=\"20\" height=\"20\"> inline image by cid</p>".into(),
        "--alt--".into(),
        "--related".into(),
        "Content-Type: image/png".into(),
        "Content-ID: <dot@hailer>".into(),
        "Content-Disposition: inline; filename=\"dot.png\"".into(),
        "Content-Transfer-Encoding: base64".into(),
        String::new(),
        "iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==".into(),
        "--related--".into(),
        "--mixed".into(),
        "Content-Type: text/csv; name=\"report.csv\"".into(),
        "Content-Disposition: attachment; filename=\"report.csv\"".into(),
        String::new(),
        "provider,status".into(),
        "postmark,accepted".into(),
        "--mixed--".into(),
        String::new(),
    ]
    .join("\r\n")
}

/// Delivers a Rails-shaped message to `HAILER_POSTMARK_TO` (else Postmark's
/// black hole), then dispatches a queued copy to the black hole.
/// `HAILER_POSTMARK_FROM` must be on a verified sender domain.
#[tokio::test]
#[ignore = "needs POSTMARK_SERVER_TOKEN and HAILER_POSTMARK_FROM"]
async fn live_postmark_delivers_a_rails_shaped_message() {
    let (token, from) = live();
    let to = std::env::var("HAILER_POSTMARK_TO").unwrap_or_else(|_| BLACK_HOLE.to_owned());
    let bcc: &[&str] = if to == BLACK_HOLE { &[] } else { &[BLACK_HOLE] };
    let stamp = jiff::Timestamp::now().as_millisecond();
    let provider = PostmarkProvider::new(token, &PostmarkConfig::default());

    let server = provider.server().await.expect("token names a server");
    eprintln!("server: {} ({})", server.name, server.id);
    let request = envelope(rails_shaped(&from, &to, stamp), &[&to], bcc);
    let id = provider.send(&request).await.expect("sync send accepted");
    eprintln!("sync send to {to}: {id}");
    let queued = envelope(rails_shaped(&from, BLACK_HOLE, stamp), &[BLACK_HOLE], &[]);
    let outcome = provider.dispatch(&queued, &correlation()).await;
    eprintln!("queued dispatch: {outcome:?}");
    assert_matches!(outcome, DispatchOutcome::Accepted(_));
}

/// Postmark's refusals, live: a wrong token, a missing stream, an
/// unverified sender, and an inactive recipient. With an active recipient
/// beside it, Postmark accepts and skips the inactive one.
#[tokio::test]
#[ignore = "needs POSTMARK_SERVER_TOKEN and HAILER_POSTMARK_FROM"]
async fn live_postmark_refusals_map_to_outcomes() {
    let (token, from) = live();
    let provider = PostmarkProvider::new(token.clone(), &PostmarkConfig::default());
    let message = plain(&from, &[BLACK_HOLE], "hailer refusal check");

    let wrong = PostmarkProvider::new(
        "00000000-0000-0000-0000-000000000000".into(),
        &PostmarkConfig::default(),
    );
    assert_matches!(wrong.send(&message).await, Err(ProviderError::Transport(_)));
    assert_eq!(
        wrong.dispatch(&message, &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_unauthorized", retryable: true }
    );

    let missing_stream = PostmarkProvider::new(
        token,
        &PostmarkConfig { message_stream: "hailer-no-such-stream".into() },
    );
    let error = missing_stream.send(&message).await.expect_err("unknown stream");
    eprintln!("missing stream: {error}");
    assert_matches!(error, ProviderError::Rejected(_));

    let stranger = plain("nobody@hailer.invalid", &[BLACK_HOLE], "hailer refusal check");
    let error = provider.send(&stranger).await.expect_err("unverified sender");
    eprintln!("unverified sender: {error}");
    assert_matches!(error, ProviderError::Rejected(_));

    // Every message to this address hard-bounces; once Postmark records the
    // bounce, it refuses the address as inactive.
    let inactive = plain(&from, &[HARD_BOUNCE], "hailer inactive check");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        match provider.send(&inactive).await {
            Err(ProviderError::Suppressed) => break,
            Ok(id) => eprintln!("accepted {id}; bounce not recorded yet"),
            Err(other) => panic!("inactive recipient: {other}"),
        }
        assert!(tokio::time::Instant::now() < deadline, "the bounce was never recorded");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    assert_eq!(
        provider.dispatch(&inactive, &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
    );

    let mixed = plain(&from, &[HARD_BOUNCE, BLACK_HOLE], "hailer partial check");
    let id = provider.send(&mixed).await.expect("accepted for the active recipient");
    eprintln!("mixed recipients accepted: {id}");
}
