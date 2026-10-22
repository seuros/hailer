use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::emailable::EmailableClient;
use super::*;

/// Accepts exactly one connection, ignores the request, writes `body` with
/// `status` back. Good enough for Emailable's GET-with-no-request-body
/// contract; nothing here needs to inspect what was sent.
async fn fixture(status: u16, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let addr = listener.local_addr().expect("fixture address");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("fixture accept");
        let mut buffer = [0_u8; 4096];
        let _ = socket.read(&mut buffer).await;
        let response = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn disabled_verifier_always_fails_open() {
    let verdict = Verifier::Disabled.verify("alice@example.com").await.expect("disabled verify");
    assert_eq!(verdict.state, State::Unknown);
}

#[tokio::test]
async fn deliverable_response_is_parsed() {
    let url = fixture(
        200,
        r#"{"email":"alice@example.com","state":"deliverable","reason":"accepted_email","disposable":false,"role":false}"#,
    )
    .await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let verdict = client.verify("alice@example.com").await.expect("verify");
    assert_eq!(verdict.state, State::Deliverable);
    assert_eq!(verdict.reason, "accepted_email");
    assert!(!verdict.disposable);
    assert!(!verdict.role);
}

#[tokio::test]
async fn undeliverable_disposable_role_flags_survive() {
    let url = fixture(
        200,
        r#"{"email":"temp@mailinator.com","state":"undeliverable","reason":"invalid_mailbox","disposable":true,"role":true}"#,
    )
    .await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let verdict = client.verify("temp@mailinator.com").await.expect("verify");
    assert_eq!(verdict.state, State::Undeliverable);
    assert!(verdict.disposable);
    assert!(verdict.role);
}

#[tokio::test]
async fn unrecognized_state_string_maps_to_unknown() {
    let url =
        fixture(200, r#"{"email":"a@example.com","state":"something_new","reason":"n/a"}"#).await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let verdict = client.verify("a@example.com").await.expect("verify");
    assert_eq!(verdict.state, State::Unknown);
}

#[tokio::test]
async fn non_success_status_is_a_transport_error() {
    let url = fixture(500, "internal error").await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let error = client.verify("a@example.com").await.expect_err("should fail");
    assert!(matches!(error, VerifyError::Transport(_)));
}

#[tokio::test]
async fn server_side_timeout_status_is_a_transport_error() {
    let url = fixture(249, r#"{"message":"try again"}"#).await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let error = client.verify("a@example.com").await.expect_err("should fail");
    assert!(matches!(error, VerifyError::Transport(_)));
}

#[tokio::test]
async fn malformed_json_body_is_an_invalid_response_error() {
    let url = fixture(200, "not json").await;
    let client = EmailableClient::with_base_url("key".into(), 5, url);
    let error = client.verify("a@example.com").await.expect_err("should fail");
    assert!(matches!(error, VerifyError::InvalidResponse(_)));
}
