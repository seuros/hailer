use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::emailable::{BatchStatus, EmailableClient};
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

/// Serves `submit_body` for POST, `status_body` for GET - both batch
/// operations hit the same URL, distinguished only by HTTP method, mirroring
/// the real `/v1/batch` endpoint.
async fn batch_fixture(submit_body: &'static str, status_body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let addr = listener.local_addr().expect("fixture address");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { break };
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            if read == 0 {
                break;
            }
            let request = String::from_utf8_lossy(&buffer[..read]);
            let body = if request.starts_with("POST") { submit_body } else { status_body };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    format!("http://{addr}")
}

fn batch_client(batch_url: String, account_url: String) -> EmailableClient {
    EmailableClient::with_urls(
        "key".into(),
        5,
        "http://unused.invalid".into(),
        batch_url,
        account_url,
    )
}

/// Serves a canned account response with `available_credits` for any
/// request - good enough for exercising the pre-submit credit check.
async fn account_fixture(available_credits: u64) -> String {
    let body =
        format!(r#"{{"owner_email":"ops@example.dev","available_credits":{available_credits}}}"#);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let addr = listener.local_addr().expect("fixture address");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("fixture accept");
        let mut buffer = [0_u8; 4096];
        let _ = socket.read(&mut buffer).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn submit_batch_returns_the_batch_id() {
    let url =
        batch_fixture(r#"{"message":"Batch successfully created.","id":"batch-abc"}"#, "{}").await;
    let account_url = account_fixture(100).await;
    let client = batch_client(url, account_url);
    let id = client.submit_batch(&["a@example.dev".into()]).await.expect("submit");
    assert_eq!(id, "batch-abc");
}

#[tokio::test]
async fn submit_batch_rejects_an_empty_list_without_a_request() {
    let client = batch_client("http://unused.invalid".into(), "http://unused.invalid".into());
    let error = client.submit_batch(&[]).await.expect_err("should reject");
    assert!(matches!(error, VerifyError::InvalidRequest(_)));
}

#[tokio::test]
async fn submit_batch_rejects_more_than_the_inline_results_limit_without_a_request() {
    let client = batch_client("http://unused.invalid".into(), "http://unused.invalid".into());
    let addresses: Vec<String> =
        (0..1001).map(|index| format!("user{index}@example.dev")).collect();
    let error = client.submit_batch(&addresses).await.expect_err("should reject");
    assert!(matches!(error, VerifyError::InvalidRequest(_)));
}

#[tokio::test]
async fn submit_batch_rejects_a_zero_credit_account_without_submitting() {
    let (url, hits) = fixture_with_hits(200, "{}").await;
    let account_url = account_fixture(0).await;
    let client = batch_client(url, account_url);
    let error = client.submit_batch(&["a@example.dev".into()]).await.expect_err("should reject");
    assert!(matches!(error, VerifyError::InsufficientCredits { available: 0, needed: 1 }));
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn submit_batch_rejects_a_batch_larger_than_available_credits_without_submitting() {
    let (url, hits) = fixture_with_hits(200, "{}").await;
    let account_url = account_fixture(2).await;
    let client = batch_client(url, account_url);
    let addresses =
        vec!["a@example.dev".to_string(), "b@example.dev".into(), "c@example.dev".into()];
    let error = client.submit_batch(&addresses).await.expect_err("should reject");
    assert!(matches!(error, VerifyError::InsufficientCredits { available: 2, needed: 3 }));
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// Same shape as `batch_fixture`, but counts how many requests it actually
/// received - used to prove the credit check short-circuits before any
/// batch-submission request reaches the server.
async fn fixture_with_hits(
    status: u16,
    body: &'static str,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = hits.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let addr = listener.local_addr().expect("fixture address");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { break };
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut buffer = [0_u8; 4096];
            let _ = socket.read(&mut buffer).await;
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn batch_status_reports_progress_while_processing() {
    let url = batch_fixture(
        "{}",
        r#"{"id":"batch-abc","message":"Your batch is being processed.","processed":3,"total":10,"reason_counts":{},"total_counts":{}}"#,
    )
    .await;
    let client = batch_client(url, "http://unused.invalid".into());
    let status = client.batch_status("batch-abc").await.expect("status");
    assert!(matches!(status, BatchStatus::Processing { processed: 3, total: 10 }));
}

#[tokio::test]
async fn batch_status_reports_inline_verdicts_once_complete() {
    // A completed batch's real response has no top-level processed/total -
    // verified live against the real API, despite the docs implying
    // otherwise. Deliberately omitted here to pin that down.
    let url = batch_fixture(
        "{}",
        r#"{"id":"batch-abc","message":"Batch verification completed.",
            "reason_counts":{},"total_counts":{"deliverable":1,"undeliverable":0,"risky":0,"unknown":0,"duplicate":0,"processed":1,"total":1},
            "emails":[{"email":"a@example.dev","state":"deliverable","reason":"accepted_email","disposable":false,"role":false}]}"#,
    )
    .await;
    let client = batch_client(url, "http://unused.invalid".into());
    let status = client.batch_status("batch-abc").await.expect("status");
    let BatchStatus::Complete { verdicts } = status else { panic!("expected Complete") };
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].0, "a@example.dev");
    assert_eq!(verdicts[0].1.state, State::Deliverable);
}

#[tokio::test]
async fn batch_status_reports_a_download_link_for_batches_too_large_for_inline_results() {
    let url = batch_fixture(
        "{}",
        r#"{"id":"batch-abc","message":"Batch verification completed.",
            "reason_counts":{},"total_counts":{"deliverable":0,"undeliverable":0,"risky":0,"unknown":0,"duplicate":0,"processed":5000,"total":5000},
            "download_file":"https://emailable.com/batches/abc.zip"}"#,
    )
    .await;
    let client = batch_client(url, "http://unused.invalid".into());
    let status = client.batch_status("batch-abc").await.expect("status");
    let BatchStatus::TooLargeForInlineResults { download_file, total } = status else {
        panic!("expected TooLargeForInlineResults")
    };
    assert_eq!(download_file, "https://emailable.com/batches/abc.zip");
    assert_eq!(total, 5000);
}

fn account_client(account_url: String) -> EmailableClient {
    EmailableClient::with_urls(
        "key".into(),
        5,
        "http://unused.invalid".into(),
        "http://unused.invalid".into(),
        account_url,
    )
}

#[tokio::test]
async fn account_reports_owner_and_credits() {
    let url = account_fixture(10_250).await;
    let account = account_client(url).account().await.expect("account");
    assert_eq!(account.owner_email, "ops@example.dev");
    assert_eq!(account.available_credits, 10_250);
}

#[tokio::test]
async fn account_non_success_status_is_a_transport_error() {
    let url = fixture(500, "internal error").await;
    let error = account_client(url).account().await.expect_err("should fail");
    assert!(matches!(error, VerifyError::Transport(_)));
}
