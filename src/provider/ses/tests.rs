use std::sync::atomic::{AtomicUsize, Ordering};

use aws_sdk_sesv2::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

fn request() -> SendRequest {
    SendRequest {
        raw_mime: "From: alice@example.com\r\nTo: bob@example.com\r\n\r\nTest".into(),
        from_email_address: None,
        to_addresses: vec!["bob@example.com".into()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

async fn fixture(
    status: u16,
    body: &'static str,
    disconnect: bool,
) -> (SesProvider, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
    let calls = Arc::new(AtomicUsize::new(0));
    let received = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("fixture accept");
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let count = socket.read(&mut chunk).await.expect("request");
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().expect("length"))
                        })
                        .expect("Content-Length");
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            received.fetch_add(1, Ordering::SeqCst);
            if !disconnect {
                socket.write_all(format!(
                    "HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await.expect("response");
            }
        }
    });
    let config = aws_config::SdkConfig::builder()
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "test-key",
            "test-secret",
            None,
            None,
            "fixture",
        )))
        .behavior_version(BehaviorVersion::latest())
        .retry_config(aws_sdk_sesv2::config::retry::RetryConfig::standard().with_max_attempts(3))
        .endpoint_url(endpoint)
        .build();
    (SesProvider::new(&config), calls, task)
}

#[tokio::test]
async fn sdk_send_is_single_attempt_even_for_server_errors() {
    let (provider, calls, task) = fixture(500, r#"{"message":"server failed"}"#, false).await;
    assert!(matches!(provider.send(&request()).await, Err(ProviderError::Transport(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.client.config().retry_config().expect("retry config").max_attempts(), 1);
    task.abort();
    assert!(task.await.expect_err("fixture stopped").is_cancelled());
}

#[tokio::test]
async fn network_loss_has_an_explicit_ambiguous_outcome_and_is_not_retried() {
    let (provider, calls, task) = fixture(200, "", true).await;
    assert!(matches!(provider.send(&request()).await, Err(ProviderError::Ambiguous)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.expect_err("fixture stopped").is_cancelled());
}

#[tokio::test]
async fn client_rejections_and_invalid_input_do_not_trip_the_provider_circuit() {
    let (provider, calls, task) =
        fixture(400, r#"{"__type":"MessageRejected","message":"recipient private detail"}"#, false)
            .await;
    for _ in 0..8 {
        let error = provider.send(&request()).await.expect_err("rejected");
        assert!(matches!(error, ProviderError::Rejected(_)));
        assert!(!error.to_string().contains("private"));
    }
    let mut invalid = request();
    invalid.to_addresses = vec!["invalid".into()];
    for _ in 0..8 {
        assert!(provider.send(&invalid).await.is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    assert_eq!(provider.circuit_state(), "closed");
    task.abort();
    assert!(task.await.expect_err("fixture stopped").is_cancelled());
}

#[tokio::test]
async fn transient_failures_open_the_circuit_without_more_http_calls() {
    let (provider, calls, task) = fixture(503, r#"{"message":"unavailable"}"#, false).await;
    for _ in 0..8 {
        assert!(provider.send(&request()).await.is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    assert_eq!(provider.circuit_state(), "open");
    task.abort();
    assert!(task.await.expect_err("fixture stopped").is_cancelled());
}
