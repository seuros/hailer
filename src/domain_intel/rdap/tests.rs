use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

/// One fixture serves both the bootstrap registry and the domain RDAP
/// response, routed by request path, the same server address for both -
/// the bootstrap JSON declares its own loopback address as the TLD's RDAP
/// server, so `RdapClient` naturally follows it back to this fixture.
async fn fixture(
    tld: &'static str,
    domain_status: u16,
    domain_body: &'static str,
) -> (String, Arc<AtomicUsize>) {
    let bootstrap_hits = Arc::new(AtomicUsize::new(0));
    let counted = bootstrap_hits.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let addr = listener.local_addr().expect("fixture address");
    let base_url = format!("http://{addr}/");
    let bootstrap_body = format!(r#"{{"services":[[["{tld}"],["{base_url}"]]]}}"#);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { break };
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            if read == 0 {
                break;
            }
            let request = String::from_utf8_lossy(&buffer[..read]);
            let path = request.lines().next().unwrap_or_default();
            let (status, body) = if path.starts_with("GET /domain/") {
                (domain_status, domain_body)
            } else {
                counted.fetch_add(1, Ordering::SeqCst);
                (200, bootstrap_body.as_str())
            };
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (base_url, bootstrap_hits)
}

#[tokio::test]
async fn registration_date_is_parsed_from_the_events_array() {
    let (bootstrap_url, _hits) = fixture(
        "test",
        200,
        r#"{"events":[
            {"eventAction":"registration","eventDate":"1995-08-14T04:00:00Z"},
            {"eventAction":"expiration","eventDate":"2027-08-13T04:00:00Z"}
        ]}"#,
    )
    .await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    let facts = client.lookup("example.test").await.expect("lookup");
    assert_eq!(facts.registered_at, Some("1995-08-14T04:00:00Z".parse().expect("timestamp")));
}

#[tokio::test]
async fn an_unknown_tld_fails_open_as_unknown() {
    let (bootstrap_url, _hits) = fixture("test", 200, "{}").await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    let facts = client.lookup("example.nonexistent-tld").await.expect("lookup");
    assert_eq!(facts.registered_at, None);
}

#[tokio::test]
async fn a_404_domain_response_is_unknown_not_an_error() {
    let (bootstrap_url, _hits) = fixture("test", 404, r#"{"errorCode":404}"#).await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    let facts = client.lookup("example.test").await.expect("lookup");
    assert_eq!(facts.registered_at, None);
}

#[tokio::test]
async fn a_server_error_is_a_transport_error() {
    let (bootstrap_url, _hits) = fixture("test", 500, "oops").await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    let error = client.lookup("example.test").await.expect_err("should fail");
    assert!(matches!(error, IntelError::Transport(_)));
}

#[tokio::test]
async fn missing_registration_event_is_unknown_not_an_error() {
    let (bootstrap_url, _hits) = fixture(
        "test",
        200,
        r#"{"events":[{"eventAction":"last changed","eventDate":"2026-01-01T00:00:00Z"}]}"#,
    )
    .await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    let facts = client.lookup("example.test").await.expect("lookup");
    assert_eq!(facts.registered_at, None);
}

#[tokio::test]
async fn the_bootstrap_response_is_reused_across_lookups() {
    let (bootstrap_url, hits) = fixture(
        "test",
        200,
        r#"{"events":[{"eventAction":"registration","eventDate":"2020-01-01T00:00:00Z"}]}"#,
    )
    .await;
    let client = RdapClient::with_bootstrap_url(bootstrap_url);
    client.lookup("a.test").await.expect("first lookup");
    client.lookup("b.test").await.expect("second lookup");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}
