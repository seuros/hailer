use std::sync::Arc;

use base64::Engine;

use super::*;
use crate::aws::Credentials;
use crate::provider::test_support::{Endpoint, correlation, endpoint, request};
use std::assert_matches;

fn provider(server: &Endpoint, configuration_set: Option<&str>) -> SesProvider {
    let credentials =
        Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", None);
    let aws = Aws::fixed(server.url.clone(), "us-east-1", credentials);
    SesProvider::new(Arc::new(aws), configuration_set)
}

async fn fixture(status: u16, body: &'static str, disconnect: bool) -> (SesProvider, Endpoint) {
    let server = endpoint(status, body, disconnect).await;
    (provider(&server, None), server)
}

#[tokio::test]
async fn queued_send_is_signed_raw_mime_with_attempt_tags() {
    let server = endpoint(200, r#"{"MessageId":"queued-provider-id"}"#, false).await;
    let provider = provider(&server, Some("hailer-feedback"));
    assert_eq!(
        provider.dispatch(&request(), &correlation()).await,
        DispatchOutcome::Accepted("queued-provider-id".into())
    );
    assert_eq!(server.calls(), 1);
    let (headers, body) = {
        let requests = server.requests();
        (requests[0].headers.clone(), requests[0].body.clone())
    };
    assert!(headers.starts_with("POST /v2/email/outbound-emails "), "{headers}");
    assert!(
        headers
            .contains("/us-east-1/ses/aws4_request, SignedHeaders=content-type;host;x-amz-date,"),
        "{headers}"
    );
    let raw = base64::engine::general_purpose::STANDARD
        .decode(body["Content"]["Raw"]["Data"].as_str().expect("raw data"))
        .expect("base64");
    assert_eq!(raw, request().raw_mime.as_bytes());
    assert_eq!(body["Destination"]["ToAddresses"], serde_json::json!(["bob@example.dev"]));
    let tags = body["EmailTags"].as_array().expect("tags");
    assert_eq!(body["ConfigurationSetName"], "hailer-feedback");
    assert_eq!(tags.len(), 2);
    assert!(tags.contains(
        &serde_json::json!({"Name": "hailer_message_id", "Value": correlation().hailer_message_id})
    ));
    assert!(tags.contains(
        &serde_json::json!({"Name": "hailer_attempt_id", "Value": correlation().attempt_id})
    ));
    server.stop().await;
}

#[tokio::test]
async fn queued_provider_distinguishes_positive_rejections_from_unknown_outcomes() {
    for (status, body, disconnect, outcome) in [
        (
            429,
            r#"{"__type":"TooManyRequestsException","message":"quota"}"#,
            false,
            DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true },
        ),
        (
            400,
            r#"{"__type":"MessageRejected","message":"private recipient detail"}"#,
            false,
            DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false },
        ),
        (500, r#"{"message":"server error"}"#, false, DispatchOutcome::Unknown),
        (200, "", true, DispatchOutcome::Unknown),
        (200, "{}", false, DispatchOutcome::Unknown),
    ] {
        let (provider, server) = fixture(status, body, disconnect).await;
        assert_eq!(provider.dispatch(&request(), &correlation()).await, outcome);
        assert_eq!(server.calls(), 1);
        server.stop().await;
    }
}

#[tokio::test]
async fn send_is_single_attempt_even_for_server_errors() {
    let (provider, server) = fixture(500, r#"{"message":"server failed"}"#, false).await;
    assert_matches!(provider.send(&request()).await, Err(ProviderError::Transport(_)));
    assert_eq!(server.calls(), 1);
    server.stop().await;
}

#[tokio::test]
async fn network_loss_has_an_explicit_ambiguous_outcome_and_is_not_retried() {
    let (provider, server) = fixture(200, "", true).await;
    assert_matches!(provider.send(&request()).await, Err(ProviderError::Ambiguous));
    assert_eq!(server.calls(), 1);
    server.stop().await;
}

#[tokio::test]
async fn client_rejections_and_invalid_input_do_not_trip_the_provider_circuit() {
    let (provider, server) =
        fixture(400, r#"{"__type":"MessageRejected","message":"recipient private detail"}"#, false)
            .await;
    for _ in 0..8 {
        let error = provider.send(&request()).await.expect_err("rejected");
        assert_matches!(error, ProviderError::Rejected(_));
        assert!(!error.to_string().contains("private"));
    }
    let mut invalid = request();
    invalid.to_addresses = vec!["invalid".into()];
    for _ in 0..8 {
        assert!(provider.send(&invalid).await.is_err());
    }
    assert_eq!(server.calls(), 8);
    assert_eq!(provider.circuit_state(), "closed");
    server.stop().await;
}

#[tokio::test]
async fn transient_failures_open_the_circuit_without_more_http_calls() {
    let (provider, server) = fixture(503, r#"{"message":"unavailable"}"#, false).await;
    for _ in 0..8 {
        assert!(provider.send(&request()).await.is_err());
    }
    assert_eq!(server.calls(), 5);
    assert_eq!(provider.circuit_state(), "open");
    server.stop().await;
}

/// Sends through the real SES account the environment or profile names, to
/// the mailbox simulator: accepted, never delivered, no reputation effect.
/// `HAILER_SES_FROM` must be a verified identity there.
#[tokio::test]
#[ignore = "needs AWS credentials and HAILER_SES_FROM"]
async fn live_ses_accepts_raw_mime_through_the_mailbox_simulator() {
    crate::install_crypto_provider().expect("TLS crypto provider");
    let from = std::env::var("HAILER_SES_FROM").expect("HAILER_SES_FROM");
    let to = "success@simulator.amazonses.com";
    let aws = Aws::load().await.expect("AWS credentials and region");
    eprintln!("region: {}", aws.region());
    let provider = SesProvider::new(Arc::new(aws), None);
    let request = SendRequest {
        raw_mime: format!(
            "From: Hailer Live Test <{from}>\r\nTo: {to}\r\nSubject: hailer own SES client\r\n\
             MIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n\
             Signed by hailer's SigV4, no AWS SDK.\r\n"
        ),
        from_email_address: None,
        to_addresses: vec![to.into()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    };
    let id = provider.send(&request).await.expect("sync send accepted");
    eprintln!("sync send: {id}");
    let outcome = provider.dispatch(&request, &correlation()).await;
    eprintln!("queued dispatch: {outcome:?}");
    assert_matches!(outcome, DispatchOutcome::Accepted(_));
}
