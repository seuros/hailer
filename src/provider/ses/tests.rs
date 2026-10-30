use aws_sdk_sesv2::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};

use super::*;
use crate::provider::test_support::{Endpoint, correlation, endpoint, request};
use std::assert_matches;

fn provider(server: &Endpoint, configuration_set: Option<&str>) -> SesProvider {
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
        .endpoint_url(server.url.clone())
        .build();
    SesProvider::new(&config, configuration_set)
}

async fn fixture(status: u16, body: &'static str, disconnect: bool) -> (SesProvider, Endpoint) {
    let server = endpoint(status, body, disconnect).await;
    (provider(&server, None), server)
}

#[tokio::test]
async fn queued_send_carries_attempt_tags_without_sdk_retries() {
    let server = endpoint(200, r#"{"MessageId":"queued-provider-id"}"#, false).await;
    let provider = provider(&server, Some("hailer-feedback"));
    assert_eq!(
        provider.dispatch(&request(), &correlation()).await,
        DispatchOutcome::Accepted("queued-provider-id".into())
    );
    assert_eq!(server.calls(), 1);
    let body = server.requests()[0].body.clone();
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
async fn sdk_send_is_single_attempt_even_for_server_errors() {
    let (provider, server) = fixture(500, r#"{"message":"server failed"}"#, false).await;
    assert_matches!(provider.send(&request()).await, Err(ProviderError::Transport(_)));
    assert_eq!(server.calls(), 1);
    assert_eq!(provider.client.config().retry_config().expect("retry config").max_attempts(), 1);
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
