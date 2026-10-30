use std::assert_matches;

use super::*;
use crate::provider::test_support::endpoint;

/// Settings on top of static keys, and the region or error they lead to.
type Case<'a> = (&'a [(&'a str, &'a str)], Result<&'a str, &'a str>);

fn aws(url: String, token: Option<&str>) -> Aws {
    Aws::fixed(
        url,
        "eu-west-1",
        Credentials::new("AKIDEXAMPLE", "secret", token.map(str::to_owned)),
    )
}

#[tokio::test]
async fn calls_are_signed_for_their_service_and_region() {
    let server = endpoint(200, r#"{"SuppressedDestinationSummaries":[]}"#, false).await;
    let aws = aws(server.url.clone(), Some("session-token"));
    let response = ses::suppressed_destinations(&aws, Some("next+token/=")).await.expect("answer");
    assert!(response.is_success());
    let headers = server.requests()[0].headers.clone();
    let request_line = headers.lines().next().unwrap_or_default();
    assert_eq!(
        request_line,
        "GET /v2/email/suppression/addresses?NextToken=next%2Btoken%2F%3D&PageSize=1000 HTTP/1.1"
    );
    let authorization = headers
        .lines()
        .find_map(|line| line.strip_prefix("authorization: "))
        .expect("authorization header");
    let (scope, signature) = authorization.split_once(", Signature=").expect("signature");
    assert!(scope.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"), "{scope}");
    assert!(
        scope.ends_with(
            "/eu-west-1/ses/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token"
        ),
        "{scope}"
    );
    assert!(signature.len() == 64 && signature.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(headers.contains("x-amz-security-token: session-token"), "{headers}");
    assert!(
        headers.contains(&format!("user-agent: hailer/{}", env!("CARGO_PKG_VERSION"))),
        "{headers}"
    );
    server.stop().await;
}

#[test]
fn error_codes_come_from_the_header_or_the_body() {
    let response = |error_type: Option<&str>, body: &str| Response {
        status: 400,
        error_type: error_type.map(str::to_owned),
        body: body.to_owned(),
    };
    for (answer, code) in [
        (
            response(Some("MessageRejected:http://internal.amazon.com/coral/"), ""),
            Some("MessageRejected"),
        ),
        (
            response(None, r#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist"}"#),
            Some("QueueDoesNotExist"),
        ),
        (
            response(None, r#"{"code":"InvalidNextTokenException"}"#),
            Some("InvalidNextTokenException"),
        ),
        (
            response(
                None,
                "<ErrorResponse><Error><Code>InvalidClientTokenId</Code></Error></ErrorResponse>",
            ),
            Some("InvalidClientTokenId"),
        ),
        (response(None, ""), None),
    ] {
        assert_eq!(answer.code().as_deref(), code, "{:?} {}", answer.error_type, answer.body);
    }
}

#[tokio::test]
async fn sqs_speaks_the_json_protocol() {
    let server =
        endpoint(200, r#"{"Messages":[{"ReceiptHandle":"r-1","Body":"notification"}]}"#, false)
            .await;
    let aws = aws(server.url.clone(), None);
    let queue = "https://sqs.eu-west-1.amazonaws.com/123456789012/feedback";
    let messages = sqs::receive(&aws, queue).await.expect("messages");
    assert_eq!(messages[0].receipt_handle.as_deref(), Some("r-1"));
    assert_eq!(messages[0].body.as_deref(), Some("notification"));
    {
        let requests = server.requests();
        let recorded = &requests[0];
        assert!(
            recorded.headers.contains("x-amz-target: AmazonSQS.ReceiveMessage"),
            "{}",
            recorded.headers
        );
        assert!(
            recorded.headers.contains("content-type: application/x-amz-json-1.0"),
            "{}",
            recorded.headers
        );
        assert!(recorded.headers.contains("/eu-west-1/sqs/aws4_request"), "{}", recorded.headers);
        assert_eq!(recorded.body["QueueUrl"], queue);
        assert_eq!(recorded.body["WaitTimeSeconds"], 20);
    }
    server.stop().await;
}

#[tokio::test]
async fn sts_reads_the_account_from_xml() {
    let server = endpoint(
        200,
        "<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>arn:aws:iam::123456789012:user/hailer</Arn><Account>123456789012</Account></GetCallerIdentityResult></GetCallerIdentityResponse>",
        false,
    )
    .await;
    let aws = aws(server.url.clone(), None);
    assert_eq!(sts::account(&aws).await.expect("account"), "123456789012");
    assert_eq!(server.requests()[0].body, "Action=GetCallerIdentity&Version=2011-06-15");
    server.stop().await;
}

#[tokio::test]
async fn a_refused_connection_is_not_sent_and_a_dropped_answer_is_lost() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let url = format!("http://{}", closed.local_addr().expect("address"));
    drop(closed);
    assert_matches!(
        ses::send_email(&aws(url, None), b"{}".to_vec()).await,
        Err(CallError::NotSent(_))
    );

    let server = endpoint(200, "", true).await;
    assert_matches!(
        ses::send_email(&aws(server.url.clone(), None), b"{}".to_vec()).await,
        Err(CallError::Lost)
    );
    server.stop().await;
}

#[tokio::test]
async fn the_region_comes_from_the_environment_then_the_profile() {
    let dir = tempfile::tempdir().expect("dir");
    let config = dir.path().join("config");
    std::fs::write(&config, "[default]\nregion = ap-south-1\n").expect("config");
    let config = config.to_str().expect("path");
    let keys = [("AWS_ACCESS_KEY_ID", "a"), ("AWS_SECRET_ACCESS_KEY", "b")];
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let imds = format!("http://{}", closed.local_addr().expect("address"));
    drop(closed);
    let cases: [Case<'_>; 5] = [
        (&[("AWS_REGION", "us-west-2"), ("AWS_DEFAULT_REGION", "eu-west-1")], Ok("us-west-2")),
        (&[("AWS_DEFAULT_REGION", "eu-west-1")], Ok("eu-west-1")),
        (&[("AWS_CONFIG_FILE", config)], Ok("ap-south-1")),
        (&[("AWS_REGION", "Us East")], Err("not a region name")),
        (&[("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds.as_str())], Err("region is required")),
    ];
    for (settings, expected) in cases {
        let pairs: Vec<(&str, &str)> = keys.iter().chain(settings.iter()).copied().collect();
        let loaded = Aws::from_env(&Env::fixed(&pairs)).await;
        match expected {
            Ok(region) => assert_eq!(loaded.expect("loaded").region(), region, "{settings:?}"),
            Err(message) => {
                let error = loaded.expect_err("refused").to_string();
                assert!(error.contains(message), "{settings:?}: {error}");
            }
        }
    }
}
