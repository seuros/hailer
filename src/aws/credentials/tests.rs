use std::assert_matches;

use rama::http::client::DefaultHttpWebClient;

use super::*;
use crate::provider::test_support::routed;

fn role_json(expires_in: SignedDuration) -> String {
    let expiration = Timestamp::now().checked_add(expires_in).expect("time");
    serde_json::json!({
        "Code": "Success",
        "AccessKeyId": "ASIAROLE",
        "SecretAccessKey": "role-secret",
        "Token": "role-token",
        "Expiration": expiration.to_string(),
    })
    .to_string()
}

#[test]
fn sources_are_tried_in_the_sdk_order() {
    let dir = tempfile::tempdir().expect("dir");
    let credentials = dir.path().join("credentials");
    std::fs::write(
        &credentials,
        "[default]\naws_access_key_id = PROFILE\naws_secret_access_key = p\n",
    )
    .expect("credentials");
    let credentials = credentials.to_str().expect("path");
    let profile = |env: &Env| Profile::load(env).expect("profile");
    let cases: [(&[(&str, &str)], &str); 7] = [
        (
            &[
                ("AWS_ACCESS_KEY_ID", "ENV"),
                ("AWS_SECRET_ACCESS_KEY", "e"),
                ("AWS_SHARED_CREDENTIALS_FILE", credentials),
            ],
            "env",
        ),
        (
            &[
                ("AWS_SHARED_CREDENTIALS_FILE", credentials),
                ("AWS_ROLE_ARN", "arn"),
                ("AWS_WEB_IDENTITY_TOKEN_FILE", "/t"),
            ],
            "profile",
        ),
        (
            &[
                ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
                ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/token"),
            ],
            "web identity",
        ),
        (
            &[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc")],
            "container http://169.254.170.2/v2/credentials/abc",
        ),
        (
            &[("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://169.254.170.23/v1/credentials")],
            "container http://169.254.170.23/v1/credentials",
        ),
        (&[], "imds http://169.254.169.254"),
        (
            &[("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:1338/")],
            "imds http://127.0.0.1:1338",
        ),
    ];
    for (pairs, expected) in cases {
        let env = Env::fixed(pairs);
        let found = match Source::resolve(&env, profile(&env).as_ref()).expect("source") {
            Source::Static(credentials) if credentials.access_key_id == "ENV" => "env".to_owned(),
            Source::Static(_) => "profile".to_owned(),
            Source::WebIdentity { .. } => "web identity".to_owned(),
            Source::Container { url, .. } => format!("container {url}"),
            Source::Imds { endpoint } => format!("imds {endpoint}"),
        };
        assert_eq!(found, expected, "{pairs:?}");
    }
    assert_matches!(
        Source::resolve(&Env::fixed(&[("AWS_EC2_METADATA_DISABLED", "true")]), None),
        Err(AwsError::NoCredentials)
    );
}

#[test]
fn a_full_container_uri_must_stay_local_unless_it_is_https() {
    for (url, allowed) in [
        ("https://credentials.example.dev/v1", true),
        ("http://127.0.0.1:8080/creds", true),
        ("http://localhost/creds", true),
        ("http://[::1]:80/creds", true),
        ("http://169.254.170.2/v2/credentials", true),
        ("http://[fd00:ec2::23]/v1/credentials", true),
        ("http://credentials.example.dev/v1", false),
        ("http://169.254.169.254/latest", false),
    ] {
        assert_eq!(container_url_allowed(url).is_ok(), allowed, "{url}");
    }
}

#[tokio::test]
async fn container_credentials_are_cached_until_they_near_expiry() {
    let http = DefaultHttpWebClient::default();
    let server = routed(vec![("GET /creds", 200, role_json(SignedDuration::from_hours(1)))]).await;
    let provider = Provider::new(Source::Container {
        url: format!("{}/creds", server.url),
        token: Some("pod-token".into()),
        token_file: None,
    });
    for _ in 0..3 {
        let credentials = provider.get(&http, "http://unused").await.expect("credentials");
        assert_eq!(credentials.access_key_id, "ASIAROLE");
        assert_eq!(credentials.session_token.as_deref(), Some("role-token"));
    }
    assert_eq!(server.calls(), 1);
    assert!(server.requests()[0].headers.contains("authorization: pod-token"));
    server.stop().await;

    let server = routed(vec![("GET /creds", 200, role_json(SignedDuration::from_mins(2)))]).await;
    let provider = Provider::new(Source::Container {
        url: format!("{}/creds", server.url),
        token: None,
        token_file: None,
    });
    for _ in 0..2 {
        provider.get(&http, "http://unused").await.expect("credentials");
    }
    assert_eq!(server.calls(), 2, "credentials expiring within the margin are refetched");
    server.stop().await;
}

#[tokio::test]
async fn imds_hands_out_the_instance_role_after_a_session_token() {
    let http = DefaultHttpWebClient::default();
    let server = routed(vec![
        ("PUT /latest/api/token", 200, "imds-token".into()),
        (
            "GET /latest/meta-data/iam/security-credentials/hailer-role",
            200,
            role_json(SignedDuration::from_hours(6)),
        ),
        ("GET /latest/meta-data/iam/security-credentials/", 200, "hailer-role\n".into()),
    ])
    .await;
    let provider = Provider::new(Source::Imds { endpoint: server.url.clone() });
    let credentials = provider.get(&http, "http://unused").await.expect("credentials");
    assert_eq!(credentials.secret_access_key, "role-secret");
    {
        let requests = server.requests();
        assert!(requests[0].headers.contains("x-aws-ec2-metadata-token-ttl-seconds: 21600"));
        assert!(
            requests[1..]
                .iter()
                .all(|request| request.headers.contains("x-aws-ec2-metadata-token: imds-token"))
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn a_web_identity_token_is_exchanged_with_sts() {
    let http = DefaultHttpWebClient::default();
    let dir = tempfile::tempdir().expect("dir");
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, "eyJ.web+identity/token=\n").expect("token");
    let expiration = Timestamp::now().checked_add(SignedDuration::from_hours(1)).expect("time");
    let xml = format!(
        "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
         <AccessKeyId>ASIAWEB</AccessKeyId><SecretAccessKey>web&amp;secret</SecretAccessKey>\
         <SessionToken>web-token</SessionToken><Expiration>{expiration}</Expiration>\
         </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
    );
    let server = routed(vec![("POST /", 200, xml)]).await;
    let provider = Provider::new(Source::WebIdentity {
        token_file,
        role_arn: "arn:aws:iam::123456789012:role/hailer".into(),
        session_name: "hailer".into(),
    });
    let credentials = provider.get(&http, &server.url).await.expect("credentials");
    assert_eq!(credentials.access_key_id, "ASIAWEB");
    assert_eq!(credentials.secret_access_key, "web&secret");
    assert_eq!(credentials.expires_at.map(Timestamp::as_second), Some(expiration.as_second()));
    let body = server.requests()[0].body.as_str().map(str::to_owned).expect("form body");
    assert!(body.contains("Action=AssumeRoleWithWebIdentity"), "{body}");
    assert!(body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fhailer"), "{body}");
    assert!(body.contains("WebIdentityToken=eyJ.web%2Bidentity%2Ftoken%3D"), "{body}");
    server.stop().await;
}

#[test]
fn credentials_never_print_their_secrets() {
    let credentials =
        Credentials::new("AKIDSECRETID", "the-secret-key", Some("the-session".into()));
    let printed = format!("{credentials:?} {:?}", Source::Static(credentials.clone()));
    for secret in ["AKIDSECRETID", "the-secret-key", "the-session"] {
        assert!(!printed.contains(secret), "{printed}");
    }
}
