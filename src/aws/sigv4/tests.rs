use super::*;

const DATE: &str = "20150830T123600Z";

fn example() -> Credentials {
    Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", None)
}

fn sign(
    credentials: &Credentials,
    method: &str,
    query: &[(&str, &str)],
    headers: &[(&str, String)],
    body: &[u8],
) -> String {
    let query: Vec<(String, String)> =
        query.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect();
    authorization(
        credentials,
        &Signing {
            method,
            path: "/",
            query: &query,
            headers,
            body,
            service: "service",
            region: "us-east-1",
            amz_date: DATE,
        },
    )
}

fn base() -> Vec<(&'static str, String)> {
    vec![("host", "example.amazonaws.com".into()), ("x-amz-date", DATE.into())]
}

/// `aws-signing-test-suite/v4`, the vectors the AWS SDKs are tested with.
#[test]
fn signatures_match_the_official_test_suite() {
    let unreserved = "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let session = Credentials::new(
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        Some("6e86291e8372ff2a2260956d9b8aae1d763fbf315fa00fa31553b73ebf194267".into()),
    );
    let mut session_headers = base();
    session_headers
        .push(("x-amz-security-token", session.session_token.clone().unwrap_or_default()));
    let form = b"Param1=value1";
    let mut form_headers = base();
    form_headers.extend([
        ("content-length", "13".to_owned()),
        ("content-type", "application/x-www-form-urlencoded".to_owned()),
        ("x-amz-content-sha256", hex(&Sha256::digest(form))),
    ]);
    let cases = [
        (
            "get-vanilla",
            sign(&example(), "GET", &[], &base(), b""),
            "SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
        ),
        (
            "get-vanilla-query-order-key-case",
            sign(&example(), "GET", &[("Param2", "value2"), ("Param1", "value1")], &base(), b""),
            "SignedHeaders=host;x-amz-date, Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
        ),
        (
            "get-vanilla-query-unreserved",
            sign(&example(), "GET", &[(unreserved, unreserved)], &base(), b""),
            "SignedHeaders=host;x-amz-date, Signature=9c3e54bfcdf0b19771a7f523ee5669cdf59bc7cc0884027167c21bb143a40197",
        ),
        (
            "get-vanilla-with-session-token",
            sign(&session, "GET", &[], &session_headers, b""),
            "SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=07ec1639c89043aa0e3e2de82b96708f198cceab042d4a97044c66dd9f74e7f8",
        ),
        (
            "post-x-www-form-urlencoded",
            sign(&example(), "POST", &[], &form_headers, form),
            "SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b",
        ),
    ];
    for (name, header, expected) in cases {
        assert_eq!(
            header,
            format!(
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, {expected}"
            ),
            "{name}"
        );
    }
}

#[test]
fn paths_are_encoded_again_for_the_canonical_request() {
    // `double-encode-path`: `@` once, an already encoded `%3D` twice.
    assert_eq!(
        canonical_uri("/test/@connections/JBDvjfGEIAMCERw%3D"),
        "/test/%40connections/JBDvjfGEIAMCERw%253D"
    );
    assert_eq!(canonical_uri("/"), "/");
}

#[test]
fn hmac_matches_rfc_4231() {
    assert_eq!(
        hex(&hmac(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    let long_key = [0xaa_u8; 131];
    assert_eq!(
        hex(&hmac(&long_key, b"Test Using Larger Than Block-Size Key - Hash Key First")),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}
