//! STS over its query protocol (XML answers): the account id, and the
//! credentials a web identity token is exchanged for.

use std::time::Duration;

use jiff::Timestamp;
use rama::http::BodyExtractExt;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;

use super::{Aws, AwsError, Call, Credentials};
use crate::web::encode_pairs;

const TIMEOUT: Duration = Duration::from_secs(30);
const FORM: &str = "application/x-www-form-urlencoded; charset=utf-8";

/// `GetCallerIdentity`: the 12-digit account the credentials belong to.
pub async fn account(aws: &Aws) -> Result<String, AwsError> {
    const OPERATION: &str = "GetCallerIdentity";
    let response = aws
        .call(Call {
            service: "sts",
            prefix: "sts",
            method: "POST",
            path: "/".into(),
            query: vec![],
            content_type: Some(FORM),
            target: None,
            body: b"Action=GetCallerIdentity&Version=2011-06-15".to_vec(),
            timeout: TIMEOUT,
        })
        .await
        .map_err(|error| error.into_error(OPERATION))?;
    if !response.is_success() {
        return Err(AwsError::Service {
            operation: OPERATION,
            status: response.status,
            code: response.code(),
            message: tag(&response.body, "Message"),
        });
    }
    tag(&response.body, "Account").ok_or(AwsError::Malformed(OPERATION))
}

/// `AssumeRoleWithWebIdentity`, unsigned: the token is the credential.
pub async fn assume_role_with_web_identity(
    http: &DefaultHttpWebClient,
    sts: &str,
    role_arn: &str,
    session_name: &str,
    token: &str,
) -> Result<Credentials, AwsError> {
    let body = encode_pairs([
        ("Action", "AssumeRoleWithWebIdentity"),
        ("Version", "2011-06-15"),
        ("RoleArn", role_arn),
        ("RoleSessionName", session_name),
        ("WebIdentityToken", token),
    ]);
    let response = http
        .post(format!("{sts}/"))
        .header("content-type", FORM)
        .header("user-agent", crate::web::USER_AGENT)
        .body(body)
        .send_with_timeout(TIMEOUT)
        .await
        .map_err(|_| {
            AwsError::Credentials("STS did not answer the web identity exchange".into())
        })?;
    let status = response.status();
    let xml = response
        .try_into_string()
        .await
        .map_err(|_| AwsError::Credentials("STS web identity answer unreadable".into()))?;
    if !status.is_success() {
        let code = tag(&xml, "Code").unwrap_or_else(|| status.to_string());
        return Err(AwsError::Credentials(format!("STS refused the web identity token: {code}")));
    }
    let field = |name| {
        tag(&xml, name)
            .ok_or_else(|| AwsError::Credentials(format!("STS web identity answer lacks {name}")))
    };
    let expires_at = field("Expiration")?
        .parse::<Timestamp>()
        .map_err(|_| AwsError::Credentials("STS sent an unreadable expiration".into()))?;
    Ok(Credentials {
        access_key_id: field("AccessKeyId")?,
        secret_access_key: field("SecretAccessKey")?,
        session_token: Some(field("SessionToken")?),
        expires_at: Some(expires_at),
    })
}

/// The text of the first `<name>` element, entities decoded.
pub fn tag(xml: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))?;
    let text = &xml[start..start + end];
    Some(
        text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}
