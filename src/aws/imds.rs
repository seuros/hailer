//! The EC2 instance metadata service, `IMDSv2` only (session token first).

use std::time::Duration;

use rama::http::BodyExtractExt;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;

use super::credentials::role_credentials;
use super::{AwsError, Credentials, Env};

const DEFAULT: &str = "http://169.254.169.254";
const TIMEOUT: Duration = Duration::from_secs(2);
const ROLES: &str = "/latest/meta-data/iam/security-credentials/";

pub fn endpoint(env: &Env) -> String {
    env.get("AWS_EC2_METADATA_SERVICE_ENDPOINT")
        .unwrap_or_else(|| DEFAULT.to_owned())
        .trim_end_matches('/')
        .to_owned()
}

pub async fn credentials(
    http: &DefaultHttpWebClient,
    endpoint: &str,
) -> Result<Credentials, AwsError> {
    let token = token(http, endpoint).await?;
    let roles = get(http, endpoint, &token, ROLES).await?;
    let role = roles
        .lines()
        .map(str::trim)
        .find(|role| !role.is_empty())
        .ok_or_else(|| AwsError::Credentials("the EC2 instance has no IAM role".into()))?;
    let body = get(http, endpoint, &token, &format!("{ROLES}{role}")).await?;
    role_credentials(&body, "the EC2 instance metadata service")
}

/// `None` off EC2, or when the service does not answer.
pub async fn region(http: &DefaultHttpWebClient, endpoint: &str) -> Option<String> {
    let token = token(http, endpoint).await.ok()?;
    let region = get(http, endpoint, &token, "/latest/meta-data/placement/region").await.ok()?;
    let region = region.trim();
    (!region.is_empty()).then(|| region.to_owned())
}

async fn token(http: &DefaultHttpWebClient, endpoint: &str) -> Result<String, AwsError> {
    let response = http
        .put(format!("{endpoint}/latest/api/token"))
        .header("x-aws-ec2-metadata-token-ttl-seconds", "21600")
        .header("user-agent", crate::web::USER_AGENT)
        .send_with_timeout(TIMEOUT)
        .await
        .map_err(|_| unavailable())?;
    text(response).await
}

async fn get(
    http: &DefaultHttpWebClient,
    endpoint: &str,
    token: &str,
    path: &str,
) -> Result<String, AwsError> {
    let response = http
        .get(format!("{endpoint}{path}"))
        .header("x-aws-ec2-metadata-token", token)
        .header("user-agent", crate::web::USER_AGENT)
        .send_with_timeout(TIMEOUT)
        .await
        .map_err(|_| unavailable())?;
    text(response).await
}

async fn text(response: rama::http::Response) -> Result<String, AwsError> {
    if !response.status().is_success() {
        return Err(AwsError::Credentials(format!(
            "the EC2 instance metadata service answered HTTP {}",
            response.status()
        )));
    }
    response.try_into_string().await.map_err(|_| unavailable())
}

fn unavailable() -> AwsError {
    AwsError::Credentials("the EC2 instance metadata service did not answer".into())
}
