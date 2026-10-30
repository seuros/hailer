//! Where `SigV4` credentials come from, in the order the AWS SDKs look: the
//! environment, the shared files, a web identity (EKS IRSA), a container
//! role (ECS, EKS Pod Identity), then the EC2 instance role over `IMDSv2`.
//! Temporary credentials are cached and refreshed before they expire.

#[cfg(test)]
mod tests;

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use rama::http::BodyExtractExt;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::profile::Profile;
use super::{AwsError, Env, imds, sts};

const REFRESH_MARGIN: SignedDuration = SignedDuration::from_mins(5);
const CONTAINER: &str = "http://169.254.170.2";
const CONTAINER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<Timestamp>,
}

impl Credentials {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
            expires_at: None,
        }
    }

    fn fresh(&self, now: Timestamp) -> bool {
        self.expires_at
            .is_none_or(|at| now.saturating_add(REFRESH_MARGIN).is_ok_and(|soon| soon < at))
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials").field("expires_at", &self.expires_at).finish_non_exhaustive()
    }
}

/// The JSON an instance or container role answers with.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RoleCredentials {
    access_key_id: String,
    secret_access_key: String,
    token: Option<String>,
    expiration: Option<String>,
}

pub fn role_credentials(body: &str, source: &str) -> Result<Credentials, AwsError> {
    let role: RoleCredentials = serde_json::from_str(body)
        .map_err(|_| AwsError::Credentials(format!("{source} answered without credentials")))?;
    let expires_at =
        role.expiration.map(|at| at.parse::<Timestamp>()).transpose().map_err(|_| {
            AwsError::Credentials(format!("{source} sent an unreadable expiration"))
        })?;
    Ok(Credentials {
        access_key_id: role.access_key_id,
        secret_access_key: role.secret_access_key,
        session_token: role.token,
        expires_at,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Static(Credentials),
    WebIdentity { token_file: PathBuf, role_arn: String, session_name: String },
    Container { url: String, token: Option<String>, token_file: Option<PathBuf> },
    Imds { endpoint: String },
}

impl Source {
    pub fn resolve(env: &Env, profile: Option<&Profile>) -> Result<Self, AwsError> {
        if let (Some(id), Some(secret)) =
            (env.get("AWS_ACCESS_KEY_ID"), env.get("AWS_SECRET_ACCESS_KEY"))
        {
            return Ok(Self::Static(Credentials::new(id, secret, env.get("AWS_SESSION_TOKEN"))));
        }
        if let Some(profile) = profile
            && let Some(credentials) = profile.credentials()?
        {
            return Ok(Self::Static(credentials));
        }
        if let (Some(token_file), Some(role_arn)) =
            (env.get("AWS_WEB_IDENTITY_TOKEN_FILE"), env.get("AWS_ROLE_ARN"))
        {
            return Ok(Self::WebIdentity {
                token_file: token_file.into(),
                role_arn,
                session_name: env
                    .get("AWS_ROLE_SESSION_NAME")
                    .unwrap_or_else(|| "hailer".to_owned()),
            });
        }
        let token = env.get("AWS_CONTAINER_AUTHORIZATION_TOKEN");
        let token_file = env.get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE").map(PathBuf::from);
        if let Some(relative) = env.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            return Ok(Self::Container {
                url: format!("{CONTAINER}{relative}"),
                token,
                token_file,
            });
        }
        if let Some(full) = env.get("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
            container_url_allowed(&full)?;
            return Ok(Self::Container { url: full, token, token_file });
        }
        if env
            .get("AWS_EC2_METADATA_DISABLED")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Err(AwsError::NoCredentials);
        }
        Ok(Self::Imds { endpoint: imds::endpoint(env) })
    }

    async fn fetch(&self, http: &DefaultHttpWebClient, sts: &str) -> Result<Credentials, AwsError> {
        match self {
            Self::Static(credentials) => Ok(credentials.clone()),
            Self::WebIdentity { token_file, role_arn, session_name } => {
                let token = read_token(token_file).await?;
                sts::assume_role_with_web_identity(http, sts, role_arn, session_name, &token).await
            }
            Self::Container { url, token, token_file } => {
                let authorization = match token_file {
                    Some(file) => Some(read_token(file).await?),
                    None => token.clone(),
                };
                let mut request =
                    http.get(url.as_str()).header("user-agent", crate::web::USER_AGENT);
                if let Some(authorization) = authorization {
                    request = request.header("authorization", authorization);
                }
                let response =
                    request.send_with_timeout(CONTAINER_TIMEOUT).await.map_err(|_| {
                        AwsError::Credentials("container credentials unreachable".into())
                    })?;
                if !response.status().is_success() {
                    return Err(AwsError::Credentials(format!(
                        "container credentials answered HTTP {}",
                        response.status()
                    )));
                }
                let body = response.try_into_string().await.map_err(|_| {
                    AwsError::Credentials("container credentials answer unreadable".into())
                })?;
                role_credentials(&body, "the container credentials endpoint")
            }
            Self::Imds { endpoint } => imds::credentials(http, endpoint).await,
        }
    }
}

async fn read_token(path: &PathBuf) -> Result<String, AwsError> {
    tokio::fs::read_to_string(path)
        .await
        .map(|token| token.trim().to_owned())
        .map_err(|error| AwsError::Credentials(format!("reading {}: {error}", path.display())))
}

/// Plain HTTP anywhere but loopback or the ECS/EKS link-local hosts would
/// send the authorization token and credentials over the network.
fn container_url_allowed(url: &str) -> Result<(), AwsError> {
    if url.starts_with("https://") {
        return Ok(());
    }
    let authority = url.strip_prefix("http://").and_then(|rest| rest.split('/').next());
    let host = authority.map(|authority| {
        if authority.starts_with('[') {
            authority.split_inclusive(']').next().unwrap_or(authority)
        } else {
            authority.split(':').next().unwrap_or(authority)
        }
    });
    match host {
        Some("localhost" | "[::1]" | "169.254.170.2" | "169.254.170.23" | "[fd00:ec2::23]") => {
            Ok(())
        }
        Some(host) if host.starts_with("127.") => Ok(()),
        _ => Err(AwsError::Credentials(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI must be HTTPS, loopback, or the ECS/EKS endpoint"
                .into(),
        )),
    }
}

/// One source, its credentials cached until shortly before they expire.
pub struct Provider {
    source: Source,
    cache: Mutex<Option<Credentials>>,
}

impl Provider {
    pub const fn new(source: Source) -> Self {
        Self { source, cache: Mutex::const_new(None) }
    }

    pub async fn get(
        &self,
        http: &DefaultHttpWebClient,
        sts: &str,
    ) -> Result<Credentials, AwsError> {
        if let Source::Static(credentials) = &self.source {
            return Ok(credentials.clone());
        }
        let mut cache = self.cache.lock().await;
        if let Some(credentials) =
            cache.as_ref().filter(|credentials| credentials.fresh(Timestamp::now()))
        {
            return Ok(credentials.clone());
        }
        let fresh = self.source.fetch(http, sts).await?;
        *cache = Some(fresh.clone());
        drop(cache);
        Ok(fresh)
    }
}
