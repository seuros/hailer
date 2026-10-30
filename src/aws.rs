//! Hailer's own AWS client for the calls the `ses` feature makes: `SigV4`
//! over rama, and the credential sources servers use (see `credentials`).

mod credentials;
mod imds;
mod profile;
pub mod ses;
mod sigv4;
pub mod sqs;
pub mod sts;
#[cfg(test)]
mod tests;

use std::time::Duration;

use jiff::Timestamp;
use rama::http::BodyExtractExt;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;
use serde::de::DeserializeOwned;

pub use credentials::Credentials;
use credentials::{Provider, Source};
use profile::Profile;
use sigv4::Signing;

/// Where AWS settings are read from: the process environment, or a fixed
/// map in tests.
pub struct Env(Lookup);

type Lookup = Box<dyn Fn(&str) -> Option<String> + Send + Sync>;

impl Env {
    pub fn process() -> Self {
        Self(Box::new(|name| std::env::var(name).ok().filter(|value| !value.is_empty())))
    }

    #[cfg(test)]
    pub fn fixed(pairs: &[(&str, &str)]) -> Self {
        let map: std::collections::HashMap<String, String> =
            pairs.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect();
        Self(Box::new(move |name| map.get(name).cloned()))
    }

    fn get(&self, name: &str) -> Option<String> {
        (self.0)(name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    #[error("AWS region is required: set AWS_REGION or a profile `region`")]
    NoRegion,
    #[error("AWS region `{0}` is not a region name")]
    InvalidRegion(String),
    #[error(
        "no AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, a profile with keys, \
         a web identity, a container role, or run on EC2 with an instance role"
    )]
    NoCredentials,
    #[error("AWS profile `{0}` is not in the shared config or credentials files")]
    MissingProfile(String),
    #[error("AWS profile `{profile}` uses `{key}`, which hailer does not support")]
    UnsupportedProfile { profile: String, key: String },
    #[error("AWS credentials unavailable: {0}")]
    Credentials(String),
    #[error("AWS {operation} failed: {reason}")]
    Request { operation: &'static str, reason: String },
    #[error("AWS {operation} returned HTTP {status} ({})", code.as_deref().unwrap_or("no error code"))]
    Service { operation: &'static str, status: u16, code: Option<String>, message: Option<String> },
    #[error("AWS {0} returned an unreadable response")]
    Malformed(&'static str),
}

/// Why a signed call has no response to read.
#[derive(Debug)]
pub enum CallError {
    /// Nothing reached AWS: no credentials, or a refused connection.
    NotSent(String),
    /// The request may have reached AWS, but no answer came back.
    Lost,
}

impl CallError {
    fn into_error(self, operation: &'static str) -> AwsError {
        let reason = match self {
            Self::NotSent(reason) => reason,
            Self::Lost => "no response".to_owned(),
        };
        AwsError::Request { operation, reason }
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    error_type: Option<String>,
    pub body: String,
}

impl Response {
    pub const fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    /// The error code: the `x-amzn-ErrorType` header, else the JSON body's
    /// `__type`/`code`, else the XML body's `<Code>`. Namespaces stripped.
    pub fn code(&self) -> Option<String> {
        let raw = self
            .error_type
            .clone()
            .or_else(|| {
                let body: serde_json::Value = serde_json::from_str(&self.body).ok()?;
                ["__type", "code", "Code"]
                    .iter()
                    .find_map(|key| body.get(*key)?.as_str().map(str::to_owned))
            })
            .or_else(|| sts::tag(&self.body, "Code"))?;
        let code = raw.split(':').next().unwrap_or_default();
        let code = code.rsplit('#').next().unwrap_or_default().trim();
        (!code.is_empty()).then(|| code.to_owned())
    }

    pub fn message(&self) -> Option<String> {
        let body: serde_json::Value = serde_json::from_str(&self.body).ok()?;
        ["message", "Message"].iter().find_map(|key| body.get(*key)?.as_str().map(str::to_owned))
    }

    pub fn json<T: DeserializeOwned>(&self) -> Option<T> {
        serde_json::from_str(&self.body).ok()
    }
}

/// One signed request.
pub struct Call {
    /// The signing name: `ses`, `sqs`, `sts`.
    pub service: &'static str,
    /// The endpoint host's first label: `email`, `sqs`, `sts`.
    pub prefix: &'static str,
    pub method: &'static str,
    /// Each segment already percent-encoded.
    pub path: String,
    pub query: Vec<(String, String)>,
    pub content_type: Option<&'static str>,
    pub target: Option<&'static str>,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

pub struct Aws {
    http: DefaultHttpWebClient,
    region: String,
    /// Replaces `https://{prefix}.{region}.amazonaws.com` for every service.
    endpoint: Option<String>,
    credentials: Provider,
}

impl std::fmt::Debug for Aws {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aws").field("region", &self.region).finish_non_exhaustive()
    }
}

impl Aws {
    pub async fn load() -> Result<Self, AwsError> {
        Self::from_env(&Env::process()).await
    }

    async fn from_env(env: &Env) -> Result<Self, AwsError> {
        let http = DefaultHttpWebClient::default();
        let profile = Profile::load(env)?;
        let source = Source::resolve(env, profile.as_ref())?;
        let configured = env
            .get("AWS_REGION")
            .or_else(|| env.get("AWS_DEFAULT_REGION"))
            .or_else(|| profile.as_ref().and_then(|profile| profile.region().map(str::to_owned)));
        let region = match configured {
            Some(region) => region,
            None => imds::region(&http, &imds::endpoint(env)).await.ok_or(AwsError::NoRegion)?,
        };
        if region.is_empty()
            || !region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(AwsError::InvalidRegion(region));
        }
        Ok(Self { http, region, endpoint: None, credentials: Provider::new(source) })
    }

    #[cfg(test)]
    pub fn fixed(endpoint: String, region: &str, credentials: Credentials) -> Self {
        Self {
            http: DefaultHttpWebClient::default(),
            region: region.to_owned(),
            endpoint: Some(endpoint),
            credentials: Provider::new(Source::Static(credentials)),
        }
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    fn base(&self, prefix: &str) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{prefix}.{}.amazonaws.com", self.region))
    }

    pub async fn call(&self, call: Call) -> Result<Response, CallError> {
        let credentials = self
            .credentials
            .get(&self.http, &self.base("sts"))
            .await
            .map_err(|error| CallError::NotSent(error.to_string()))?;
        let base = self.base(call.prefix);
        let host = base
            .split_once("://")
            .map_or(base.as_str(), |(_, rest)| rest)
            .split('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let amz_date = Timestamp::now().strftime("%Y%m%dT%H%M%SZ").to_string();
        let mut headers: Vec<(&str, String)> =
            vec![("host", host), ("x-amz-date", amz_date.clone())];
        if let Some(content_type) = call.content_type {
            headers.push(("content-type", content_type.to_owned()));
        }
        if let Some(target) = call.target {
            headers.push(("x-amz-target", target.to_owned()));
        }
        if let Some(token) = &credentials.session_token {
            headers.push(("x-amz-security-token", token.clone()));
        }
        let authorization = sigv4::authorization(
            &credentials,
            &Signing {
                method: call.method,
                path: &call.path,
                query: &call.query,
                headers: &headers,
                body: &call.body,
                service: call.service,
                region: &self.region,
                amz_date: &amz_date,
            },
        );
        let query = sigv4::canonical_query(&call.query);
        let url = if query.is_empty() {
            format!("{base}{}", call.path)
        } else {
            format!("{base}{}?{query}", call.path)
        };
        let mut request =
            if call.method == "GET" { self.http.get(url) } else { self.http.post(url) };
        for (name, value) in headers.iter().filter(|(name, _)| *name != "host") {
            request = request.header(*name, value.as_str());
        }
        request = request
            .header("authorization", authorization)
            .header("user-agent", crate::web::USER_AGENT);
        if call.method != "GET" {
            request = request.body(call.body);
        }
        let response = request.send_with_timeout(call.timeout).await.map_err(|error| {
            if crate::web::never_connected(&*error) {
                CallError::NotSent("connection refused".to_owned())
            } else {
                CallError::Lost
            }
        })?;
        let status = response.status().as_u16();
        let error_type = response
            .headers()
            .get("x-amzn-errortype")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = match response.try_into_string().await {
            Ok(body) => body,
            Err(_) if (200..300).contains(&status) => return Err(CallError::Lost),
            Err(_) => String::new(),
        };
        Ok(Response { status, error_type, body })
    }

    /// A call whose JSON answer is the result, for the operator and
    /// background paths that only need success or a readable error.
    async fn read<T: DeserializeOwned>(
        &self,
        call: Call,
        operation: &'static str,
    ) -> Result<T, AwsError> {
        let response = self.call(call).await.map_err(|error| error.into_error(operation))?;
        if !response.is_success() {
            return Err(AwsError::Service {
                operation,
                status: response.status,
                code: response.code(),
                message: response.message(),
            });
        }
        response.json().ok_or(AwsError::Malformed(operation))
    }
}
