//! `GET https://api.emailable.com/v1/verify` - one address per call.
//! <https://emailable.com/docs/api/emails/>

use std::time::Duration;

use serde::Deserialize;

use super::{State, Verdict, VerifyError};

const DEFAULT_BASE_URL: &str = "https://api.emailable.com/v1/verify";

pub struct EmailableClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    timeout_seconds: u8,
}

#[derive(Debug, Deserialize)]
struct VerifyResponse {
    state: String,
    reason: String,
    #[serde(default)]
    disposable: bool,
    #[serde(default)]
    role: bool,
}

impl EmailableClient {
    pub fn new(api_key: String, timeout_seconds: u8) -> Self {
        Self::with_base_url(api_key, timeout_seconds, DEFAULT_BASE_URL.to_owned())
    }

    /// Only the test suite overrides `base_url`, to point at a loopback
    /// fixture instead of the real Emailable API.
    pub fn with_base_url(api_key: String, timeout_seconds: u8, base_url: String) -> Self {
        Self {
            // A couple of seconds of local headroom over Emailable's own
            // `timeout` query param, so our own client times out second.
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(u64::from(timeout_seconds) + 3))
                .build()
                .expect("static reqwest client config"),
            base_url,
            api_key,
            timeout_seconds,
        }
    }

    pub async fn verify(&self, address: &str) -> Result<Verdict, VerifyError> {
        let response = self
            .http
            .get(&self.base_url)
            .query(&[
                ("email", address),
                ("api_key", self.api_key.as_str()),
                ("timeout", &self.timeout_seconds.to_string()),
            ])
            .send()
            .await
            .map_err(|error| VerifyError::Transport(error.to_string()))?;
        // Emailable's own "exceeded timeout, retry within 5 minutes" status.
        // Treated as a transport failure: the caller's policy decides
        // whether that means fail-open (Unknown) or fail-closed.
        if response.status().as_u16() == 249 {
            return Err(VerifyError::Transport("emailable timed out server-side (249)".into()));
        }
        if !response.status().is_success() {
            return Err(VerifyError::Transport(format!(
                "emailable returned HTTP {}",
                response.status()
            )));
        }
        let body: VerifyResponse = response
            .json()
            .await
            .map_err(|error| VerifyError::InvalidResponse(error.to_string()))?;
        Ok(Verdict {
            state: State::parse(&body.state),
            reason: body.reason,
            disposable: body.disposable,
            role: body.role,
        })
    }
}
