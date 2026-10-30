//! Postmark's HTTP API. Single-attempt like SES: a request that may have
//! reached Postmark is never retried here.
//!
//! Postmark skips inactive recipients of a message that still has an active
//! one and says so only in the 200's `Message`; a 406 means none was active,
//! so nothing was sent.

mod message;
#[cfg(test)]
mod tests;

use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::Deserialize;
use tracing::warn;

use crate::config::PostmarkConfig;
use crate::mail::SendRequest;
use crate::outbound::Correlation;
use crate::provider::queued::{DispatchFailure, DispatchOutcome, QueuedProvider};
use crate::provider::{MailProvider, ProviderError, SendCircuit};

const API: &str = "https://api.postmarkapp.com";
const INACTIVE_RECIPIENT: i64 = 406;

pub struct PostmarkProvider {
    http: reqwest::Client,
    api: String,
    token: String,
    stream: String,
    circuit: SendCircuit,
}

/// What Postmark said, before deciding what it means for a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Accepted(String),
    /// The connection failed: nothing reached Postmark.
    Unsent,
    Refused {
        status: u16,
        code: Option<i64>,
    },
    /// The request left, but no usable answer came back.
    Unknown,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Reply {
    #[serde(rename = "MessageID", default)]
    message_id: String,
    #[serde(default)]
    error_code: Option<i64>,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Server {
    #[serde(rename = "ID")]
    pub id: u64,
    pub name: String,
    #[serde(default)]
    pub delivery_type: Option<String>,
}

impl PostmarkProvider {
    pub fn new(token: String, config: &PostmarkConfig) -> Self {
        Self::with_api(token, config, API.to_owned())
    }

    fn with_api(token: String, config: &PostmarkConfig, api: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(20))
                .build()
                .expect("static reqwest client config"),
            api,
            token,
            stream: config.message_stream.clone(),
            circuit: SendCircuit::new("postmark_send", "Postmark"),
        }
    }

    /// `GET /server`: proves the token and names the server it belongs to.
    pub async fn server(&self) -> anyhow::Result<Server> {
        let response = self
            .http
            .get(format!("{}/server", self.api))
            .header("X-Postmark-Server-Token", &self.token)
            .header(ACCEPT, "application/json")
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "Postmark server lookup returned HTTP {}",
            response.status()
        );
        Ok(response.json().await?)
    }

    fn render(
        &self,
        request: &SendRequest,
        correlation: Option<&Correlation>,
    ) -> Result<Vec<u8>, message::RenderError> {
        message::render(request, &self.stream, correlation)
            .inspect_err(|error| warn!(%error, "message cannot be sent through Postmark"))
    }

    async fn post(&self, body: Vec<u8>) -> Answer {
        let response = match self
            .http
            .post(format!("{}/email", self.api))
            .header("X-Postmark-Server-Token", &self.token)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_builder() => return Answer::Unsent,
            Err(_) => return Answer::Unknown,
        };
        let status = response.status().as_u16();
        let reply = response
            .bytes()
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Reply>(&bytes).ok());
        if status != 200 {
            return Answer::Refused { status, code: reply.and_then(|reply| reply.error_code) };
        }
        match reply {
            Some(reply)
                if reply.error_code.unwrap_or(0) == 0
                    && !reply.message_id.trim().is_empty()
                    && reply.message_id.len() <= 256
                    && !reply.message_id.contains('\0') =>
            {
                if let Some((_, addresses)) = reply.message.split_once("inactive addresses:") {
                    let skipped = addresses.split(',').filter(|a| !a.trim().is_empty()).count();
                    warn!(
                        message_id = %reply.message_id,
                        skipped,
                        "Postmark accepted the message but skips inactive recipients"
                    );
                }
                Answer::Accepted(reply.message_id)
            }
            _ => Answer::Unknown,
        }
    }
}

fn send_result(answer: Answer) -> Result<String, ProviderError> {
    match answer {
        Answer::Accepted(id) => Ok(id),
        Answer::Unsent => Err(ProviderError::Transport("Postmark unreachable".into())),
        Answer::Unknown => Err(ProviderError::Ambiguous),
        Answer::Refused { status: 422, code: Some(INACTIVE_RECIPIENT) } => {
            Err(ProviderError::Suppressed)
        }
        Answer::Refused { status: 401, .. } => {
            Err(ProviderError::Transport("Postmark refused the server token".into()))
        }
        Answer::Refused { status, .. } if status == 429 || status >= 500 => {
            Err(ProviderError::Transport("Postmark temporarily unavailable".into()))
        }
        Answer::Refused { status: 413, .. } => Err(ProviderError::InvalidRequest),
        // Do not echo Postmark's message: it can name recipients.
        Answer::Refused { status, code } => Err(ProviderError::Rejected(code.map_or_else(
            || format!("Postmark rejected the request (HTTP {status})"),
            |code| format!("Postmark rejected the request (error {code})"),
        ))),
    }
}

fn dispatch_result(answer: Answer) -> Result<String, DispatchFailure> {
    match answer {
        Answer::Accepted(id) => Ok(id),
        Answer::Unsent => Err(DispatchFailure::NotDispatched),
        Answer::Unknown => Err(DispatchFailure::Unknown),
        Answer::Refused { status: 401, .. } => Err(DispatchFailure::Unauthorized),
        Answer::Refused { status: 429, .. } => Err(DispatchFailure::Throttled),
        Answer::Refused { status, .. } if status >= 500 => Err(DispatchFailure::Unknown),
        Answer::Refused { .. } => Err(DispatchFailure::Rejected),
    }
}

impl MailProvider for PostmarkProvider {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        // Invalid input must not consume a half-open probe or trip a circuit.
        request.validate()?;
        let body = self.render(request, None).map_err(|_| ProviderError::InvalidRequest)?;
        self.circuit.send(|| async { send_result(self.post(body).await) }).await
    }

    fn circuit_state(&self) -> &'static str {
        self.circuit.state()
    }
}

impl QueuedProvider for PostmarkProvider {
    async fn dispatch(&self, request: &SendRequest, correlation: &Correlation) -> DispatchOutcome {
        const INVALID: DispatchOutcome =
            DispatchOutcome::NotAccepted { code: "invalid_message", retryable: false };
        if request.validate().is_err() {
            return INVALID;
        }
        if !correlation.is_valid() {
            return DispatchFailure::NotDispatched.outcome();
        }
        let Ok(body) = self.render(request, Some(correlation)) else {
            return INVALID;
        };
        self.circuit.dispatch(|| async { dispatch_result(self.post(body).await) }).await
    }
}
