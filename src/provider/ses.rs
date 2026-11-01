//! SES sends are single-attempt. A missing response is not safe to retry.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use deadline_machines::Deadline;

use crate::aws::{Aws, CallError, Response, ses};
use crate::mail::SendRequest;
use crate::outbound::Correlation;
use crate::provider::queued::{DispatchFailure, DispatchOutcome, QueuedProvider};
use crate::provider::{MailProvider, ProviderError, SendCircuit, send_timeout};
use crate::rogue::Experiments;

pub struct SesProvider {
    aws: Arc<Aws>,
    circuit: SendCircuit,
    configuration_set: Option<String>,
}

impl SesProvider {
    pub fn new(aws: Arc<Aws>, configuration_set: Option<&str>) -> Self {
        Self {
            aws,
            configuration_set: configuration_set.map(str::to_owned),
            circuit: SendCircuit::new("ses_send", "SES"),
        }
    }

    pub fn with_experiments(mut self, experiments: Arc<Experiments>) -> Self {
        self.circuit.experiments = experiments;
        self
    }

    /// The `SendEmail` body: raw MIME, the envelope, and the attempt's tags.
    fn body(
        &self,
        request: &SendRequest,
        correlation: Option<&Correlation>,
    ) -> Result<Vec<u8>, ProviderError> {
        let mut body = serde_json::json!({
            "Content": {"Raw": {
                "Data": base64::engine::general_purpose::STANDARD.encode(request.raw_mime.as_bytes()),
            }},
            "Destination": {
                "ToAddresses": request.to_addresses,
                "CcAddresses": request.cc_addresses,
                "BccAddresses": request.bcc_addresses,
            },
        });
        if let Some(set) = &self.configuration_set {
            body["ConfigurationSetName"] = set.as_str().into();
        }
        if let Some(from) = &request.from_email_address {
            body["FromEmailAddress"] = from.as_str().into();
        }
        if let Some(correlation) = correlation {
            if !correlation.is_valid() {
                return Err(ProviderError::InvalidRequest);
            }
            body["EmailTags"] = serde_json::json!([
                {"Name": "hailer_message_id", "Value": correlation.hailer_message_id},
                {"Name": "hailer_attempt_id", "Value": correlation.attempt_id},
            ]);
        }
        Ok(body.to_string().into_bytes())
    }

    async fn send_inner(
        &self,
        request: &SendRequest,
        timeout: Duration,
    ) -> Result<String, ProviderError> {
        let body = self.body(request, None)?;
        let response =
            ses::send_email(&self.aws, body, timeout).await.map_err(|error| match error {
                CallError::NotSent(_) => ProviderError::Transport("SES unreachable".into()),
                CallError::Lost => ProviderError::Ambiguous,
            })?;
        if !response.is_success() {
            return Err(classify(response.code().as_deref(), Some(response.status)));
        }
        message_id(&response).ok_or(ProviderError::Ambiguous)
    }

    async fn dispatch_inner(
        &self,
        request: &SendRequest,
        correlation: &Correlation,
        timeout: Duration,
    ) -> Result<String, DispatchFailure> {
        let body =
            self.body(request, Some(correlation)).map_err(|_| DispatchFailure::NotDispatched)?;
        let response =
            ses::send_email(&self.aws, body, timeout).await.map_err(|error| match error {
                CallError::NotSent(_) => DispatchFailure::NotDispatched,
                CallError::Lost => DispatchFailure::Unknown,
            })?;
        if !response.is_success() {
            return Err(match response.code().as_deref() {
                Some("TooManyRequestsException" | "LimitExceededException") => {
                    DispatchFailure::Throttled
                }
                Some(
                    "AccountSuspendedException"
                    | "BadRequestException"
                    | "MailFromDomainNotVerifiedException"
                    | "MessageRejected"
                    | "NotFoundException"
                    | "SendingPausedException",
                ) => DispatchFailure::Rejected,
                // A generic 5xx, timeout or disconnect is never retry evidence.
                _ => DispatchFailure::Unknown,
            });
        }
        message_id(&response).ok_or(DispatchFailure::Unknown)
    }
}

fn message_id(response: &Response) -> Option<String> {
    let body: serde_json::Value = response.json()?;
    let id = body.get("MessageId")?.as_str()?;
    (!id.trim().is_empty() && id.len() <= 256 && !id.contains('\0')).then(|| id.to_owned())
}

fn classify(code: Option<&str>, status: Option<u16>) -> ProviderError {
    if matches!(code, Some("TooManyRequestsException" | "LimitExceededException"))
        || status.is_some_and(|status| status >= 500 || status == 429)
    {
        ProviderError::Transport("SES temporarily unavailable".into())
    } else {
        // Do not echo AWS response messages: they can contain recipients.
        ProviderError::Rejected("SES rejected the request".into())
    }
}

impl MailProvider for SesProvider {
    async fn send(
        &self,
        request: &SendRequest,
        deadline: Deadline,
    ) -> Result<String, ProviderError> {
        // Invalid input must not consume a half-open probe or trip a circuit.
        request.validate()?;
        let timeout = send_timeout(deadline, ses::SEND_TIMEOUT)?;
        self.circuit.send(timeout, |left| self.send_inner(request, left)).await
    }

    fn circuit_state(&self) -> &'static str {
        self.circuit.state()
    }
}

impl QueuedProvider for SesProvider {
    async fn dispatch(&self, request: &SendRequest, correlation: &Correlation) -> DispatchOutcome {
        if request.validate().is_err() {
            return DispatchOutcome::NotAccepted { code: "invalid_message", retryable: false };
        }
        self.circuit
            .dispatch(ses::SEND_TIMEOUT, |left| self.dispatch_inner(request, correlation, left))
            .await
    }
}
