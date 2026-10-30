//! SES sends are single-attempt. A missing response is not safe to retry.

#[cfg(test)]
mod tests;

use std::time::Duration;

use aws_sdk_sesv2::Client;
use aws_sdk_sesv2::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_sesv2::primitives::Blob;
use aws_sdk_sesv2::types::{Destination, EmailContent, MessageTag, RawMessage};

use crate::mail::SendRequest;
use crate::outbound::Correlation;
use crate::provider::queued::{DispatchFailure, DispatchOutcome, QueuedProvider};
use crate::provider::{MailProvider, ProviderError, SendCircuit};

pub struct SesProvider {
    client: Client,
    circuit: SendCircuit,
    configuration_set: Option<String>,
}

impl SesProvider {
    pub fn new(config: &aws_config::SdkConfig, configuration_set: Option<&str>) -> Self {
        let client = crate::aws_client::ses_client(
            config,
            Some((Duration::from_secs(3), Duration::from_secs(20))),
        );
        Self {
            configuration_set: configuration_set.map(str::to_owned),
            client,
            circuit: SendCircuit::new("ses_send", "SES"),
        }
    }

    fn call(
        &self,
        request: &SendRequest,
        correlation: Option<&Correlation>,
    ) -> Result<aws_sdk_sesv2::operation::send_email::builders::SendEmailFluentBuilder, ProviderError>
    {
        let raw = RawMessage::builder()
            .data(Blob::new(request.raw_mime.as_bytes().to_vec()))
            .build()
            .map_err(|_| ProviderError::InvalidRequest)?;
        let destination = Destination::builder()
            .set_to_addresses(Some(request.to_addresses.clone()))
            .set_cc_addresses(Some(request.cc_addresses.clone()))
            .set_bcc_addresses(Some(request.bcc_addresses.clone()))
            .build();
        let mut call = self
            .client
            .send_email()
            .set_configuration_set_name(self.configuration_set.clone())
            .content(EmailContent::builder().raw(raw).build())
            .destination(destination);
        if let Some(from) = &request.from_email_address {
            call = call.from_email_address(from);
        }
        if let Some(correlation) = correlation {
            if !correlation.is_valid() {
                return Err(ProviderError::InvalidRequest);
            }
            for (name, value) in [
                ("hailer_message_id", &correlation.hailer_message_id),
                ("hailer_attempt_id", &correlation.attempt_id),
            ] {
                call = call.email_tags(
                    MessageTag::builder()
                        .name(name)
                        .value(value)
                        .build()
                        .map_err(|_| ProviderError::InvalidRequest)?,
                );
            }
        }
        Ok(call)
    }

    async fn send_inner(&self, request: &SendRequest) -> Result<String, ProviderError> {
        let output = self.call(request, None)?.send().await.map_err(|error| {
            error.as_service_error().map_or_else(
                || {
                    if matches!(error, SdkError::ConstructionFailure(_)) {
                        ProviderError::InvalidRequest
                    } else {
                        ProviderError::Ambiguous
                    }
                },
                |service| {
                    classify(
                        service.code(),
                        error.raw_response().map(|response| response.status().as_u16()),
                    )
                },
            )
        })?;
        output
            .message_id()
            .filter(|id| !id.trim().is_empty())
            .map(ToOwned::to_owned)
            .ok_or(ProviderError::Ambiguous)
    }

    async fn dispatch_inner(
        &self,
        request: &SendRequest,
        correlation: &Correlation,
    ) -> Result<String, DispatchFailure> {
        let call =
            self.call(request, Some(correlation)).map_err(|_| DispatchFailure::NotDispatched)?;
        let output = call.send().await.map_err(|error| {
            if matches!(error, SdkError::ConstructionFailure(_)) {
                return DispatchFailure::NotDispatched;
            }
            match error.as_service_error().and_then(ProvideErrorMetadata::code) {
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
            }
        })?;
        output
            .message_id()
            .filter(|id| !id.trim().is_empty() && id.len() <= 256 && !id.contains('\0'))
            .map(ToOwned::to_owned)
            .ok_or(DispatchFailure::Unknown)
    }
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
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        // Invalid input must not consume a half-open probe or trip a circuit.
        request.validate()?;
        self.circuit.send(|| self.send_inner(request)).await
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
        self.circuit.dispatch(|| self.dispatch_inner(request, correlation)).await
    }
}
