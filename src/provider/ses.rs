//! SES sends are single-attempt. A missing response is not safe to retry.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sesv2::Client;
use aws_sdk_sesv2::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_sesv2::primitives::Blob;
use aws_sdk_sesv2::types::{Destination, EmailContent, RawMessage};
use breaker_machines::{AsyncCircuitBreaker, CircuitError, PredicateClassifier};

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};

pub struct SesProvider {
    client: Client,
    breaker: AsyncCircuitBreaker,
}

impl SesProvider {
    pub fn new(config: &aws_config::SdkConfig) -> Self {
        let sdk = aws_sdk_sesv2::config::Builder::from(config)
            .retry_config(aws_sdk_sesv2::config::retry::RetryConfig::disabled())
            .timeout_config(
                aws_sdk_sesv2::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(3))
                    .operation_timeout(Duration::from_secs(20))
                    .build(),
            )
            .build();
        Self {
            client: Client::from_conf(sdk),
            breaker: AsyncCircuitBreaker::builder("ses_send")
                .failure_threshold(5)
                .failure_window_secs(30.0)
                .half_open_timeout_secs(15.0)
                .success_threshold(2)
                .failure_classifier(Arc::new(PredicateClassifier::new(|context| {
                    context
                        .error
                        .downcast_ref::<ProviderError>()
                        .is_none_or(ProviderError::counts_as_failure)
                })))
                .build_async(),
        }
    }

    async fn send_inner(&self, request: &SendRequest) -> Result<String, ProviderError> {
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
            .content(EmailContent::builder().raw(raw).build())
            .destination(destination);
        if let Some(from) = &request.from_email_address {
            call = call.from_email_address(from);
        }
        let output = call.send().await.map_err(|error| {
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

#[async_trait::async_trait]
impl MailProvider for SesProvider {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        // Invalid input must not consume a half-open probe or trip a circuit.
        request.validate()?;
        match self.breaker.call(|| self.send_inner(request)).await {
            Ok(id) => Ok(id),
            Err(CircuitError::Execution(error)) => Err(error),
            Err(_) => Err(ProviderError::Transport("SES circuit open or busy".into())),
        }
    }

    fn circuit_state(&self) -> &'static str {
        if self.breaker.is_open() {
            "open"
        } else if self.breaker.is_closed() {
            "closed"
        } else {
            "half_open"
        }
    }
}
