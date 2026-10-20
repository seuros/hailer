//! SES backend: `send_email` with an `EmailContent::Raw` payload — the
//! SESv2-era equivalent of the legacy `SendRawEmail` operation (SES v2 has no
//! separate raw-send call; raw MIME is a content variant of the unified
//! `send_email`).
//!
//! Retries/backoff are intentionally not the SDK's: `breaker-machines` owns
//! failure accounting here, consistent with how orbitcast treats its Rails
//! RPC caller (see `orbitcast/src/rpc_client.rs`), so a flapping SES account
//! trips one breaker fleet-wide operators already know how to read.

use aws_sdk_sesv2::Client;
use aws_sdk_sesv2::primitives::Blob;
use aws_sdk_sesv2::types::{Destination, EmailContent, RawMessage};
use breaker_machines::{AsyncCircuitBreaker, CircuitError};
use email_address::EmailAddress;
use tracing::warn;

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};

const CIRCUIT_NAME: &str = "ses_send";

pub struct SesProvider {
    client: Client,
    breaker: AsyncCircuitBreaker,
}

impl SesProvider {
    /// Share the startup credential/region configuration with the SQS consumer.
    pub fn new(config: &aws_config::SdkConfig) -> Self {
        let client = Client::new(config);
        let breaker = AsyncCircuitBreaker::builder(CIRCUIT_NAME)
            .failure_threshold(5)
            .failure_window_secs(30.0)
            .half_open_timeout_secs(15.0)
            .success_threshold(2)
            .build_async();
        Self { client, breaker }
    }

    fn validate_addresses(request: &SendRequest) -> Result<(), ProviderError> {
        if let Some(from) = &request.from_email_address {
            validate_one(from)?;
        }
        let mut any = false;
        for address in request.recipients() {
            validate_one(address)?;
            any = true;
        }
        if !any {
            return Err(ProviderError::NoRecipients);
        }
        Ok(())
    }

    fn destination(request: &SendRequest) -> Destination {
        let mut builder = Destination::builder();
        if !request.to_addresses.is_empty() {
            builder = builder.set_to_addresses(Some(request.to_addresses.clone()));
        }
        if !request.cc_addresses.is_empty() {
            builder = builder.set_cc_addresses(Some(request.cc_addresses.clone()));
        }
        if !request.bcc_addresses.is_empty() {
            builder = builder.set_bcc_addresses(Some(request.bcc_addresses.clone()));
        }
        builder.build()
    }

    async fn send_inner(&self, request: &SendRequest) -> Result<String, ProviderError> {
        Self::validate_addresses(request)?;

        let raw = RawMessage::builder()
            .data(Blob::new(request.raw_mime.as_bytes().to_vec()))
            .build()
            .map_err(|error| ProviderError::Transport(error.to_string()))?;
        let content = EmailContent::builder().raw(raw).build();

        let mut call =
            self.client.send_email().content(content).destination(Self::destination(request));
        if let Some(from) = &request.from_email_address {
            call = call.from_email_address(from);
        }

        let output =
            call.send().await.map_err(|error| ProviderError::Rejected(error.to_string()))?;
        output.message_id().map(ToOwned::to_owned).ok_or_else(|| {
            ProviderError::Rejected("SES accepted the message without a message id".into())
        })
    }
}

fn validate_one(address: &str) -> Result<(), ProviderError> {
    if EmailAddress::is_valid(address) {
        Ok(())
    } else {
        Err(ProviderError::InvalidAddress(address.to_string()))
    }
}

#[async_trait::async_trait]
impl MailProvider for SesProvider {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        let outcome = self.breaker.call(|| async { self.send_inner(request).await }).await;
        match outcome {
            Ok(message_id) => Ok(message_id),
            Err(CircuitError::Execution(error)) => {
                warn!(error = %error, "SES send failed");
                Err(error)
            }
            Err(other) => {
                warn!(reason = %other, "SES send short-circuited by the breaker");
                Err(ProviderError::Transport(format!("circuit open: {other}")))
            }
        }
    }
}
