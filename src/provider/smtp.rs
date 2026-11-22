//! Direct SMTP submission (`provider = "smtp"`).
//!
//! Single-attempt like SES and Postmark: failures before transmitting the
//! `DATA` payload are guaranteed unsent and safe for the queued worker to retry;
//! a timeout or disconnect after `DATA` is reported as `Ambiguous` / `Unknown`
//! and never blindly retried.
//!
//! Unlike Postmark's JSON API, SMTP accepts Rails's rendered `raw_mime`
//! verbatim (after CRLF normalization and RFC 5321 dot-stuffing), preserving
//! MIME structure, inline CID attachments, custom headers, and DKIM signatures.

mod client;
mod mime;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use deadline_machines::Deadline;
use tracing::warn;

use crate::config::{SmtpConfig, SmtpTlsMode};
use crate::mail::SendRequest;
use crate::outbound::Correlation;
use crate::provider::queued::{DispatchFailure, DispatchOutcome, QueuedProvider};
use crate::provider::{MailProvider, ProviderError, SendCircuit, send_timeout};
use crate::rogue::Experiments;

pub use client::{SmtpCheckReport, SmtpCredentials};

use client::{SmtpClient, SmtpOutcome};

const TIMEOUT: Duration = Duration::from_secs(30);

pub struct SmtpProvider {
    client: SmtpClient,
    circuit: SendCircuit,
}

struct PreparedMessage {
    from: String,
    recipients: Vec<String>,
    data: Vec<u8>,
    header_message_id: Option<String>,
}

impl SmtpProvider {
    pub fn new(config: &SmtpConfig, credentials: Option<SmtpCredentials>) -> Self {
        if config.tls == SmtpTlsMode::None && credentials.is_some() {
            warn!(
                host = %config.host,
                port = config.effective_port(),
                "SMTP credentials configured with tls = \"none\"; credentials will be sent without TLS"
            );
        }
        Self {
            client: SmtpClient::new(config, credentials),
            circuit: SendCircuit::new("smtp_send", "SMTP"),
        }
    }

    pub fn with_experiments(mut self, experiments: Arc<Experiments>) -> Self {
        self.circuit.experiments = experiments;
        self
    }

    #[cfg(test)]
    pub(super) fn with_tls_config(
        config: &SmtpConfig,
        credentials: Option<SmtpCredentials>,
        tls_config: Arc<rama::tls::rustls::dep::rustls::ClientConfig>,
    ) -> Self {
        Self {
            client: SmtpClient::with_tls_config(config, credentials, tls_config),
            circuit: SendCircuit::new("smtp_send", "SMTP"),
        }
    }

    /// Connect, complete TLS (`STARTTLS` / implicit), run `EHLO`, authenticate
    /// when configured, and send `QUIT`. Used by `hailer smtp-check`.
    pub async fn check(&self) -> anyhow::Result<SmtpCheckReport> {
        self.client.check(TIMEOUT).await
    }

    fn prepare(
        request: &SendRequest,
        correlation: Option<&Correlation>,
    ) -> Result<PreparedMessage, ProviderError> {
        let from = mime::envelope_from(request).ok_or(ProviderError::InvalidRequest)?;
        let mut recipients = Vec::new();
        for addr in request.recipients() {
            if !mime::is_safe_envelope_address(addr) {
                return Err(ProviderError::InvalidAddress(addr.to_owned()));
            }
            recipients.push(addr.to_owned());
        }
        let header_message_id = mime::extract_message_id(&request.raw_mime);
        let data = mime::prepare_data(&request.raw_mime, correlation);
        Ok(PreparedMessage { from, recipients, data, header_message_id })
    }

    async fn deliver(
        &self,
        from: &str,
        recipients: &[String],
        data: &[u8],
        timeout: Duration,
    ) -> SmtpOutcome {
        let rcpt_refs: Vec<&str> = recipients.iter().map(String::as_str).collect();
        self.client.send(from, &rcpt_refs, data, timeout).await
    }
}

fn resolve_message_id(
    queue_id: Option<String>,
    header_message_id: Option<String>,
    correlation: Option<&Correlation>,
) -> String {
    if let Some(id) = queue_id {
        return id;
    }
    if let Some(id) = header_message_id {
        return id;
    }
    if let Some(corr) = correlation {
        return format!("smtp:{}", corr.attempt_id);
    }
    format!("smtp:{}", jiff::Timestamp::now().as_nanosecond())
}

fn send_result(
    outcome: SmtpOutcome,
    header_message_id: Option<String>,
) -> Result<String, ProviderError> {
    match outcome {
        SmtpOutcome::Accepted { queue_id } => {
            Ok(resolve_message_id(queue_id, header_message_id, None))
        }
        SmtpOutcome::Unsent => Err(ProviderError::Transport("SMTP server unreachable".into())),
        SmtpOutcome::AuthFailed => {
            Err(ProviderError::Transport("SMTP authentication failed".into()))
        }
        SmtpOutcome::TempFailed(code) => {
            Err(ProviderError::Transport(format!("SMTP temporarily unavailable ({code})")))
        }
        // Do not echo raw SMTP text: it can contain recipient addresses.
        SmtpOutcome::Rejected(code) => {
            Err(ProviderError::Rejected(format!("SMTP server rejected the message ({code})")))
        }
        SmtpOutcome::Unknown => Err(ProviderError::Ambiguous),
    }
}

fn dispatch_result(
    outcome: SmtpOutcome,
    header_message_id: Option<String>,
    correlation: &Correlation,
) -> Result<String, DispatchFailure> {
    match outcome {
        SmtpOutcome::Accepted { queue_id } => {
            Ok(resolve_message_id(queue_id, header_message_id, Some(correlation)))
        }
        SmtpOutcome::Unsent => Err(DispatchFailure::NotDispatched),
        SmtpOutcome::AuthFailed => Err(DispatchFailure::Unauthorized),
        SmtpOutcome::TempFailed(_) => Err(DispatchFailure::Throttled),
        SmtpOutcome::Rejected(_) => Err(DispatchFailure::Rejected),
        SmtpOutcome::Unknown => Err(DispatchFailure::Unknown),
    }
}

impl MailProvider for SmtpProvider {
    async fn send(
        &self,
        request: &SendRequest,
        deadline: Deadline,
    ) -> Result<String, ProviderError> {
        request.validate()?;
        let prepared = Self::prepare(request, None)?;
        let timeout = send_timeout(deadline, TIMEOUT)?;
        self.circuit
            .send(timeout, |left| async move {
                let outcome =
                    self.deliver(&prepared.from, &prepared.recipients, &prepared.data, left).await;
                send_result(outcome, prepared.header_message_id)
            })
            .await
    }

    fn circuit_state(&self) -> &'static str {
        self.circuit.state()
    }
}

impl QueuedProvider for SmtpProvider {
    async fn dispatch(&self, request: &SendRequest, correlation: &Correlation) -> DispatchOutcome {
        const INVALID: DispatchOutcome =
            DispatchOutcome::NotAccepted { code: "invalid_message", retryable: false };
        if request.validate().is_err() {
            return INVALID;
        }
        if !correlation.is_valid() {
            return DispatchFailure::NotDispatched.outcome();
        }
        let Ok(prepared) = Self::prepare(request, Some(correlation)) else {
            return INVALID;
        };
        self.circuit
            .dispatch(TIMEOUT, |left| async move {
                let outcome =
                    self.deliver(&prepared.from, &prepared.recipients, &prepared.data, left).await;
                dispatch_result(outcome, prepared.header_message_id, correlation)
            })
            .await
    }
}
