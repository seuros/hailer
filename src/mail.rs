//! Internal request/response contract between the Ruby `hailer` gem's
//! `Hailer::DeliveryMethod` and this ship.
//!
//! Rails already renders the full RFC 5322 message via the `mail` gem
//! (`message.to_s`), so hailer never reconstructs MIME itself — it passes
//! `raw_mime` straight through to the active [`crate::provider::MailProvider`].
//! The envelope override fields mirror exactly what
//! `Aws::Rails::Sesv2Mailer#deliver!` computes today, so the cutover changes
//! nothing about delivery semantics.

use serde::{Deserialize, Serialize};

/// One `deliver!` call, as posted by `Hailer::DeliveryMethod`.
#[derive(Debug, Clone, Deserialize)]
pub struct SendRequest {
    /// Full raw MIME message (`message.to_s`).
    pub raw_mime: String,
    /// Set only when `smtp_envelope_from` was explicitly set on the Ruby
    /// message; `None` lets the provider use the MIME's own From header.
    #[serde(default)]
    pub from_email_address: Option<String>,
    #[serde(default)]
    pub to_addresses: Vec<String>,
    #[serde(default)]
    pub cc_addresses: Vec<String>,
    #[serde(default)]
    pub bcc_addresses: Vec<String>,
}

impl SendRequest {
    pub fn validate(&self) -> Result<(), crate::provider::ProviderError> {
        use crate::provider::ProviderError;
        if self.raw_mime.trim().is_empty() || self.raw_mime.len() > 40 * 1024 * 1024 {
            return Err(ProviderError::InvalidRequest);
        }
        if let Some(from) = &self.from_email_address
            && !email_address::EmailAddress::is_valid(from)
        {
            return Err(ProviderError::InvalidAddress(from.clone()));
        }
        let mut count = 0;
        for address in self.recipients() {
            if !email_address::EmailAddress::is_valid(address) {
                return Err(ProviderError::InvalidAddress(address.to_owned()));
            }
            count += 1;
        }
        if count == 0 {
            return Err(ProviderError::NoRecipients);
        }
        if count > 50 {
            return Err(ProviderError::InvalidRequest);
        }
        Ok(())
    }

    /// Every recipient address across to/cc/bcc, for boundary validation.
    pub fn recipients(&self) -> impl Iterator<Item = &str> {
        self.to_addresses
            .iter()
            .chain(self.cc_addresses.iter())
            .chain(self.bcc_addresses.iter())
            .map(String::as_str)
    }
}

/// A successful send: mirrors the `ses_message_id` the Ruby side already
/// stashes into `message.header[:ses_message_id]` today.
#[derive(Debug, Clone, Serialize)]
pub struct SendResponse {
    pub message_id: String,
}

/// A failed send, serialized back so the Ruby delivery method can raise.
#[derive(Debug, Clone, Serialize)]
pub struct SendErrorBody {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<&'static str>,
}
