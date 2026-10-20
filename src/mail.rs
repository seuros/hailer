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
}
