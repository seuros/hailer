//! Pluggable outbound send backends.
//!
//! One backend is active per deployment, chosen in `main.rs` from
//! [`crate::config::Config`]. SES is the only implementation today
//! (`provider/ses.rs`); a future SMTP/Postmark backend implements the same
//! trait and is swapped in the same way — no change to `bay.rs` or `mail.rs`.

pub mod guard;
pub mod ses;

use async_trait::async_trait;
use thiserror::Error;

use crate::mail::SendRequest;

/// Why a send failed, coarse enough for the Ruby side to decide whether to
/// retry (`Transport`) or give up (`InvalidAddress`/`Rejected`).
#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("invalid recipient address: {0}")]
    InvalidAddress(String),
    #[error("no recipients")]
    NoRecipients,
    #[error("all recipients are suppressed")]
    Suppressed,
    #[error("provider rejected the message: {0}")]
    Rejected(String),
    #[error("provider call failed: {0}")]
    Transport(String),
}

/// A backend that can hand a fully-rendered raw MIME message to a real mail
/// transport and report back the transport's message id.
#[async_trait]
pub trait MailProvider: Send + Sync {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError>;
}
