//! Pluggable outbound send backends.
//!
//! One backend is active per deployment, chosen in `main.rs` from
//! [`crate::config::Config`]. SES is the only implementation today
//! (`provider/ses.rs`); a future SMTP/Postmark backend implements the same
//! trait and is swapped in the same way — no change to `bay.rs` or `mail.rs`.

pub mod guard;
pub mod queued;
pub mod ses;

use std::future::Future;
use thiserror::Error;

use crate::mail::SendRequest;

/// A failure is not a promise that retrying a send is safe.
#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("invalid recipient address")]
    InvalidAddress(String),
    #[error("invalid message or envelope")]
    InvalidRequest,
    #[error("no recipients")]
    NoRecipients,
    #[error("all recipients are suppressed")]
    Suppressed,
    #[error("provider rejected the message: {0}")]
    Rejected(String),
    #[error("provider call failed: {0}")]
    Transport(String),
    #[error("provider outcome unknown; do not automatically retry")]
    Ambiguous,
    #[error("local recipient rate limit exceeded")]
    RateLimited,
}

impl ProviderError {
    pub const fn status(&self) -> u16 {
        match self {
            Self::InvalidAddress(_)
            | Self::InvalidRequest
            | Self::NoRecipients
            | Self::Suppressed => 422,
            Self::Rejected(_) => 502,
            Self::Transport(_) | Self::Ambiguous => 503,
            Self::RateLimited => 429,
        }
    }

    pub const fn code(&self) -> Option<&'static str> {
        match self {
            Self::Ambiguous => Some("outcome_unknown"),
            Self::RateLimited => Some("rate_limited"),
            _ => None,
        }
    }

    pub const fn counts_as_failure(&self) -> bool {
        matches!(self, Self::Transport(_) | Self::Ambiguous)
    }
}

/// A backend that can hand a fully-rendered raw MIME message to a real mail
/// transport and report back the transport's message id.
///
/// Static dispatch keeps the native future unboxed. The explicit `Send`
/// contract allows both docking links and supervised workers to spawn it.
pub trait MailProvider: Send + Sync {
    fn send(
        &self,
        request: &SendRequest,
    ) -> impl Future<Output = Result<String, ProviderError>> + Send;

    fn circuit_state(&self) -> &'static str {
        "unreported"
    }
}
