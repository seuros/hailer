//! Optional pre-send address verification. Disabled by default; a single
//! `Verifier::Disabled` variant always returns [`State::Unknown`] with
//! `Allow` as the derived policy, so the guard that wraps a
//! [`crate::provider::MailProvider`] never needs to special-case "no
//! verifier configured" — it just always has one.
//!
//! Swapping providers (Emailable today, something else tomorrow) means
//! adding a variant to [`Verifier`] and an arm to its `verify` match, not
//! touching the guard or the trait.

pub mod emailable;
#[cfg(test)]
mod tests;

use std::future::Future;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use emailable::EmailableClient;

/// Emailable's four states, kept provider-neutral so a future provider maps
/// onto the same four buckets rather than inventing its own vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Deliverable,
    Undeliverable,
    Risky,
    Unknown,
}

impl State {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deliverable => "deliverable",
            Self::Undeliverable => "undeliverable",
            Self::Risky => "risky",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "deliverable" => Self::Deliverable,
            "undeliverable" => Self::Undeliverable,
            "risky" => Self::Risky,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub state: State,
    pub reason: String,
    pub disposable: bool,
    pub role: bool,
}

impl Verdict {
    /// The fail-open verdict: no provider configured, or the provider call
    /// itself was inconclusive. Never blocks a send on its own.
    pub fn unknown(reason: impl Into<String>) -> Self {
        Self { state: State::Unknown, reason: reason.into(), disposable: false, role: false }
    }
}

#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("verification request failed: {0}")]
    Transport(String),
    #[error("verification response was not understood: {0}")]
    InvalidResponse(String),
}

/// Static dispatch, matching [`crate::provider::MailProvider`]'s own
/// `impl Future` convention — no `async-trait`, no boxed futures.
pub trait AddressVerifier: Send + Sync {
    fn verify(&self, address: &str) -> impl Future<Output = Result<Verdict, VerifyError>> + Send;
}

/// The one compile-time dispatch point a new provider needs: add a variant
/// here (and to [`crate::config::VerifierKind`]), nothing in
/// `provider/verify_guard.rs` or `state.rs` changes.
pub enum Verifier {
    Disabled,
    Emailable(EmailableClient),
}

impl AddressVerifier for Verifier {
    async fn verify(&self, address: &str) -> Result<Verdict, VerifyError> {
        match self {
            Self::Disabled => Ok(Verdict::unknown("verification disabled")),
            Self::Emailable(client) => client.verify(address).await,
        }
    }
}
