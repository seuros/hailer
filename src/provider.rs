//! Pluggable outbound send backends.
//!
//! One backend is active per deployment, chosen in `main.rs` from
//! [`crate::config::Config`]: SES (`provider/ses.rs`) or Postmark
//! (`provider/postmark.rs`). Neither `bay.rs` nor `mail.rs` knows which.

pub mod domain_block_guard;
pub mod guard;
#[cfg(feature = "postmark")]
pub mod postmark;
pub mod queued;
#[cfg(feature = "ses")]
pub mod ses;
#[cfg(test)]
pub mod test_support;
pub mod verify_guard;

use std::future::Future;
use std::sync::Arc;

use breaker_machines::{AsyncCircuitBreaker, CircuitError, PredicateClassifier};
use thiserror::Error;

use crate::mail::SendRequest;
use queued::{DispatchFailure, DispatchOutcome};

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

/// The breaker every provider sends through. Only transport failures and
/// unknown outcomes count; a rejection or invalid input never trips it.
struct SendCircuit {
    breaker: AsyncCircuitBreaker,
    provider: &'static str,
}

impl SendCircuit {
    fn new(name: &'static str, provider: &'static str) -> Self {
        Self {
            provider,
            breaker: AsyncCircuitBreaker::builder(name)
                .failure_threshold(5)
                .failure_window_secs(30.0)
                .half_open_timeout_secs(15.0)
                .success_threshold(2)
                .failure_classifier(Arc::new(PredicateClassifier::new(|context| {
                    context.error.downcast_ref::<ProviderError>().map_or_else(
                        || {
                            context
                                .error
                                .downcast_ref::<DispatchFailure>()
                                .is_none_or(DispatchFailure::counts_as_failure)
                        },
                        ProviderError::counts_as_failure,
                    )
                })))
                .build_async(),
        }
    }

    async fn send<F, Fut>(&self, call: F) -> Result<String, ProviderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<String, ProviderError>>,
    {
        match self.breaker.call(call).await {
            Ok(id) => Ok(id),
            Err(CircuitError::Execution(error)) => Err(error),
            Err(_) => {
                Err(ProviderError::Transport(format!("{} circuit open or busy", self.provider)))
            }
        }
    }

    async fn dispatch<F, Fut>(&self, call: F) -> DispatchOutcome
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<String, DispatchFailure>>,
    {
        match self.breaker.call(call).await {
            Ok(id) => DispatchOutcome::Accepted(id),
            Err(CircuitError::Execution(error)) => error.outcome(),
            Err(_) => DispatchOutcome::NotDispatched("provider_circuit_busy"),
        }
    }

    fn state(&self) -> &'static str {
        if self.breaker.is_open() {
            "open"
        } else if self.breaker.is_closed() {
            "closed"
        } else {
            "half_open"
        }
    }
}
