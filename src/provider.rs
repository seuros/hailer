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
use std::time::Duration;

use breaker_machines::{AsyncCircuitBreaker, CircuitError, PredicateClassifier};
use deadline_machines::Deadline;
use thiserror::Error;

use crate::mail::SendRequest;
use crate::rogue::{Experiments, Injected, Target};
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
    #[error("request budget spent before dispatch; nothing was sent")]
    BudgetSpent,
}

impl ProviderError {
    pub const fn status(&self) -> u16 {
        match self {
            Self::InvalidAddress(_)
            | Self::InvalidRequest
            | Self::NoRecipients
            | Self::Suppressed => 422,
            Self::Rejected(_) => 502,
            Self::Transport(_) | Self::Ambiguous | Self::BudgetSpent => 503,
            Self::RateLimited => 429,
        }
    }

    pub const fn code(&self) -> Option<&'static str> {
        match self {
            Self::Ambiguous => Some("outcome_unknown"),
            Self::RateLimited => Some("rate_limited"),
            Self::BudgetSpent => Some("budget_spent"),
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
        deadline: Deadline,
    ) -> impl Future<Output = Result<String, ProviderError>> + Send;

    fn circuit_state(&self) -> &'static str {
        "unreported"
    }
}

/// Less budget than this and a send is refused instead of started.
pub const SEND_FLOOR: Duration = Duration::from_secs(2);

/// A provider id that a garbling experiment reported instead of the real one.
fn garble(id: &str) -> String {
    format!("rogue-{id}")
}

/// The timeout a send gets under `deadline`, at most `cap`.
fn send_timeout(deadline: Deadline, cap: Duration) -> Result<Duration, ProviderError> {
    crate::budget::within(deadline, SEND_FLOOR, cap).ok_or(ProviderError::BudgetSpent)
}

/// The breaker every provider sends through. Only transport failures and
/// unknown outcomes count; a rejection or invalid input never trips it. An
/// armed [`Target::Provider`] experiment runs inside it, so injected faults
/// reach the breaker the way real ones do.
struct SendCircuit {
    breaker: AsyncCircuitBreaker,
    provider: &'static str,
    experiments: Arc<Experiments>,
}

impl SendCircuit {
    fn new(name: &'static str, provider: &'static str) -> Self {
        Self {
            provider,
            experiments: Arc::default(),
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

    async fn send<F, Fut>(&self, timeout: Duration, call: F) -> Result<String, ProviderError>
    where
        F: FnOnce(Duration) -> Fut,
        Fut: Future<Output = Result<String, ProviderError>>,
    {
        let attempt = || async move {
            match self.experiments.inject(Target::Provider, timeout).await {
                Injected::Pass(left) => call(left).await,
                Injected::Garble(left) => call(left).await.map(|id| garble(&id)),
                Injected::Raise => Err(ProviderError::Transport(format!(
                    "{} unreachable (rogue experiment)",
                    self.provider
                ))),
                Injected::TimedOut => Err(ProviderError::Ambiguous),
            }
        };
        match self.breaker.call(attempt).await {
            Ok(id) => Ok(id),
            Err(CircuitError::Execution(error)) => Err(error),
            Err(_) => {
                Err(ProviderError::Transport(format!("{} circuit open or busy", self.provider)))
            }
        }
    }

    async fn dispatch<F, Fut>(&self, timeout: Duration, call: F) -> DispatchOutcome
    where
        F: FnOnce(Duration) -> Fut,
        Fut: Future<Output = Result<String, DispatchFailure>>,
    {
        let attempt = || async move {
            match self.experiments.inject(Target::Provider, timeout).await {
                Injected::Pass(left) => call(left).await,
                Injected::Garble(left) => call(left).await.map(|id| garble(&id)),
                Injected::Raise => Err(DispatchFailure::NotDispatched),
                Injected::TimedOut => Err(DispatchFailure::Unknown),
            }
        };
        match self.breaker.call(attempt).await {
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
