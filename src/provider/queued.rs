//! Positive evidence of no acceptance is required for automatic retries.

use std::future::Future;

use crate::mail::SendRequest;
use crate::outbound::Correlation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    Accepted(String),
    NotDispatched(&'static str),
    NotAccepted { code: &'static str, retryable: bool },
    Unknown,
}

pub trait QueuedProvider: super::MailProvider {
    fn dispatch(
        &self,
        request: &SendRequest,
        correlation: &Correlation,
    ) -> impl Future<Output = DispatchOutcome> + Send;
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DispatchFailure {
    #[error("provider definitely rejected the message")]
    Rejected,
    #[error("provider definitely did not accept the throttled message")]
    Throttled,
    #[error("request definitely did not reach the provider")]
    NotDispatched,
    #[error("provider outcome unknown")]
    Unknown,
}

impl DispatchFailure {
    pub const fn counts_as_failure(&self) -> bool {
        matches!(self, Self::Throttled | Self::Unknown)
    }

    pub const fn outcome(&self) -> DispatchOutcome {
        match self {
            Self::Rejected => {
                DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
            }
            Self::Throttled => {
                DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
            }
            Self::NotDispatched => DispatchOutcome::NotDispatched("request_not_dispatched"),
            Self::Unknown => DispatchOutcome::Unknown,
        }
    }
}
