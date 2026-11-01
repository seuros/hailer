//! Suppression policy wraps any backend. A state failure fails closed.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use deadline_machines::Deadline;

use crate::control::{Control, Decision, Snapshot};
use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};
use crate::state::{Store, normalize};

pub struct SuppressionGuard<P> {
    pub inner: Arc<P>,
    pub store: Store,
    pub control: Arc<Control>,
}

impl<P: MailProvider> MailProvider for SuppressionGuard<P> {
    async fn send(
        &self,
        request: &SendRequest,
        deadline: Deadline,
    ) -> Result<String, ProviderError> {
        let validation = request.validate();
        let mut snapshot = Snapshot {
            valid: validation.is_ok(),
            service_ready: true,
            storage_ready: true,
            recipients_allowed: true,
            rate_allowed: true,
        };
        match self.control.decide(snapshot) {
            Decision::Invalid => {
                return Err(validation.err().unwrap_or(ProviderError::InvalidRequest));
            }
            Decision::Unavailable => {
                return Err(ProviderError::Transport("service not ready".into()));
            }
            _ => {}
        }
        let addresses: Vec<_> = request.recipients().map(str::to_string).collect();
        let blocked = self.store.blocked(&addresses).await.map_err(|_| {
            snapshot.storage_ready = false;
            debug_assert_eq!(self.control.decide(snapshot), Decision::Unavailable);
            ProviderError::Transport("suppression state unavailable".into())
        })?;
        let mut filtered = request.clone();
        for group in
            [&mut filtered.to_addresses, &mut filtered.cc_addresses, &mut filtered.bcc_addresses]
        {
            group.retain(|address| !normalize(address).is_ok_and(|key| blocked.contains(&key)));
        }
        snapshot.recipients_allowed = filtered.recipients().next().is_some();
        snapshot.rate_allowed = snapshot.recipients_allowed
            && self.control.rate_admit(filtered.recipients().count()).await;
        match self.control.decide(snapshot) {
            Decision::Allow => self.inner.send(&filtered, deadline).await,
            Decision::Suppressed => Err(ProviderError::Suppressed),
            Decision::RateLimited => Err(ProviderError::RateLimited),
            Decision::Unavailable => Err(ProviderError::Transport("service not ready".into())),
            Decision::Invalid => Err(ProviderError::InvalidRequest),
        }
    }

    fn circuit_state(&self) -> &'static str {
        self.inner.circuit_state()
    }
}
