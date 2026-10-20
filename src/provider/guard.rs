//! Suppression policy wraps any backend. A state failure fails closed.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};
use crate::state::{Store, normalize};

pub struct SuppressionGuard {
    pub inner: Arc<dyn MailProvider>,
    pub store: Store,
}

#[async_trait::async_trait]
impl MailProvider for SuppressionGuard {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        let addresses: Vec<_> = request.recipients().map(str::to_string).collect();
        let store = self.store.clone();
        let blocked = tokio::task::spawn_blocking(move || store.blocked(&addresses))
            .await
            .map_err(|_| ProviderError::Transport("suppression lookup task failed".into()))?
            .map_err(|_| ProviderError::Transport("suppression state unavailable".into()))?;
        if blocked.is_empty() {
            return self.inner.send(request).await;
        }
        let mut filtered = request.clone();
        for group in
            [&mut filtered.to_addresses, &mut filtered.cc_addresses, &mut filtered.bcc_addresses]
        {
            group.retain(|address| !normalize(address).is_ok_and(|key| blocked.contains(&key)));
        }
        if filtered.recipients().next().is_none() {
            return Err(ProviderError::Suppressed);
        }
        self.inner.send(&filtered).await
    }
}
