//! Domain/pattern blocklist policy wraps any backend, the same shape as
//! [`crate::provider::guard::SuppressionGuard`]. Always active (an empty
//! table is a no-op) - there is no "off" switch, matching suppression's own
//! unconditional wrap.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use tracing::warn;

use crate::domain_block;
use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};
use crate::state::Store;

pub struct DomainBlockGuard<P> {
    pub inner: Arc<P>,
    pub store: Store,
}

impl<P: MailProvider> MailProvider for DomainBlockGuard<P> {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        // A lookup failure fails open - the same policy as the verification
        // guard. A broken blocklist query should not stop real mail.
        let patterns = self.store.domain_block_patterns().await.unwrap_or_else(|error| {
            warn!(%error, "domain block lookup failed");
            Vec::new()
        });
        let mut filtered = request.clone();
        for group in
            [&mut filtered.to_addresses, &mut filtered.cc_addresses, &mut filtered.bcc_addresses]
        {
            group.retain(|address| {
                domain_block::domain_of(address)
                    .is_none_or(|domain| domain_block::blocking_reason(&patterns, domain).is_none())
            });
        }
        if filtered.recipients().next().is_none() {
            return Err(ProviderError::Suppressed);
        }
        self.inner.send(&filtered).await
    }

    fn circuit_state(&self) -> &'static str {
        self.inner.circuit_state()
    }
}
