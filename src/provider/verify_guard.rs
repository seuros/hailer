//! Address verification policy wraps any backend, the same shape as
//! [`crate::provider::guard::SuppressionGuard`]. A verifier call failure (or
//! [`crate::verify::State::Unknown`]) fails open by default - this guard
//! never becomes a new reason real mail stops sending, unless a deployment
//! explicitly configures `Unknown` into `reject_states` too.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};
use crate::state::Store;
use crate::verify::{AddressVerifier, State, Verdict};

pub struct EmailVerificationGuard<P, V> {
    pub inner: Arc<P>,
    pub verifier: Arc<V>,
    pub store: Store,
    pub reject_states: Vec<State>,
    pub cache_ttl: Duration,
}

impl<P: MailProvider, V: AddressVerifier> EmailVerificationGuard<P, V> {
    /// Cache hit skips the paid lookup entirely. A verifier failure (and an
    /// `Unknown` result) both fail open rather than block a real send on a
    /// flaky third party - see the module docs. Cache read/write failures
    /// also fail open, but are logged: a silently broken cache would mean
    /// paying for every lookup forever with no visibility into why.
    async fn verdict(&self, address: &str) -> Verdict {
        match self.store.cached_verdict(address).await {
            Ok(Some(cached)) => return cached,
            Ok(None) => {}
            Err(error) => warn!(%error, "verification cache read failed"),
        }
        match self.verifier.verify(address).await {
            Ok(verdict) => {
                if let Err(error) =
                    self.store.cache_verdict(address, &verdict, self.cache_ttl).await
                {
                    warn!(%error, "verification cache write failed");
                }
                verdict
            }
            Err(_) => Verdict::unknown("verification call failed"),
        }
    }
}

impl<P: MailProvider, V: AddressVerifier> MailProvider for EmailVerificationGuard<P, V> {
    async fn send(&self, request: &SendRequest) -> Result<String, ProviderError> {
        let mut filtered = request.clone();
        for group in
            [&mut filtered.to_addresses, &mut filtered.cc_addresses, &mut filtered.bcc_addresses]
        {
            let mut kept = Vec::with_capacity(group.len());
            for address in group.drain(..) {
                let verdict = self.verdict(&address).await;
                if !self.reject_states.contains(&verdict.state) {
                    kept.push(address);
                }
            }
            *group = kept;
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
