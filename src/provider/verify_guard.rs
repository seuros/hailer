//! Address verification policy wraps any backend, the same shape as
//! [`crate::provider::guard::SuppressionGuard`]. A verifier call failure (or
//! [`crate::verify::State::Unknown`]) fails open by default - this guard
//! never becomes a new reason real mail stops sending, unless a deployment
//! explicitly configures `Unknown` into `reject_states` too.
//!
//! Lookups share the request's budget minus [`SEND_RESERVE`]; one that cannot
//! finish inside it is `Unknown`.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use deadline_machines::Deadline;
use tracing::warn;

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};
use crate::rogue::{Experiments, Injected, Target};
use crate::state::Store;
use crate::verify::{AddressVerifier, State, Verdict};

/// Budget no verification may touch, kept for the send behind it.
pub const SEND_RESERVE: Duration = Duration::from_secs(5);
/// Less budget than this and a lookup is skipped instead of started.
const LOOKUP_FLOOR: Duration = Duration::from_secs(1);

pub struct EmailVerificationGuard<P, V> {
    pub inner: Arc<P>,
    pub verifier: Arc<V>,
    pub store: Store,
    pub reject_states: Vec<State>,
    pub cache_ttl: Duration,
    pub experiments: Arc<Experiments>,
}

impl<P: MailProvider, V: AddressVerifier> EmailVerificationGuard<P, V> {
    /// Cache hit skips the paid lookup entirely. A verifier failure (and an
    /// `Unknown` result) both fail open rather than block a real send on a
    /// flaky third party - see the module docs. Cache read/write failures
    /// also fail open, but are logged: a silently broken cache would mean
    /// paying for every lookup forever with no visibility into why.
    async fn verdict(&self, address: &str, deadline: Deadline) -> Verdict {
        match self.store.cached_verdict(address).await {
            Ok(Some(cached)) => return cached,
            Ok(None) => {}
            Err(error) => warn!(%error, "verification cache read failed"),
        }
        let Some(wait) = crate::budget::within(deadline, LOOKUP_FLOOR, Duration::MAX) else {
            return Verdict::unknown("verification budget spent");
        };
        let wait = match self.experiments.inject(Target::Verifier, wait).await {
            Injected::Pass(left) | Injected::Garble(left) => left,
            Injected::Raise => return Verdict::unknown("verification call failed"),
            Injected::TimedOut => return Verdict::unknown("verification budget spent"),
        };
        match tokio::time::timeout(wait, self.verifier.verify(address)).await {
            Err(_) => Verdict::unknown("verification budget spent"),
            Ok(Ok(verdict)) => {
                if let Err(error) =
                    self.store.cache_verdict(address, &verdict, self.cache_ttl).await
                {
                    warn!(%error, "verification cache write failed");
                }
                verdict
            }
            Ok(Err(_)) => Verdict::unknown("verification call failed"),
        }
    }
}

impl<P: MailProvider, V: AddressVerifier> MailProvider for EmailVerificationGuard<P, V> {
    async fn send(
        &self,
        request: &SendRequest,
        deadline: Deadline,
    ) -> Result<String, ProviderError> {
        let lookups = crate::budget::leaving(deadline, SEND_RESERVE);
        let mut filtered = request.clone();
        for group in
            [&mut filtered.to_addresses, &mut filtered.cc_addresses, &mut filtered.bcc_addresses]
        {
            let mut kept = Vec::with_capacity(group.len());
            for address in group.drain(..) {
                let verdict = self.verdict(&address, lookups).await;
                if !self.reject_states.contains(&verdict.state) {
                    kept.push(address);
                }
            }
            *group = kept;
        }
        if filtered.recipients().next().is_none() {
            return Err(ProviderError::Suppressed);
        }
        self.inner.send(&filtered, deadline).await
    }

    fn circuit_state(&self) -> &'static str {
        self.inner.circuit_state()
    }
}
