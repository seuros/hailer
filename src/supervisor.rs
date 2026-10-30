//! Cooperative stop first; bounded task cancellation only at the deadline.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{Config, DockingConfig};
use crate::control::Control;
use crate::domain_intel::Intel;
use crate::provider::domain_block_guard::DomainBlockGuard;
use crate::provider::guard::SuppressionGuard;
use crate::provider::queued::QueuedProvider;
use crate::provider::verify_guard::EmailVerificationGuard;
use crate::runtime::Runtime;
use crate::state::Store;
use crate::verify::Verifier;

#[cfg(feature = "ses")]
mod ses;

// One-time startup wiring, called from exactly one place (main.rs); grouping
// these into a struct would add indirection without a second call site to
// justify it.
#[allow(clippy::too_many_arguments)]
pub async fn run<P: QueuedProvider + 'static>(
    docking: DockingConfig,
    config: Config,
    provider: Arc<P>,
    store: Store,
    control: Arc<Control>,
    verifier: Arc<Verifier>,
    intel: Arc<Intel>,
) -> anyhow::Result<()> {
    let new_domain_threshold_days =
        config.domain_intel.as_ref().map(|intel| intel.new_domain_threshold_days);
    let runtime = Runtime::new(
        control,
        config.feedback.is_some(),
        config.suppression_sync.is_some(),
        config.queued.clone(),
        intel,
        new_domain_threshold_days,
    );
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::new();
    // Cheapest/broadest filters run first: domain blocklist (free, local,
    // whole-domain), then suppression (free, local, per-address), then paid
    // external verification only for recipients that survived both - never
    // pay to verify an address we were dropping anyway.
    let (reject_states, cache_ttl) = config.email_verification.as_ref().map_or_else(
        || (vec![crate::verify::State::Undeliverable], Duration::from_hours(168)),
        |verification| {
            (
                verification.reject_states.clone(),
                Duration::from_secs(verification.cache_ttl_seconds),
            )
        },
    );
    let email_verified = Arc::new(EmailVerificationGuard {
        inner: provider.clone(),
        verifier,
        store: store.clone(),
        reject_states,
        cache_ttl,
    });
    let suppressed = Arc::new(SuppressionGuard {
        inner: email_verified,
        store: store.clone(),
        control: runtime.control.clone(),
    });
    let guarded = Arc::new(DomainBlockGuard { inner: suppressed, store: store.clone() });
    tasks.spawn(crate::bay::serve(docking, guarded, store.clone(), runtime.clone(), stop.clone()));
    if runtime.queued.is_some() {
        tasks.spawn(crate::outbound::run(provider, store.clone(), runtime.clone(), stop.clone()));
    }
    #[cfg(feature = "ses")]
    ses::spawn(&mut tasks, &config, &store, &runtime, &stop).await;
    let unexpected = tokio::select! {
        () = crate::wait_for_signal() => false,
        _ = tasks.join_next() => true,
    };
    runtime.control.drain();
    stop.cancel();
    info!("Stopping admission and draining active operations");
    drain(&mut tasks, Duration::from_secs(config.shutdown_timeout_seconds)).await?;
    anyhow::ensure!(!unexpected, "a supervised worker exited unexpectedly");
    Ok(())
}

pub async fn drain(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    deadline: Duration,
) -> anyhow::Result<()> {
    let finished = tokio::time::timeout(deadline, async {
        let mut failed = false;
        while let Some(result) = tasks.join_next().await {
            failed |= !matches!(result, Ok(Ok(())));
        }
        failed
    })
    .await;
    match finished {
        Ok(false) => Ok(()),
        Ok(true) => anyhow::bail!("a worker failed while draining"),
        Err(_) => {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            anyhow::bail!("shutdown deadline elapsed; in-flight send outcomes may be unknown")
        }
    }
}

#[cfg(test)]
mod tests;
