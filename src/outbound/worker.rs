use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::backoff::{Backoff, pause};
use crate::provider::queued::{DispatchOutcome, QueuedProvider};
use crate::runtime::Runtime;
use crate::state::Store;

/// How long the dispatcher idles between cycles while nothing is claimable.
const IDLE: Duration = Duration::from_millis(250);

pub async fn run<P: QueuedProvider>(
    provider: Arc<P>,
    store: Store,
    runtime: Arc<Runtime>,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let retention = runtime
        .queued
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("dispatcher requires queued configuration"))?
        .unknown_retention_seconds;
    let mut backoff = Backoff::new();
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        let result = cycle(&*provider, &store, &runtime, &stop, retention).await;
        runtime.dispatch_state(result.is_ok());
        let delay = if result.is_ok() {
            backoff.succeeded();
            IDLE
        } else {
            warn!("Dispatcher operation failed; durable attempts retained for recovery");
            backoff.failed()
        };
        if !pause(&stop, delay).await {
            return Ok(());
        }
    }
}

async fn cycle<P: QueuedProvider>(
    provider: &P,
    store: &Store,
    runtime: &Runtime,
    stop: &CancellationToken,
    retention: u64,
) -> anyhow::Result<()> {
    store.maintain_messages(retention).await?;
    if !runtime.control.ready() || stop.is_cancelled() {
        return Ok(());
    }
    let Some(claim) = store.claim_message().await? else { return Ok(()) };
    let Some(ticket) = store.prepare_dispatch(&claim, &runtime.control).await? else {
        return Ok(());
    };
    let outcome = if stop.is_cancelled() || !store.dispatch_fence(&ticket).await? {
        // This host has positive local evidence: it has not invoked the adapter.
        DispatchOutcome::NotDispatched("dispatch_fence_closed")
    } else {
        // Never select cancellation against this irreversible effect. The
        // supervisor's bounded drain is the only forced cancellation boundary.
        provider.dispatch(&ticket.request, &ticket.correlation).await
    };
    store.finish_dispatch(&ticket.correlation, &outcome).await
}
