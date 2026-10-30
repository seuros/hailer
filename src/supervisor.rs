//! Cooperative stop first; bounded task cancellation only at the deadline.

use std::sync::Arc;
use std::time::Duration;

use chrono_machines::Policy;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

// One-time startup wiring, called from exactly one place (main.rs); grouping
// these into a struct would add indirection without a second call site to
// justify it.
#[allow(clippy::too_many_arguments)]
pub async fn run<P: QueuedProvider + 'static>(
    docking: DockingConfig,
    config: Config,
    aws: aws_config::SdkConfig,
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
        config.queued,
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
    if let Some(feedback) = config.feedback {
        let queue = crate::feedback::AwsQueue::new(&aws, feedback.queue_url.clone());
        let (store, runtime, stop) = (store.clone(), runtime.clone(), stop.clone());
        tasks.spawn(
            async move { crate::feedback::run(&queue, store, &feedback, &runtime, stop).await },
        );
    }
    if let Some(rate) = config.rate_limit.as_ref().filter(|rate| rate.discover_from_aws) {
        tasks.spawn(discover_rate(
            aws.clone(),
            runtime.control.clone(),
            Duration::from_secs(rate.discovery_interval_seconds),
            stop.clone(),
        ));
    }
    if let Some(sync) = config.suppression_sync {
        tasks.spawn(reconcile(
            aws,
            store,
            runtime.clone(),
            Duration::from_secs(sync.interval_seconds),
            stop.clone(),
        ));
    }
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

async fn reconcile(
    aws: aws_config::SdkConfig,
    store: Store,
    runtime: Arc<Runtime>,
    interval: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let backoff = Policy {
        max_attempts: u8::MAX,
        base_delay_ms: 1000,
        multiplier: 2.0,
        max_delay_ms: 30_000,
    };
    let mut failures = 0u8;
    loop {
        let operation = async {
            let source = crate::suppression_sync::AwsSuppressions::new(&aws).await?;
            crate::suppression_sync::run(&source, &store, false).await
        };
        let result = tokio::select! {
            () = stop.cancelled() => return Ok(()),
            result = operation => result,
        };
        let delay = if let Ok(report) = result {
            failures = 0;
            runtime.sync_ok(report.merged);
            info!(
                pages = report.pages,
                merged = report.merged,
                "AWS suppression reconciliation complete"
            );
            interval
        } else {
            runtime.sync_error();
            failures = failures.saturating_add(1);
            warn!("AWS suppression reconciliation failed; checkpoint retained");
            Duration::from_millis(backoff.calculate_delay(failures, 0.25))
        };
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn discover_rate(
    aws: aws_config::SdkConfig,
    control: Arc<Control>,
    interval: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let backoff = Policy {
        max_attempts: u8::MAX,
        base_delay_ms: 1000,
        multiplier: 2.0,
        max_delay_ms: 30_000,
    };
    let client = crate::aws_client::ses_client(&aws, Some(crate::aws_client::TIMEOUT));
    let mut failures = 0u8;
    loop {
        let operation = async { client.get_account().send().await };
        let result = tokio::select! {
            () = stop.cancelled() => return Ok(()),
            result = operation => result,
        };
        let delay =
            match result.ok().and_then(|output| output.send_quota).map(|quota| quota.max_send_rate)
            {
                Some(rate) if rate > 0.0 => {
                    failures = 0;
                    if let Err(error) = control.set_rate(rate).await {
                        warn!(%error, "could not persist discovered SES send rate");
                    } else {
                        info!(recipients_per_second = rate, "discovered SES send rate from AWS");
                    }
                    interval
                }
                _ => {
                    failures = failures.saturating_add(1);
                    warn!("SES account rate discovery failed; keeping the last known rate");
                    Duration::from_millis(backoff.calculate_delay(failures, 0.25))
                }
            };
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests;
