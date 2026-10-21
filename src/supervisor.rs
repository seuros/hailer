//! Cooperative stop first; bounded task cancellation only at the deadline.

use std::sync::Arc;
use std::time::Duration;

use chrono_machines::Policy;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{Config, DockingConfig};
use crate::control::Control;
use crate::provider::MailProvider;
use crate::runtime::Runtime;
use crate::state::Store;

pub async fn run(
    docking: DockingConfig,
    config: Config,
    aws: aws_config::SdkConfig,
    provider: Arc<dyn MailProvider>,
    store: Store,
    control: Arc<Control>,
) -> anyhow::Result<()> {
    let runtime =
        Runtime::new(control, config.feedback.is_some(), config.suppression_sync.is_some());
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(crate::bay::serve(docking, provider, store.clone(), runtime.clone(), stop.clone()));
    if let Some(feedback) = config.feedback {
        let queue = crate::feedback::AwsQueue::new(&aws, feedback.queue_url.clone());
        let (store, runtime, stop) = (store.clone(), runtime.clone(), stop.clone());
        tasks.spawn(
            async move { crate::feedback::run(&queue, store, &feedback, &runtime, stop).await },
        );
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

#[cfg(test)]
mod tests;
