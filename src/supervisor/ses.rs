//! The SES-side workers: SQS feedback, account rate discovery and
//! suppression reconciliation. All of them share one AWS config.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::Worker;
use crate::aws::Aws;
use crate::backoff::{Backoff, pause};
use crate::config::Config;
use crate::control::Control;
use crate::rogue::Experiments;
use crate::runtime::Runtime;
use crate::state::Store;

pub async fn workers(
    config: &Config,
    store: &Store,
    runtime: &Arc<Runtime>,
    experiments: &Arc<Experiments>,
    stop: &CancellationToken,
) -> anyhow::Result<Vec<Worker>> {
    let discovery = config.rate_limit.as_ref().filter(|rate| rate.discover_from_aws);
    if config.feedback.is_none() && discovery.is_none() && config.suppression_sync.is_none() {
        return Ok(Vec::new());
    }
    let aws = Arc::new(Aws::load().await?);
    let mut workers = Vec::new();
    if let Some(feedback) = config.feedback.clone() {
        let (aws, store, runtime, stop) =
            (aws.clone(), store.clone(), runtime.clone(), stop.clone());
        let experiments = experiments.clone();
        workers.push(Worker::new("feedback", move || {
            let queue = crate::feedback::AwsQueue::new(
                aws.clone(),
                feedback.queue_url.clone(),
                experiments.clone(),
            );
            let (feedback, store, runtime, stop) =
                (feedback.clone(), store.clone(), runtime.clone(), stop.clone());
            async move { crate::feedback::run(&queue, store, &feedback, &runtime, stop).await }
        }));
    }
    if let Some(rate) = discovery {
        let interval = Duration::from_secs(rate.discovery_interval_seconds);
        let (aws, control, stop) = (aws.clone(), runtime.control.clone(), stop.clone());
        workers.push(Worker::new("rate_discovery", move || {
            discover_rate(aws.clone(), control.clone(), interval, stop.clone())
        }));
    }
    if let Some(sync) = &config.suppression_sync {
        let interval = Duration::from_secs(sync.interval_seconds);
        let (store, runtime, stop) = (store.clone(), runtime.clone(), stop.clone());
        workers.push(Worker::new("suppression_sync", move || {
            reconcile(aws.clone(), store.clone(), runtime.clone(), interval, stop.clone())
        }));
    }
    Ok(workers)
}

async fn reconcile(
    aws: Arc<Aws>,
    store: Store,
    runtime: Arc<Runtime>,
    interval: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff = Backoff::new();
    loop {
        let operation = async {
            let source = crate::suppression_sync::AwsSuppressions::new(aws.clone()).await?;
            crate::suppression_sync::run(&source, &store, false).await
        };
        let result = tokio::select! {
            () = stop.cancelled() => return Ok(()),
            result = operation => result,
        };
        let delay = if let Ok(report) = result {
            backoff.succeeded();
            runtime.sync_ok(report.merged);
            info!(
                pages = report.pages,
                merged = report.merged,
                "AWS suppression reconciliation complete"
            );
            interval
        } else {
            runtime.sync_error();
            warn!("AWS suppression reconciliation failed; checkpoint retained");
            backoff.failed()
        };
        if !pause(&stop, delay).await {
            return Ok(());
        }
    }
}

async fn discover_rate(
    aws: Arc<Aws>,
    control: Arc<Control>,
    interval: Duration,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff = Backoff::new();
    loop {
        let operation = crate::aws::ses::account(&aws);
        let result = tokio::select! {
            () = stop.cancelled() => return Ok(()),
            result = operation => result,
        };
        let delay = match result
            .ok()
            .and_then(|account| account.send_quota)
            .and_then(|quota| quota.max_send_rate)
        {
            Some(rate) if rate > 0.0 => {
                backoff.succeeded();
                if let Err(error) = control.set_rate(rate).await {
                    warn!(%error, "could not persist discovered SES send rate");
                } else {
                    info!(recipients_per_second = rate, "discovered SES send rate from AWS");
                }
                interval
            }
            _ => {
                warn!("SES account rate discovery failed; keeping the last known rate");
                backoff.failed()
            }
        };
        if !pause(&stop, delay).await {
            return Ok(());
        }
    }
}
