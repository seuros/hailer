//! The SES-side workers: SQS feedback, account rate discovery and
//! suppression reconciliation. All of them share one AWS config.

use std::sync::Arc;
use std::time::Duration;

use chrono_machines::Policy;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::Config;
use crate::control::Control;
use crate::runtime::Runtime;
use crate::state::Store;

pub async fn spawn(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    config: &Config,
    store: &Store,
    runtime: &Arc<Runtime>,
    stop: &CancellationToken,
) {
    let discovery = config.rate_limit.as_ref().filter(|rate| rate.discover_from_aws);
    if config.feedback.is_none() && discovery.is_none() && config.suppression_sync.is_none() {
        return;
    }
    let aws = crate::aws_client::load_config().await;
    if let Some(feedback) = config.feedback.clone() {
        let queue = crate::feedback::AwsQueue::new(&aws, feedback.queue_url.clone());
        let (store, runtime, stop) = (store.clone(), runtime.clone(), stop.clone());
        tasks.spawn(
            async move { crate::feedback::run(&queue, store, &feedback, &runtime, stop).await },
        );
    }
    if let Some(rate) = discovery {
        tasks.spawn(discover_rate(
            aws.clone(),
            runtime.control.clone(),
            Duration::from_secs(rate.discovery_interval_seconds),
            stop.clone(),
        ));
    }
    if let Some(sync) = &config.suppression_sync {
        tasks.spawn(reconcile(
            aws,
            store.clone(),
            runtime.clone(),
            Duration::from_secs(sync.interval_seconds),
            stop.clone(),
        ));
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
