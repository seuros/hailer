use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::FeedbackEvent;
use crate::aws::Aws;
use crate::config::FeedbackConfig;
use crate::runtime::Runtime;
use crate::state::Store;

pub struct QueuedMessage {
    pub receipt: String,
    pub body: String,
}

/// Statically dispatched queue operations. The future itself must be sendable
/// because the supervisor moves the consumer into a Tokio task.
pub trait FeedbackQueue: Send + Sync {
    fn receive(&self) -> impl Future<Output = anyhow::Result<Vec<QueuedMessage>>> + Send;
    fn delete(&self, receipt: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct AwsQueue {
    aws: Arc<Aws>,
    url: String,
}

impl AwsQueue {
    pub const fn new(aws: Arc<Aws>, url: String) -> Self {
        Self { aws, url }
    }
}

impl FeedbackQueue for AwsQueue {
    async fn receive(&self) -> anyhow::Result<Vec<QueuedMessage>> {
        crate::aws::sqs::receive(&self.aws, &self.url)
            .await?
            .into_iter()
            .map(|message| {
                Ok(QueuedMessage {
                    receipt: message.receipt_handle.context("SQS message missing receipt")?,
                    body: message.body.context("SQS message missing body")?,
                })
            })
            .collect()
    }

    async fn delete(&self, receipt: &str) -> anyhow::Result<()> {
        crate::aws::sqs::delete(&self.aws, &self.url, receipt).await?;
        Ok(())
    }
}

/// Return only after the state commit and the SQS acknowledgement both succeed.
pub async fn process(
    queue: &impl FeedbackQueue,
    store: &Store,
    topic: &str,
    message: QueuedMessage,
) -> anyhow::Result<()> {
    let event = FeedbackEvent::from_sns(&message.body, topic)?;
    let event_id = event.event_id.clone();
    let inserted = store.record(&event).await?;
    queue.delete(&message.receipt).await?;
    info!(%event_id, inserted, "SES feedback committed and acknowledged");
    Ok(())
}

pub async fn run(
    queue: &impl FeedbackQueue,
    store: Store,
    config: &FeedbackConfig,
    runtime: &Runtime,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let backoff = chrono_machines::Policy {
        max_attempts: u8::MAX,
        base_delay_ms: 1000,
        multiplier: 2.0,
        max_delay_ms: 30_000,
    };
    let mut failures = 0u8;
    loop {
        let received = tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(()),
            result = queue.receive() => result,
        };
        if let Ok(messages) = received {
            failures = 0;
            runtime.feedback_ok();
            for message in messages {
                if stop.is_cancelled() {
                    return Ok(());
                }
                // Invalid/untrusted messages remain unacknowledged for the
                // queue's configured dead-letter policy. Never log bodies.
                if process(queue, &store, &config.topic_arn, message).await.is_err() {
                    runtime.feedback_error();
                    warn!("Feedback not acknowledged; SQS will retry");
                } else {
                    runtime.acknowledged();
                }
            }
        } else {
            runtime.feedback_error();
            failures = failures.saturating_add(1);
            warn!("SQS receive failed");
            tokio::select! {
                () = stop.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(backoff.calculate_delay(failures, 0.25))) => {}
            }
        }
    }
}
