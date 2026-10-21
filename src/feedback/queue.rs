use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::FeedbackEvent;
use crate::config::FeedbackConfig;
use crate::runtime::Runtime;
use crate::state::Store;

pub struct QueuedMessage {
    pub receipt: String,
    pub body: String,
}

#[async_trait]
pub trait FeedbackQueue: Send + Sync {
    async fn receive(&self) -> anyhow::Result<Vec<QueuedMessage>>;
    async fn delete(&self, receipt: &str) -> anyhow::Result<()>;
}

pub struct AwsQueue {
    client: aws_sdk_sqs::Client,
    url: String,
}

impl AwsQueue {
    pub fn new(config: &aws_config::SdkConfig, url: String) -> Self {
        Self { client: aws_sdk_sqs::Client::new(config), url }
    }
}

#[async_trait]
impl FeedbackQueue for AwsQueue {
    async fn receive(&self) -> anyhow::Result<Vec<QueuedMessage>> {
        let output = self
            .client
            .receive_message()
            .queue_url(&self.url)
            .max_number_of_messages(10)
            .wait_time_seconds(20)
            .visibility_timeout(60)
            .send()
            .await?;
        output
            .messages()
            .iter()
            .map(|message| {
                Ok(QueuedMessage {
                    receipt: message
                        .receipt_handle()
                        .context("SQS message missing receipt")?
                        .into(),
                    body: message.body().context("SQS message missing body")?.into(),
                })
            })
            .collect()
    }

    async fn delete(&self, receipt: &str) -> anyhow::Result<()> {
        self.client.delete_message().queue_url(&self.url).receipt_handle(receipt).send().await?;
        Ok(())
    }
}

/// Return only after the state commit and the SQS acknowledgement both succeed.
pub async fn process(
    queue: &dyn FeedbackQueue,
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
    queue: &dyn FeedbackQueue,
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
