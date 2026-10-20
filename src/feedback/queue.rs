use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use tracing::{info, warn};

use super::FeedbackEvent;
use crate::config::FeedbackConfig;
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
) -> anyhow::Result<()> {
    loop {
        match queue.receive().await {
            Ok(messages) => {
                for message in messages {
                    // Invalid/untrusted messages remain unacknowledged for the
                    // queue's configured dead-letter policy. Never log bodies.
                    if let Err(error) = process(queue, &store, &config.topic_arn, message).await {
                        warn!(%error, "Feedback not acknowledged; SQS will retry");
                    }
                }
            }
            Err(error) => {
                warn!(%error, "SQS receive failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
