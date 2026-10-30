//! SQS over its JSON protocol: the feedback queue's receive and delete.

use std::time::Duration;

use serde::Deserialize;

use super::{Aws, AwsError, Call};

/// Above the 20 second long poll.
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Message {
    pub receipt_handle: Option<String>,
    pub body: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Received {
    #[serde(default)]
    messages: Vec<Message>,
}

fn call(target: &'static str, body: &serde_json::Value) -> Call {
    Call {
        service: "sqs",
        prefix: "sqs",
        method: "POST",
        path: "/".into(),
        query: vec![],
        content_type: Some("application/x-amz-json-1.0"),
        target: Some(target),
        body: body.to_string().into_bytes(),
        timeout: TIMEOUT,
    }
}

pub async fn receive(aws: &Aws, queue_url: &str) -> Result<Vec<Message>, AwsError> {
    let body = serde_json::json!({
        "QueueUrl": queue_url,
        "MaxNumberOfMessages": 10,
        "WaitTimeSeconds": 20,
        "VisibilityTimeout": 60,
    });
    let received: Received =
        aws.read(call("AmazonSQS.ReceiveMessage", &body), "ReceiveMessage").await?;
    Ok(received.messages)
}

pub async fn delete(aws: &Aws, queue_url: &str, receipt: &str) -> Result<(), AwsError> {
    let body = serde_json::json!({"QueueUrl": queue_url, "ReceiptHandle": receipt});
    let _: serde_json::Value =
        aws.read(call("AmazonSQS.DeleteMessage", &body), "DeleteMessage").await?;
    Ok(())
}
