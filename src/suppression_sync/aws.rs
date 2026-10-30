use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::{Page, ReadError, Source};
use crate::aws::{Aws, ses, sts};
use crate::feedback::SuppressionReason;
use crate::state::Suppression;

pub struct AwsSuppressions {
    aws: Arc<Aws>,
    scope: String,
    account: String,
    region: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListPage {
    #[serde(default)]
    suppressed_destination_summaries: Vec<Summary>,
    next_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Summary {
    email_address: String,
    reason: String,
    /// Epoch seconds in REST-JSON; an RFC 3339 string is accepted too.
    last_update_time: serde_json::Value,
}

impl AwsSuppressions {
    pub async fn new(aws: Arc<Aws>) -> anyhow::Result<Self> {
        let account =
            sts::account(&aws).await.map_err(|_| anyhow::anyhow!("AWS identity lookup failed"))?;
        anyhow::ensure!(
            account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit()),
            "invalid AWS account"
        );
        let region = aws.region().to_owned();
        Ok(Self { aws, scope: format!("ses:{account}:{region}"), account, region })
    }
}

fn suppressed_at(value: &serde_json::Value) -> Result<String, ReadError> {
    let timestamp = match value {
        serde_json::Value::Number(seconds) => seconds
            .as_f64()
            .and_then(|seconds| jiff::SignedDuration::try_from_secs_f64(seconds).ok())
            .and_then(|since| jiff::Timestamp::from_duration(since).ok()),
        serde_json::Value::String(text) => text.parse().ok(),
        _ => None,
    };
    timestamp.map(|at| at.to_string()).ok_or(ReadError::InvalidPage)
}

impl Source for AwsSuppressions {
    fn scope(&self) -> &str {
        &self.scope
    }

    fn page_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn page(&self, next_token: Option<&str>) -> Result<Page, ReadError> {
        let response = ses::suppressed_destinations(&self.aws, next_token)
            .await
            .map_err(|_| ReadError::Unavailable)?;
        if !response.is_success() {
            return Err(match response.code().as_deref() {
                Some("InvalidNextTokenException") => ReadError::InvalidToken,
                Some(
                    "AccessDeniedException"
                    | "AccessDenied"
                    | "UnrecognizedClientException"
                    | "InvalidClientTokenId"
                    | "BadRequestException",
                ) => ReadError::Rejected,
                _ => ReadError::Unavailable,
            });
        }
        let page: ListPage = response.json().ok_or(ReadError::InvalidPage)?;
        let records = page
            .suppressed_destination_summaries
            .into_iter()
            .map(|entry| {
                let reason = match entry.reason.as_str() {
                    "BOUNCE" => SuppressionReason::Bounce,
                    "COMPLAINT" => SuppressionReason::Complaint,
                    _ => return Err(ReadError::InvalidPage),
                };
                Ok(Suppression {
                    email_address: entry.email_address,
                    reason,
                    suppressed_at: suppressed_at(&entry.last_update_time)?,
                    originating_message_id: None,
                    source_event_id: None,
                    metadata: serde_json::json!({"source": "aws_ses_account", "account_id": self.account, "region": self.region}),
                })
            })
            .collect::<Result<Vec<_>, ReadError>>()?;
        Ok(Page { records, next_token: page.next_token })
    }
}
