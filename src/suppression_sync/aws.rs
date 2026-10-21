use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_sesv2::error::ProvideErrorMetadata;
use aws_sdk_sesv2::types::SuppressionListReason;

use super::{Page, ReadError, Source};
use crate::feedback::SuppressionReason;
use crate::state::Suppression;

pub struct AwsSuppressions {
    client: aws_sdk_sesv2::Client,
    scope: String,
    account: String,
    region: String,
}

impl AwsSuppressions {
    pub async fn new(config: &aws_config::SdkConfig) -> anyhow::Result<Self> {
        let region = config
            .region()
            .ok_or_else(|| anyhow::anyhow!("AWS region is required"))?
            .as_ref()
            .to_owned();
        let identity = tokio::time::timeout(
            Duration::from_secs(30),
            aws_sdk_sts::Client::new(config).get_caller_identity().send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("AWS identity lookup timed out"))?
        .map_err(|_| anyhow::anyhow!("AWS identity lookup failed"))?;
        let account =
            identity.account().ok_or_else(|| anyhow::anyhow!("AWS account missing"))?.to_owned();
        anyhow::ensure!(
            account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit()),
            "invalid AWS account"
        );
        let client_config = aws_sdk_sesv2::config::Builder::from(config)
            .retry_config(aws_sdk_sesv2::config::retry::RetryConfig::disabled())
            .build();
        Ok(Self {
            client: aws_sdk_sesv2::Client::from_conf(client_config),
            scope: format!("ses:{account}:{region}"),
            account,
            region,
        })
    }
}

#[async_trait]
impl Source for AwsSuppressions {
    fn scope(&self) -> &str {
        &self.scope
    }

    fn page_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn page(&self, next_token: Option<&str>) -> Result<Page, ReadError> {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            self.client
                .list_suppressed_destinations()
                .page_size(1000)
                .set_next_token(next_token.map(str::to_owned))
                .send(),
        )
        .await
        .map_err(|_| ReadError::Unavailable)?
        .map_err(|error| {
            let service = error.as_service_error();
            if service.is_some_and(aws_sdk_sesv2::operation::list_suppressed_destinations::ListSuppressedDestinationsError::is_invalid_next_token_exception) {
                ReadError::InvalidToken
            } else if service.is_some_and(|error| {
                matches!(
                    error.code(),
                    Some(
                        "AccessDeniedException"
                            | "AccessDenied"
                            | "UnrecognizedClientException"
                            | "InvalidClientTokenId"
                            | "BadRequestException"
                    )
                )
            }) {
                ReadError::Rejected
            } else {
                ReadError::Unavailable
            }
        })?;
        let records = output.suppressed_destination_summaries().iter().map(|entry| {
            let reason = match entry.reason() {
                SuppressionListReason::Bounce => SuppressionReason::Bounce,
                SuppressionListReason::Complaint => SuppressionReason::Complaint,
                _ => return Err(ReadError::InvalidPage),
            };
            let timestamp = entry.last_update_time();
            let suppressed_at = jiff::Timestamp::new(timestamp.secs(), i32::try_from(timestamp.subsec_nanos()).map_err(|_| ReadError::InvalidPage)?)
                .map_err(|_| ReadError::InvalidPage)?.to_string();
            Ok(Suppression {
                email_address: entry.email_address().to_owned(),
                reason,
                suppressed_at,
                originating_message_id: None,
                source_event_id: None,
                metadata: serde_json::json!({"source": "aws_ses_account", "account_id": self.account, "region": self.region}),
            })
        }).collect::<Result<Vec<_>, ReadError>>()?;
        Ok(Page { records, next_token: output.next_token().map(str::to_owned) })
    }
}
