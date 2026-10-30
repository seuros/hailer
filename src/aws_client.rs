//! Shared `aws_sdk_sesv2` client construction. Hailer owns its own
//! retry/backoff policy (`chrono_machines`, via `breaker_machines`) and never
//! wants the SDK silently retrying underneath it, so every SES client built
//! anywhere in this crate disables the SDK's own retries.

use std::time::Duration;

/// `(connect, operation)` bounds for SES calls made on a request path.
pub const TIMEOUT: (Duration, Duration) = (Duration::from_secs(3), Duration::from_secs(20));

pub async fn load_config() -> aws_config::SdkConfig {
    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
}

/// Builds an SES client with the SDK's retries disabled. `timeout`, when
/// given as `(connect, operation)`, additionally bounds connection
/// establishment and per-operation time. Callers that already wrap every
/// call in their own `tokio::time::timeout` (suppression sync's background
/// paging) pass `None` instead of layering a second deadline.
pub fn ses_client(
    config: &aws_config::SdkConfig,
    timeout: Option<(Duration, Duration)>,
) -> aws_sdk_sesv2::Client {
    let mut builder = aws_sdk_sesv2::config::Builder::from(config)
        .retry_config(aws_sdk_sesv2::config::retry::RetryConfig::disabled());
    if let Some((connect, operation)) = timeout {
        builder = builder.timeout_config(
            aws_sdk_sesv2::config::timeout::TimeoutConfig::builder()
                .connect_timeout(connect)
                .operation_timeout(operation)
                .build(),
        );
    }
    aws_sdk_sesv2::Client::from_conf(builder.build())
}
