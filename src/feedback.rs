//! SES feedback is consumed over authenticated SQS, not a public SNS webhook.

mod event;
mod queue;

#[cfg(test)]
pub mod test_support;
#[cfg(test)]
mod tests;

pub use event::FeedbackEvent;
pub use queue::{AwsQueue, run};

use serde::{Deserialize, Serialize};

/// Provider-independent suppression policy: permanent bounces and complaints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuppressionReason {
    Bounce,
    Complaint,
}
