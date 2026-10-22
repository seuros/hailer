use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::SuppressionReason;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Delivered,
    Bounced,
    Complained,
    Rejected,
    RenderingFailed,
    Delayed,
    Sent,
    Opened,
    Clicked,
}

/// No SNS envelope, AWS subscriptions, or SES-shaped payload in the app API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedbackEvent {
    /// Internal correlation only; never change the legacy feedback wire schema.
    #[serde(skip)]
    pub correlation: Option<crate::outbound::Correlation>,
    pub event_id: String,
    pub kind: EventKind,
    pub provider: String,
    pub provider_message_id: String,
    pub message_id: Option<String>,
    pub occurred_at: String,
    pub recipients: Vec<String>,
    pub suppression: Option<SuppressionReason>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Notification {
    #[serde(rename = "Type")]
    kind: String,
    topic_arn: String,
    message_id: String,
    message: String,
}

impl FeedbackEvent {
    /// The topic allowlist is defense in depth. The SQS queue policy must also
    /// allow only this SNS topic; anyone able to `SendMessage` can forge an envelope.
    pub fn from_sns(body: &str, allowed_topic: &str) -> anyhow::Result<Self> {
        let notification: Notification =
            serde_json::from_str(body).context("invalid SNS envelope")?;
        ensure!(notification.kind == "Notification", "not an SNS notification");
        ensure!(notification.topic_arn == allowed_topic, "unexpected SNS topic");
        ensure!(!notification.message_id.trim().is_empty(), "missing SNS message id");
        let event: Value =
            serde_json::from_str(&notification.message).context("invalid SES feedback")?;
        let event_type = str_or(&event["eventType"], &event["notificationType"])
            .context("missing SES event type")?;
        let (kind, detail_name, recipients_name) = match event_type {
            "Delivery" => (EventKind::Delivered, "delivery", Some("recipients")),
            "Bounce" => (EventKind::Bounced, "bounce", Some("bouncedRecipients")),
            "Complaint" => (EventKind::Complained, "complaint", Some("complainedRecipients")),
            "Reject" => (EventKind::Rejected, "reject", None),
            "Rendering Failure" => (EventKind::RenderingFailed, "failure", None),
            "DeliveryDelay" => (EventKind::Delayed, "deliveryDelay", Some("delayedRecipients")),
            "Send" => (EventKind::Sent, "send", None),
            "Open" => (EventKind::Opened, "open", None),
            "Click" => (EventKind::Clicked, "click", None),
            _ => anyhow::bail!("unsupported SES event type"),
        };
        let detail = &event[detail_name];
        ensure!(detail.is_object(), "missing SES event detail");
        let mail = &event["mail"];
        let provider_message_id = string(&mail["messageId"], "missing SES message id")?;
        let timestamp = str_or(&detail["timestamp"], &mail["timestamp"]);
        let occurred_at = timestamp
            .context("missing SES event timestamp")?
            .parse::<jiff::Timestamp>()
            .context("invalid SES event timestamp")?
            .to_string();
        let addresses = recipients_name.map_or_else(|| &mail["destination"], |name| &detail[name]);
        let recipients = addresses
            .as_array()
            .context("missing SES event recipients")?
            .iter()
            .map(|recipient| {
                let address = str_or(recipient, &recipient["emailAddress"]);
                let address = address.context("missing recipient address")?;
                ensure!(
                    email_address::EmailAddress::is_valid(address),
                    "invalid feedback recipient"
                );
                Ok(address.to_string())
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        ensure!(!recipients.is_empty() && recipients.len() <= 50, "invalid SES recipient count");
        if kind == EventKind::Bounced {
            ensure!(
                matches!(
                    detail["bounceType"].as_str(),
                    Some("Permanent" | "Transient" | "Undetermined")
                ),
                "missing or unsupported bounce type"
            );
        }

        let suppression = match kind {
            EventKind::Bounced if detail["bounceType"] == "Permanent" => {
                Some(SuppressionReason::Bounce)
            }
            EventKind::Complained if detail["complaintFeedbackType"] != "not-spam" => {
                Some(SuppressionReason::Complaint)
            }
            _ => None,
        };
        let message_id = mail["headers"]
            .as_array()
            .and_then(|headers| {
                headers.iter().find_map(|header| {
                    let name = header["name"].as_str()?;
                    name.eq_ignore_ascii_case("message-id")
                        .then(|| header["value"].as_str().map(str::to_string))
                        .flatten()
                })
            })
            .or_else(|| mail["commonHeaders"]["messageId"].as_str().map(str::to_string));

        Ok(Self {
            correlation: correlation(&mail["tags"]),
            event_id: format!("{}:{}", notification.topic_arn, notification.message_id),
            kind,
            provider: "ses".into(),
            provider_message_id,
            message_id,
            occurred_at,
            recipients,
            suppression,
        })
    }
}

fn correlation(tags: &Value) -> Option<crate::outbound::Correlation> {
    let unique = |name: &str| {
        let values = tags[name].as_array()?;
        if values.len() != 1 {
            return None;
        }
        let value = values[0].as_str()?;
        crate::outbound::valid_id(value).then(|| value.to_owned())
    };
    Some(crate::outbound::Correlation {
        hailer_message_id: unique("hailer_message_id")?,
        attempt_id: unique("hailer_attempt_id")?,
    })
}

/// SES/SNS reports some fields at one of two locations depending on event
/// type or library version (event type, timestamps, recipient shape).
/// Reads `primary` as a string, falling back to `fallback` when absent.
fn str_or<'a>(primary: &'a Value, fallback: &'a Value) -> Option<&'a str> {
    primary.as_str().or_else(|| fallback.as_str())
}

fn string(value: &Value, message: &'static str) -> anyhow::Result<String> {
    let text = value.as_str().context(message)?;
    ensure!(!text.trim().is_empty(), "{message}");
    Ok(text.to_string())
}
