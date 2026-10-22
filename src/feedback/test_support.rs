use serde_json::{Value, json};

use super::FeedbackEvent;

pub const TOPIC: &str = "arn:aws:sns:us-east-1:123456789012:mail-feedback";
pub const ADDRESS: &str = "user+alias@example.dev";

pub fn ses(event_type: &str) -> Value {
    let mut event = json!({
        "eventType": event_type,
        "mail": {
            "messageId": "ses-message-1",
            "timestamp": "2026-10-20T12:00:00Z",
            "destination": [ADDRESS],
            "headers": [{"name": "Message-ID", "value": "<rails-message@example.dev>"}]
        }
    });
    let (key, detail) = match event_type {
        "Bounce" => (
            "bounce",
            json!({
                "bounceType": "Permanent", "bouncedRecipients": [{"emailAddress": ADDRESS}]
            }),
        ),
        "Complaint" => (
            "complaint",
            json!({
                "complaintFeedbackType": "abuse", "complainedRecipients": [{"emailAddress": ADDRESS}]
            }),
        ),
        "Delivery" => ("delivery", json!({"recipients": [ADDRESS]})),
        "DeliveryDelay" => {
            ("deliveryDelay", json!({"delayedRecipients": [{"emailAddress": ADDRESS}]}))
        }
        "Reject" => ("reject", json!({"reason": "Bad content"})),
        "Rendering Failure" => ("failure", json!({"errorMessage": "Template failed"})),
        "Open" => ("open", json!({})),
        "Click" => ("click", json!({})),
        _ => ("send", json!({})),
    };
    event[key] = detail;
    event
}

pub fn notification(id: &str, event: &Value) -> String {
    json!({
        "Type": "Notification",
        "TopicArn": TOPIC,
        "MessageId": id,
        "Message": event.to_string()
    })
    .to_string()
}

pub fn hard_bounce(id: &str) -> FeedbackEvent {
    FeedbackEvent::from_sns(&notification(id, &ses("Bounce")), TOPIC).expect("hard bounce")
}
