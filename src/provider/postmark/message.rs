//! Rails' raw MIME as a Postmark `/email` body. Postmark rebuilds the
//! message, so only what its JSON can carry survives: addresses, subject,
//! bodies, attachments (inline ones by `cid:`), and custom headers.
//!
//! Recipients come from the envelope, as with SES: header addresses outside
//! the envelope are not mailed, and the header only lends display names.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};

use base64::Engine;
use mail_parser::{Mailbox, MessageParser, MessagePart, PartKind, PartRole};
use serde::Serialize;

use crate::mail::SendRequest;
use crate::outbound::Correlation;

/// Postmark refuses a larger `/email` request with 413.
pub const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Postmark builds these from its own fields, or they describe the MIME
/// structure it replaces.
const REBUILT_HEADERS: &[&str] = &[
    "bcc",
    "cc",
    "content-description",
    "content-disposition",
    "content-id",
    "content-transfer-encoding",
    "content-type",
    "date",
    "dkim-signature",
    "from",
    "mime-version",
    "received",
    "reply-to",
    "return-path",
    "subject",
    "to",
];

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    #[error("message has no header block")]
    Unparsable,
    #[error("message has no From address")]
    NoSender,
    #[error("message has neither a text nor an HTML body")]
    NoBody,
    #[error("message exceeds Postmark's {MAX_BODY_BYTES} byte request limit")]
    TooLarge,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Email {
    from: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    to: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    cc: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    bcc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    html_body: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    headers: Vec<Header>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<Attachment>,
    message_stream: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<&'static str, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Header {
    name: String,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Attachment {
    name: String,
    content: String,
    content_type: String,
    #[serde(rename = "ContentID", skip_serializing_if = "Option::is_none")]
    content_id: Option<String>,
}

/// The serialized `/email` request body.
pub fn render(
    request: &SendRequest,
    stream: &str,
    correlation: Option<&Correlation>,
) -> Result<Vec<u8>, RenderError> {
    let parsed = MessageParser::new().parse(&request.raw_mime).ok_or(RenderError::Unparsable)?;
    let message = parsed.root();

    let from = message
        .from()
        .and_then(|list| list.first())
        .and_then(rendered)
        .or_else(|| request.from_email_address.clone())
        .ok_or(RenderError::NoSender)?;

    let names: HashMap<String, &str> = message
        .to()
        .into_iter()
        .chain(message.cc())
        .flat_map(|list| list.mailboxes().collect::<Vec<_>>())
        .filter_map(|mailbox| Some((mailbox.address()?.to_ascii_lowercase(), mailbox.name()?)))
        .collect();
    let recipients = |addresses: &[String]| {
        addresses
            .iter()
            .filter_map(|address| {
                mailbox_string(names.get(&address.to_ascii_lowercase()).copied(), address)
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut text = String::new();
    let mut html = String::new();
    let mut attachments = Vec::new();
    for part in message.parts() {
        match (part.role(), part.kind()) {
            (PartRole::Container, _) => {}
            (PartRole::TextBody | PartRole::CopiedBody, PartKind::Text) => {
                text.push_str(&part.text().unwrap_or_default());
            }
            (PartRole::HtmlBody | PartRole::CopiedBody, PartKind::Html) => {
                html.push_str(&part.text().unwrap_or_default());
            }
            _ => attachments.push(attachment(part, attachments.len() + 1)),
        }
    }
    if text.is_empty() && html.is_empty() {
        return Err(RenderError::NoBody);
    }

    let headers = message
        .headers()
        .iter()
        .filter(|header| {
            !REBUILT_HEADERS.contains(&header.raw_name().to_ascii_lowercase().as_str())
        })
        .map(|header| Header {
            name: header.raw_name().to_owned(),
            value: unfold(&String::from_utf8_lossy(header.raw_value())),
        })
        .collect();

    let reply_to = message
        .reply_to()
        .map(|list| list.mailboxes().filter_map(rendered).collect::<Vec<_>>().join(", "));

    let metadata = correlation
        .map(|correlation| {
            BTreeMap::from([
                ("hailer_message_id", correlation.hailer_message_id.clone()),
                ("hailer_attempt_id", correlation.attempt_id.clone()),
            ])
        })
        .unwrap_or_default();

    let email = Email {
        from,
        to: recipients(&request.to_addresses),
        cc: recipients(&request.cc_addresses),
        bcc: recipients(&request.bcc_addresses),
        reply_to: reply_to.filter(|value| !value.is_empty()),
        subject: message.subject().map(str::to_owned),
        text_body: (!text.is_empty()).then_some(text),
        html_body: (!html.is_empty()).then_some(html),
        headers,
        attachments,
        message_stream: stream.to_owned(),
        metadata,
    };
    let body = serde_json::to_vec(&email).map_err(|_| RenderError::Unparsable)?;
    if body.len() > MAX_BODY_BYTES {
        return Err(RenderError::TooLarge);
    }
    Ok(body)
}

fn attachment(part: MessagePart<'_>, position: usize) -> Attachment {
    let content_type = part
        .content_type()
        .and_then(|value| Some(format!("{}/{}", value.ctype(), value.subtype()?)))
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    Attachment {
        name: part
            .attachment_name()
            .map_or_else(|| format!("attachment-{position}"), str::to_owned),
        content: base64::engine::general_purpose::STANDARD.encode(part.decoded()),
        content_type: content_type.to_ascii_lowercase(),
        content_id: part.content_id().map(|id| format!("cid:{id}")),
    }
}

fn rendered(mailbox: Mailbox<'_>) -> Option<String> {
    mailbox_string(mailbox.name(), mailbox.address()?)
}

/// `Name <address>` with the name quoted, or the bare address. Control
/// characters in a decoded name cannot become header lines.
fn mailbox_string(name: Option<&str>, address: &str) -> Option<String> {
    if address.is_empty() {
        return None;
    }
    let name: String =
        name.unwrap_or_default().chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    let name = name.trim();
    if name.is_empty() {
        return Some(address.to_owned());
    }
    let quoted = name.replace('\\', "\\\\").replace('"', "\\\"");
    Some(format!("\"{quoted}\" <{address}>"))
}

fn unfold(raw: &str) -> String {
    raw.lines().map(str::trim).filter(|line| !line.is_empty()).collect::<Vec<_>>().join(" ")
}
