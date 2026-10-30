//! Durable ownership begins at the intake transaction, never at HTTP parsing.

mod attempt;
mod correlation;
mod dispatch;
#[cfg(test)]
mod dispatch_tests;
mod journal;
mod lifecycle;
mod maintenance;
mod operator;
mod store;
#[cfg(test)]
mod tests;
mod worker;

use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::mail::SendRequest;

pub const SEND_WINDOW_MS: i64 = 24 * 3600 * 1000;

pub use operator::Resolution;
pub use worker::run;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Correlation {
    pub hailer_message_id: String,
    pub attempt_id: String,
}

impl Correlation {
    pub fn is_valid(&self) -> bool {
        valid_id(&self.hailer_message_id) && valid_id(&self.attempt_id)
    }
}

pub fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) { byte == b'-' } else { byte.is_ascii_hexdigit() }
        })
}

#[derive(Clone, Serialize, Deserialize)]
struct Envelope {
    from_email_address: Option<String>,
    to_addresses: Vec<String>,
    cc_addresses: Vec<String>,
    bcc_addresses: Vec<String>,
}

impl Envelope {
    fn from_mail(mail: &SendRequest) -> Self {
        Self {
            from_email_address: mail.from_email_address.clone(),
            to_addresses: mail.to_addresses.clone(),
            cc_addresses: mail.cc_addresses.clone(),
            bcc_addresses: mail.bcc_addresses.clone(),
        }
    }

    fn with_body(self, raw_mime: String) -> SendRequest {
        SendRequest {
            raw_mime,
            from_email_address: self.from_email_address,
            to_addresses: self.to_addresses,
            cc_addresses: self.cc_addresses,
            bcc_addresses: self.bcc_addresses,
        }
    }

    fn recipients(&self) -> impl Iterator<Item = &String> {
        self.to_addresses.iter().chain(&self.cc_addresses).chain(&self.bcc_addresses)
    }
}

/// Separate from the synchronous contract: unknown command options are errors.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    #[serde(rename = "submission_key")]
    pub key: String,
    pub expires_at: Option<String>,
    pub raw_mime: String,
    #[serde(default)]
    pub from_email_address: Option<String>,
    #[serde(default)]
    pub to_addresses: Vec<String>,
    #[serde(default)]
    pub cc_addresses: Vec<String>,
    #[serde(default)]
    pub bcc_addresses: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    #[error("invalid message submission")]
    Invalid,
    #[error("submission key already belongs to a different command")]
    Conflict,
    #[error("durable spool capacity exhausted")]
    Capacity,
    #[error("durable mail state unavailable")]
    Unavailable,
}

impl IntakeError {
    pub const fn status(&self) -> u16 {
        match self {
            Self::Invalid => 422,
            Self::Conflict => 409,
            Self::Capacity => 429,
            Self::Unavailable => 503,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub hailer_message_id: String,
    pub accepted_at: String,
    pub expires_at: String,
}

#[derive(Serialize)]
pub struct MessageStatus {
    pub receipt: Receipt,
    pub status: String,
    pub version: i64,
    pub body_present: bool,
    pub provider_message_id: Option<String>,
    pub rfc_message_id: Option<String>,
    pub attempts: serde_json::Value,
    pub recipient_outcomes: serde_json::Value,
    pub attempts_truncated: bool,
    pub recipient_outcomes_truncated: bool,
}

#[derive(Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub hailer_message_id: String,
    pub kind: String,
    pub occurred_at: String,
    pub details: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
pub struct StoredLifecycleEvent {
    pub sequence: u64,
    pub event: LifecycleEvent,
}

#[derive(Serialize, Deserialize)]
pub struct LifecyclePage {
    pub events: Vec<StoredLifecycleEvent>,
    pub next_cursor: u64,
}

struct Prepared {
    key: String,
    mail: SendRequest,
    expiry: Option<jiff::Timestamp>,
    fingerprint: Vec<u8>,
    bytes: i64,
}

#[derive(Serialize)]
struct CanonicalCommand<'a> {
    version: u8,
    mail: &'a SendRequest,
    expires_at: Option<String>,
}

/// Hash canonical typed commands without allocating a second full MIME buffer.
struct Fingerprint {
    digest: Sha256,
    bytes: usize,
}

impl Write for Fingerprint {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.digest.update(bytes);
        self.bytes += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Prepared {
    fn new(command: Submission) -> Result<Self, IntakeError> {
        if command.key.trim().is_empty() || command.key.len() > 256 || command.key.contains('\0') {
            return Err(IntakeError::Invalid);
        }
        let mail = SendRequest {
            raw_mime: command.raw_mime,
            from_email_address: command.from_email_address,
            to_addresses: command.to_addresses,
            cc_addresses: command.cc_addresses,
            bcc_addresses: command.bcc_addresses,
        };
        mail.validate().map_err(|_| IntakeError::Invalid)?;
        let expiry = command
            .expires_at
            .map(|text| text.parse::<jiff::Timestamp>())
            .transpose()
            .map_err(|_| IntakeError::Invalid)?;
        let canonical = CanonicalCommand {
            version: 1,
            mail: &mail,
            expires_at: expiry.map(|value| value.to_string()),
        };
        let mut writer = Fingerprint { digest: Sha256::new(), bytes: 0 };
        serde_json::to_writer(&mut writer, &canonical).map_err(|_| IntakeError::Invalid)?;
        Ok(Self {
            key: command.key,
            mail,
            expiry,
            bytes: i64::try_from(writer.bytes).map_err(|_| IntakeError::Invalid)?,
            fingerprint: writer.digest.finalize().to_vec(),
        })
    }
}

/// Conservative audit metadata, never a correlation or delivery decision.
/// Only inspect a bounded, complete header block; ambiguous/invalid fields
/// produce no metadata rather than guessing or reconstructing MIME.
fn original_message_id(raw: &str) -> Option<String> {
    let headers = raw.split_once("\r\n\r\n").or_else(|| raw.split_once("\n\n"))?.0;
    if headers.len() > 65_536 || headers.contains('\0') {
        return None;
    }
    let mut found: Option<String> = None;
    let mut reading_id = false;
    for line in headers.lines() {
        if line.starts_with([' ', '\t']) {
            if reading_id {
                let value = found.as_mut()?;
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        let (name, value) = line.split_once(':')?;
        reading_id = name.eq_ignore_ascii_case("message-id");
        if reading_id {
            if found.is_some() || value.trim().is_empty() {
                return None;
            }
            found = Some(value.trim().to_owned());
        }
    }
    found
}
