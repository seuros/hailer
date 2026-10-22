//! Domain risk signals that justify a *quarantine* decision (hold for
//! operator review), as opposed to [`crate::domain_block`]'s hard reject.
//! Only wired into the queued/durable pipeline - a synchronous send has no
//! way to "come back later" for operator review.
//!
//! RDAP (<https://www.rfc-editor.org/rfc/rfc7483>) is the IETF-standardized
//! successor to WHOIS: structured JSON over HTTP, with an IANA bootstrap
//! registry mapping TLD -> RDAP server, instead of a different ad hoc text
//! format per registrar on port 43.

pub mod rdap;
#[cfg(test)]
mod tests;

use std::future::Future;

use jiff::Timestamp;
use thiserror::Error;

use rdap::RdapClient;

/// Pure fact, not a verdict - "is this new" is a policy threshold applied by
/// the caller (`is_new`), the same split as [`crate::verify::Verdict`] vs
/// `reject_states`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainFacts {
    pub registered_at: Option<Timestamp>,
}

impl DomainFacts {
    pub const UNKNOWN: Self = Self { registered_at: None };
}

#[derive(Debug, Error)]
pub enum IntelError {
    #[error("RDAP lookup failed: {0}")]
    Transport(String),
    #[error("RDAP response was not understood: {0}")]
    InvalidResponse(String),
}

/// `registered_at` unknown always means "not new" - a lookup failure or a
/// TLD with no RDAP coverage fails open, same policy as address
/// verification: this signal is never the only thing standing between a
/// sender and a working send.
pub fn is_new(facts: DomainFacts, now: Timestamp, threshold_days: u32) -> bool {
    facts.registered_at.is_some_and(|registered_at| {
        // Plain second arithmetic, not `Timestamp::since`'s `Span`: a span
        // between two instants carries no calendar day count unless asked
        // for one explicitly, so `.get_days()` on it is always zero.
        let elapsed_seconds = now.as_second().saturating_sub(registered_at.as_second());
        elapsed_seconds < i64::from(threshold_days) * 86_400
    })
}

/// A domain in the mail envelope that is not plain ASCII is either a
/// malformed/unencoded IDN (legitimate ones are transmitted as ASCII
/// punycode, `xn--...`) or a homograph-spoofing attempt. Either way it is
/// worth a human look, not a silent accept.
pub const fn has_non_ascii_label(domain: &str) -> bool {
    !domain.is_ascii()
}

/// Static dispatch, matching [`crate::verify::AddressVerifier`]'s own
/// convention - no `async-trait`, no boxed futures.
pub trait DomainIntelligence: Send + Sync {
    fn lookup(&self, domain: &str) -> impl Future<Output = Result<DomainFacts, IntelError>> + Send;
}

/// The one compile-time dispatch point a new intelligence source needs.
pub enum Intel {
    Disabled,
    Rdap(RdapClient),
}

impl DomainIntelligence for Intel {
    async fn lookup(&self, domain: &str) -> Result<DomainFacts, IntelError> {
        match self {
            Self::Disabled => Ok(DomainFacts::UNKNOWN),
            Self::Rdap(client) => client.lookup(domain).await,
        }
    }
}
