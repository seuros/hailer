//! Operator- and repo-curated domain/pattern blocklist. Checked before (and
//! cheaper than) paid address verification - same cost ordering as
//! suppression, and composed outermost of all three in `supervisor.rs`.
//!
//! A pattern is either an exact domain (`guerrillamail.com`) or a
//! `*.`-prefixed suffix glob (`*.il`, `*.kp`) matching that domain itself or
//! any subdomain under it. The match logic lives here, in Rust, rather than
//! as SQL `LIKE` expressions - easier to get right and to test.

#[cfg(test)]
mod tests;

/// The domain half of `local@domain`. `None` for an address with no `@`
/// (already rejected far earlier by [`crate::mail::SendRequest::validate`],
/// but this function makes no assumption about that).
pub fn domain_of(address: &str) -> Option<&str> {
    address.rsplit_once('@').map(|(_, domain)| domain)
}

/// Domains are case-insensitive; patterns are stored however an operator
/// typed them, so both sides are lowered before comparing.
pub fn matches(pattern: &str, domain: &str) -> bool {
    let domain = domain.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    pattern.strip_prefix("*.").map_or_else(
        || domain == pattern,
        |suffix| domain == suffix || domain.ends_with(&format!(".{suffix}")),
    )
}

/// First matching pattern's reason, or `None` if nothing in `patterns`
/// blocks `domain`. Order is irrelevant - a domain is either blocked or not.
pub fn blocking_reason<'a>(
    patterns: &'a [(String, Option<String>)],
    domain: &str,
) -> Option<&'a str> {
    patterns
        .iter()
        .find(|(pattern, _)| matches(pattern, domain))
        .map(|(_, reason)| reason.as_deref().unwrap_or("blocked"))
}

/// RFC 2606 reserves these for documentation/examples - `toto@example.com`
/// is syntactically a real address but can never have a real mailbox. This
/// is a boundary validation concern (an impossible address, same tier as a
/// malformed one), not an operator policy choice - always on, not a row in
/// `hailer.domain_blocks`, checked in [`crate::mail::SendRequest::validate`]
/// before any guard in the chain runs.
const RESERVED_EXAMPLE_PATTERNS: &[&str] = &[
    "*.example.com",
    "*.example.net",
    "*.example.org",
    "*.test",
    "*.example",
    "*.invalid",
    "*.localhost",
    "localhost",
];

pub fn is_reserved_example_domain(domain: &str) -> bool {
    RESERVED_EXAMPLE_PATTERNS.iter().any(|pattern| matches(pattern, domain))
}
