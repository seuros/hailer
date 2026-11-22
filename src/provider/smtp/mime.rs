//! Minimal RFC 5322 / RFC 5321 helpers for the SMTP provider.
//!
//! Rails already renders the full RFC 5322 MIME payload (`SendRequest::raw_mime`),
//! so unlike Postmark we never rebuild MIME: we extract the envelope `From`
//! address when not explicitly supplied, normalize line endings, and apply
//! RFC 5321 section 4.5.2 dot-stuffing before `DATA`.

#[cfg(test)]
mod tests;

use email_address::EmailAddress;

use crate::mail::SendRequest;
use crate::outbound::Correlation;

/// Maximum accepted length for a provider or RFC message identifier.
const MAX_ID_LEN: usize = 256;

/// Resolve the `MAIL FROM` address: explicit `smtp_envelope_from` wins,
/// otherwise extract the address from the MIME `From:` header.
pub(super) fn envelope_from(request: &SendRequest) -> Option<String> {
    if let Some(from) = &request.from_email_address {
        return is_safe_envelope_address(from).then(|| from.clone());
    }
    let header = extract_header(&request.raw_mime, "from")?;
    let addr = parse_addr_spec(&header)?;
    is_safe_envelope_address(addr).then(|| addr.to_owned())
}

/// Extract the RFC 5322 `Message-ID` header value (without surrounding `< >`),
/// used as a fallback identifier when an SMTP server omits a queue ID in `250`.
pub(super) fn extract_message_id(raw_mime: &str) -> Option<String> {
    let value = extract_header(raw_mime, "message-id")?;
    let trimmed = value.trim().trim_start_matches('<').trim_end_matches('>').trim();
    is_valid_id(trimmed).then(|| trimmed.to_owned())
}

/// Best-effort extraction of an MTA queue/message identifier from a `250` reply
/// (e.g. Postfix `2.0.0 Ok: queued as 4X1a2B3c`, Postal `OK (id=abc123)`,
/// Exim `OK id=1tAbCd-000123-45`).
pub(super) fn extract_queue_id(reply_lines: &[String]) -> Option<String> {
    for line in reply_lines {
        let lower = line.to_ascii_lowercase();
        for marker in ["queued as ", "id=", "id "] {
            if let Some(start) = find_marker(&lower, marker) {
                let rest = &line[start..];
                let token = rest
                    .split(|c: char| c.is_ascii_whitespace() || matches!(c, ')' | ']' | ',' | ';'))
                    .next()
                    .unwrap_or("");
                let token = token.trim_matches(|c| matches!(c, '<' | '>' | '"' | '\''));
                if is_valid_id(token) {
                    return Some(token.to_owned());
                }
            }
        }
    }
    None
}

fn find_marker(lower: &str, marker: &str) -> Option<usize> {
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find(marker) {
        let idx = offset + pos;
        let boundary_ok = idx == 0 || !lower.as_bytes()[idx - 1].is_ascii_alphanumeric();
        if boundary_ok {
            return Some(idx + marker.len());
        }
        offset = idx + 1;
    }
    None
}

/// Check that an address is syntactically valid and contains no ASCII control
/// bytes or SMTP command delimiters (`<`, `>`).
pub(super) fn is_safe_envelope_address(addr: &str) -> bool {
    !addr.is_empty()
        && addr.len() <= 320
        && !addr.bytes().any(|b| b < 0x20 || b == 0x7f || matches!(b, b'<' | b'>'))
        && EmailAddress::is_valid(addr)
}

/// Render `raw_mime` for SMTP `DATA`:
/// - prepends Hailer correlation headers on queued dispatches (safe with DKIM
///   signatures because unsigned `X-Hailer-*` headers are not in `h=`),
/// - normalizes bare LF (`\n`) and bare CR (`\r`) to CRLF (`\r\n`) to prevent
///   SMTP smuggling,
/// - dot-stuffs any line starting with `.` per RFC 5321 section 4.5.2,
/// - appends the final `.\r\n` terminator.
pub(super) fn prepare_data(raw_mime: &str, correlation: Option<&Correlation>) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw_mime.len() + 128);
    if let Some(corr) = correlation {
        out.extend_from_slice(b"X-Hailer-Message-Id: ");
        out.extend_from_slice(corr.hailer_message_id.as_bytes());
        out.extend_from_slice(b"\r\nX-Hailer-Attempt-Id: ");
        out.extend_from_slice(corr.attempt_id.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    let bytes = raw_mime.as_bytes();
    let mut i = 0;
    let mut line_start = true;
    while i < bytes.len() {
        let b = bytes[i];
        if line_start && b == b'.' {
            out.push(b'.');
        }
        match b {
            b'\r' => {
                out.extend_from_slice(b"\r\n");
                if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                    i += 2;
                } else {
                    i += 1;
                }
                line_start = true;
            }
            b'\n' => {
                out.extend_from_slice(b"\r\n");
                i += 1;
                line_start = true;
            }
            other => {
                out.push(other);
                i += 1;
                line_start = false;
            }
        }
    }

    if !out.ends_with(b"\r\n") {
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b".\r\n");
    out
}

fn is_valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_LEN && value.bytes().all(|b| b.is_ascii_graphic())
}

/// Extract and unfold the first matching header from the RFC 5322 header block
/// (stopping at the first blank line).
fn extract_header(raw_mime: &str, target_lower: &str) -> Option<String> {
    let mut collecting = false;
    let mut collected = String::new();

    for raw_line in raw_mime.split('\n') {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if collecting {
                collected.push(' ');
                collected.push_str(line.trim());
            }
            continue;
        }
        if collecting {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case(target_lower)
        {
            collecting = true;
            collected.push_str(value.trim());
        }
    }

    (!collected.is_empty()).then_some(collected)
}

/// Extract a single addr-spec from a `From` header value such as
/// `"Alice" <alice@example.dev>` or `alice@example.dev`.
fn parse_addr_spec(header: &str) -> Option<&str> {
    if let Some(open) = header.rfind('<')
        && let Some(rel_close) = header[open + 1..].find('>')
    {
        let inner = header[open + 1..open + 1 + rel_close].trim();
        return (!inner.is_empty()).then_some(inner);
    }
    let trimmed = header.trim().trim_matches('"');
    (!trimmed.is_empty()).then_some(trimmed)
}
