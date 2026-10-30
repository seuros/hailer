//! Helpers shared by the outbound HTTPS clients.

#[cfg(test)]
mod tests;

use std::io::ErrorKind;

/// Sent on every outbound request, never signed.
pub const USER_AGENT: &str = concat!("hailer/", env!("CARGO_PKG_VERSION"));

/// A refused or unroutable connection: the request cannot have left.
/// Anything else, including a timeout, may have reached the server.
pub fn never_connected(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(current) = cause {
        if let Some(io) = current.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::AddrNotAvailable
                    | ErrorKind::NetworkUnreachable
                    | ErrorKind::HostUnreachable
            )
        {
            return true;
        }
        cause = current.source();
    }
    false
}

/// RFC 3986 percent-encoding: every byte but the unreserved set. This is
/// what `SigV4` canonicalization and form bodies both need; an address's `+`
/// never arrives as a space.
pub fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0F)]));
        }
    }
    encoded
}

/// `name=value&...`, both sides percent-encoded.
pub fn encode_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    pairs
        .into_iter()
        .map(|(name, value)| format!("{}={}", percent_encode(name), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
