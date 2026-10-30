//! AWS Signature Version 4, header-signed. Verified against the official
//! test suite (`sigv4/tests.rs`).

#[cfg(test)]
mod tests;

use sha2::{Digest, Sha256};

use super::Credentials;
use crate::web::percent_encode;

pub struct Signing<'a> {
    pub method: &'a str,
    /// The path as sent: each segment already percent-encoded once.
    pub path: &'a str,
    /// Raw (unencoded) query pairs.
    pub query: &'a [(String, String)],
    /// Every header to sign, `host` and `x-amz-date` included.
    pub headers: &'a [(&'a str, String)],
    pub body: &'a [u8],
    pub service: &'a str,
    pub region: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
}

/// The `Authorization` header value.
pub fn authorization(credentials: &Credentials, signing: &Signing<'_>) -> String {
    let mut headers: Vec<(String, String)> = signing
        .headers
        .iter()
        .map(|(name, value)| {
            (name.to_ascii_lowercase(), value.split_whitespace().collect::<Vec<_>>().join(" "))
        })
        .collect();
    headers.sort();
    let signed = headers.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join(";");
    let canonical_headers = headers.iter().fold(String::new(), |mut out, (name, value)| {
        out.push_str(name);
        out.push(':');
        out.push_str(value);
        out.push('\n');
        out
    });
    let canonical = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed}\n{}",
        signing.method,
        canonical_uri(signing.path),
        canonical_query(signing.query),
        hex(&Sha256::digest(signing.body)),
    );
    let date = signing.amz_date.get(..8).unwrap_or(signing.amz_date);
    let scope = format!("{date}/{}/{}/aws4_request", signing.region, signing.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        signing.amz_date,
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let key = [date, signing.region, signing.service, "aws4_request"]
        .iter()
        .fold(format!("AWS4{}", credentials.secret_access_key).into_bytes(), |key, part| {
            hmac(&key, part.as_bytes()).to_vec()
        });
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={}",
        credentials.access_key_id,
        hex(&hmac(&key, string_to_sign.as_bytes()))
    )
}

/// Sorted, each side percent-encoded; also the query string as sent.
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> =
        query.iter().map(|(name, value)| (percent_encode(name), percent_encode(value))).collect();
    pairs.sort();
    pairs.iter().map(|(name, value)| format!("{name}={value}")).collect::<Vec<_>>().join("&")
}

/// Services other than S3 encode each segment again on top of the request's
/// own encoding.
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_owned();
    }
    path.split('/').map(percent_encode).collect::<Vec<_>>().join("/")
}

/// HMAC-SHA256 (RFC 2104).
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut block = [0_u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0F)]));
    }
    out
}
