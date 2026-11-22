//! Bounded SMTP command/reply wire I/O, status classification, and EHLO AUTH
//! mechanism selection.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::{SmtpOutcome, Transport};

/// Cap reply lines and line bytes so a misbehaving server cannot exhaust memory.
const MAX_REPLY_LINES: usize = 64;
const MAX_REPLY_LINE_BYTES: usize = 8192;

#[derive(Debug)]
pub(super) struct Reply {
    pub(super) code: u16,
    pub(super) lines: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthMechanism {
    Plain,
    Login,
}

pub(super) enum AuthOutcome {
    Rejected(u16),
    TempFailed(u16),
    Transport(io::Error),
}

pub(super) fn select_auth_mechanism(capabilities: &[String]) -> AuthMechanism {
    let mut supports_plain = false;
    let mut supports_login = false;
    for cap in capabilities {
        let mut parts = cap.split_ascii_whitespace();
        if let Some(keyword) = parts.next()
            && (keyword.eq_ignore_ascii_case("AUTH")
                || keyword.to_ascii_uppercase().starts_with("AUTH="))
        {
            if let Some(inline) = keyword.split_once('=').map(|(_, rest)| rest) {
                if inline.eq_ignore_ascii_case("PLAIN") {
                    supports_plain = true;
                } else if inline.eq_ignore_ascii_case("LOGIN") {
                    supports_login = true;
                }
            }
            for mech in parts {
                if mech.eq_ignore_ascii_case("PLAIN") {
                    supports_plain = true;
                } else if mech.eq_ignore_ascii_case("LOGIN") {
                    supports_login = true;
                }
            }
        }
    }
    if !supports_plain && supports_login { AuthMechanism::Login } else { AuthMechanism::Plain }
}

pub(super) const fn check_auth_reply(reply: &Reply) -> Result<(), AuthOutcome> {
    if reply.code == 235 { Ok(()) } else { Err(map_auth_error(reply.code)) }
}

pub(super) const fn map_auth_error(code: u16) -> AuthOutcome {
    if matches!(code, 400..=499) {
        AuthOutcome::TempFailed(code)
    } else {
        AuthOutcome::Rejected(code)
    }
}

pub(super) const fn check_pre_data_reply(reply: &Reply, expected: u16) -> Option<SmtpOutcome> {
    if reply.code == expected { None } else { Some(classify_reply_code(reply.code, false)) }
}

pub(super) const fn classify_reply_code(code: u16, after_data: bool) -> SmtpOutcome {
    if matches!(code, 530 | 534 | 535 | 538) {
        SmtpOutcome::AuthFailed
    } else if matches!(code, 400..=499) {
        SmtpOutcome::TempFailed(code)
    } else if matches!(code, 500..=599) {
        SmtpOutcome::Rejected(code)
    } else if after_data {
        SmtpOutcome::Unknown
    } else {
        SmtpOutcome::Unsent
    }
}

pub(super) async fn write_command(io: &mut BufReader<Transport>, cmd: &str) -> io::Result<()> {
    io.get_mut().write_all(cmd.as_bytes()).await?;
    io.get_mut().flush().await
}

pub(super) async fn command(io: &mut BufReader<Transport>, cmd: &str) -> io::Result<Reply> {
    write_command(io, cmd).await?;
    read_reply(io).await
}

pub(super) async fn read_reply(io: &mut BufReader<Transport>) -> io::Result<Reply> {
    let mut lines = Vec::new();
    let mut expected_code: Option<u16> = None;

    for _ in 0..MAX_REPLY_LINES {
        let raw = read_bounded_line(io).await?;
        let bytes = raw.as_bytes();
        if bytes.len() < 3 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short SMTP reply"));
        }
        if !bytes[..3].iter().all(u8::is_ascii_digit) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "non-numeric SMTP code"));
        }
        let code = u16::from(bytes[0] - b'0') * 100
            + u16::from(bytes[1] - b'0') * 10
            + u16::from(bytes[2] - b'0');
        if let Some(prev) = expected_code
            && prev != code
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mismatched multiline SMTP code",
            ));
        }
        expected_code = Some(code);

        let sep = bytes.get(3).copied();
        match sep {
            Some(b'-') => lines.push(raw[4..].to_owned()),
            Some(b' ') => {
                lines.push(raw[4..].to_owned());
                return Ok(Reply { code, lines });
            }
            None => {
                lines.push(String::new());
                return Ok(Reply { code, lines });
            }
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid SMTP reply separator",
                ));
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::InvalidData, "SMTP reply exceeded line limit"))
}

async fn read_bounded_line(io: &mut BufReader<Transport>) -> io::Result<String> {
    let mut buf = Vec::new();
    loop {
        let available = io.fill_buf().await?;
        if available.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "SMTP connection closed"));
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            if buf.len() + pos + 1 > MAX_REPLY_LINE_BYTES {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "SMTP reply line too long"));
            }
            buf.extend_from_slice(&available[..=pos]);
            io.consume(pos + 1);
            break;
        }
        let len = available.len();
        if buf.len() + len > MAX_REPLY_LINE_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "SMTP reply line too long"));
        }
        buf.extend_from_slice(available);
        io.consume(len);
    }
    let text = String::from_utf8_lossy(&buf);
    Ok(text.trim_end_matches(['\r', '\n']).to_owned())
}
