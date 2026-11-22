//! Native async RFC 5321 / 3207 / 4954 ESMTP client built directly on `tokio`
//! and `rama`'s `rustls` + `ring` re-exports (zero additional Cargo crates).
//!
//! Every send is single-attempt and tracks whether `DATA` was committed on the
//! wire: failures before `DATA` body transmission are guaranteed unsent;
//! timeouts or disconnects after `DATA` transmission yield [`SmtpOutcome::Unknown`]
//! so Hailer never blindly duplicates mail.

mod connect;
mod reply;

use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use rama::tls::rustls::dep::rustls::crypto::ring;
use rama::tls::rustls::dep::rustls::pki_types::ServerName;
use rama::tls::rustls::dep::rustls::{ClientConfig, RootCertStore};
use rama::tls::rustls::dep::tokio_rustls::TlsConnector;
use rama::tls::rustls::dep::tokio_rustls::client::TlsStream;
use rama::tls::rustls::dep::webpki_roots;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;

use crate::config::{SmtpAuthMode, SmtpConfig, SmtpTlsMode};

use super::mime::extract_queue_id;

#[cfg(test)]
pub(super) use connect::{connect_addrs, interleave_addr_families};

use connect::connect_tcp;
use reply::{
    AuthMechanism, AuthOutcome, check_auth_reply, check_pre_data_reply, classify_reply_code,
    command, map_auth_error, read_reply, select_auth_mechanism, write_command,
};

#[derive(Clone)]
pub struct SmtpCredentials {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for SmtpCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SmtpCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Wire-level outcome of one SMTP send attempt, before mapping to sync
/// `ProviderError` or queued `DispatchFailure`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SmtpOutcome {
    /// Server replied `250` after `DATA` terminator; optional queue ID parsed
    /// from the `250` response text.
    Accepted { queue_id: Option<String> },
    /// Failed before `DATA` payload was transmitted (DNS, TCP, TLS, banner,
    /// `EHLO`, or pre-DATA disconnect/timeout). Guaranteed nothing was sent.
    Unsent,
    /// Server refused SMTP authentication (`530`/`534`/`535`/`538`) before
    /// reading the message.
    AuthFailed,
    /// Server replied `4xx` (transient deferral / throttling).
    TempFailed(u16),
    /// Server replied `5xx` (permanent refusal of sender, recipient, or body).
    Rejected(u16),
    /// Connection dropped, timed out, or returned an invalid reply *after*
    /// transmitting `DATA` payload; acceptance is unknown.
    Unknown,
}

/// Output of `hailer smtp-check`. Never carries credentials.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SmtpCheckReport {
    pub host: String,
    pub port: u16,
    pub tls: SmtpTlsMode,
    pub auth: SmtpAuthMode,
    pub authenticated: bool,
    pub greeting: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    PreData,
    DataCommitted,
}

enum Transport {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

pub(super) struct SmtpClient {
    host: String,
    port: u16,
    tls: SmtpTlsMode,
    auth: SmtpAuthMode,
    helo_domain: String,
    credentials: Option<SmtpCredentials>,
    tls_config: Arc<ClientConfig>,
}

impl SmtpClient {
    pub(super) fn new(config: &SmtpConfig, credentials: Option<SmtpCredentials>) -> Self {
        Self::with_tls_config(config, credentials, default_tls_config())
    }

    pub(super) fn with_tls_config(
        config: &SmtpConfig,
        credentials: Option<SmtpCredentials>,
        tls_config: Arc<ClientConfig>,
    ) -> Self {
        Self {
            host: config.host.clone(),
            port: config.effective_port(),
            tls: config.tls,
            auth: config.auth,
            helo_domain: config.effective_helo_domain().to_owned(),
            credentials,
            tls_config,
        }
    }

    pub(super) async fn send(
        &self,
        envelope_from: &str,
        recipients: &[&str],
        data: &[u8],
        timeout: Duration,
    ) -> SmtpOutcome {
        let mut phase = Phase::PreData;
        tokio::time::timeout(timeout, self.send_inner(envelope_from, recipients, data, &mut phase))
            .await
            .unwrap_or(match phase {
                Phase::PreData => SmtpOutcome::Unsent,
                Phase::DataCommitted => SmtpOutcome::Unknown,
            })
    }

    pub(super) async fn check(&self, timeout: Duration) -> anyhow::Result<SmtpCheckReport> {
        tokio::time::timeout(timeout, self.check_inner())
            .await
            .map_err(|_| anyhow::anyhow!("SMTP check timed out after {}s", timeout.as_secs()))?
    }

    async fn check_inner(&self) -> anyhow::Result<SmtpCheckReport> {
        let (mut io, greeting, capabilities) = self.connect_and_ehlo().await?;
        let authenticated = match self.authenticate(&mut io, &capabilities).await {
            Ok(did_auth) => did_auth,
            Err(AuthOutcome::Rejected(code)) => {
                anyhow::bail!("SMTP authentication failed with status {code}")
            }
            Err(AuthOutcome::TempFailed(code)) => {
                anyhow::bail!("SMTP authentication temporarily refused with status {code}")
            }
            Err(AuthOutcome::Transport(err)) => {
                return Err(anyhow::anyhow!("SMTP authentication transport error: {err}"));
            }
        };
        let _ = write_command(&mut io, "QUIT\r\n").await;
        Ok(SmtpCheckReport {
            host: self.host.clone(),
            port: self.port,
            tls: self.tls,
            auth: self.auth,
            authenticated,
            greeting,
            capabilities,
        })
    }

    async fn send_inner(
        &self,
        envelope_from: &str,
        recipients: &[&str],
        data: &[u8],
        phase: &mut Phase,
    ) -> SmtpOutcome {
        let Ok((mut io, _greeting, capabilities)) = self.connect_and_ehlo().await else {
            return SmtpOutcome::Unsent;
        };

        match self.authenticate(&mut io, &capabilities).await {
            Ok(_) => {}
            Err(AuthOutcome::Rejected(_)) => return SmtpOutcome::AuthFailed,
            Err(AuthOutcome::TempFailed(code)) => return SmtpOutcome::TempFailed(code),
            Err(AuthOutcome::Transport(_)) => return SmtpOutcome::Unsent,
        }

        let mail_from = format!("MAIL FROM:<{envelope_from}>\r\n");
        let Ok(reply) = command(&mut io, &mail_from).await else {
            return SmtpOutcome::Unsent;
        };
        if let Some(err) = check_pre_data_reply(&reply, 250) {
            return err;
        }

        for recipient in recipients {
            let rcpt_to = format!("RCPT TO:<{recipient}>\r\n");
            let Ok(reply) = command(&mut io, &rcpt_to).await else {
                return SmtpOutcome::Unsent;
            };
            if !matches!(reply.code, 250 | 251) {
                return classify_reply_code(reply.code, false);
            }
        }

        let Ok(reply) = command(&mut io, "DATA\r\n").await else {
            return SmtpOutcome::Unsent;
        };
        if let Some(err) = check_pre_data_reply(&reply, 354) {
            return err;
        }

        // Point of no return: once we begin sending the dot-terminated DATA
        // payload, any subsequent I/O failure or timeout is ambiguous.
        *phase = Phase::DataCommitted;
        if io.get_mut().write_all(data).await.is_err() || io.get_mut().flush().await.is_err() {
            return SmtpOutcome::Unknown;
        }

        let Ok(reply) = read_reply(&mut io).await else {
            return SmtpOutcome::Unknown;
        };
        if reply.code == 250 {
            let queue_id = extract_queue_id(&reply.lines);
            // Best-effort QUIT: a server closing immediately after 250 must not
            // turn an accepted message into an error.
            let _ = write_command(&mut io, "QUIT\r\n").await;
            SmtpOutcome::Accepted { queue_id }
        } else {
            classify_reply_code(reply.code, true)
        }
    }

    async fn connect_and_ehlo(
        &self,
    ) -> anyhow::Result<(BufReader<Transport>, String, Vec<String>)> {
        let tcp = connect_tcp(&self.host, self.port).await?;
        let mut io = match self.tls {
            SmtpTlsMode::Implicit => {
                let tls_stream = self.tls_handshake(tcp).await?;
                BufReader::new(Transport::Tls(Box::new(tls_stream)))
            }
            SmtpTlsMode::Starttls | SmtpTlsMode::None => BufReader::new(Transport::Plain(tcp)),
        };

        let greeting = read_reply(&mut io).await?;
        anyhow::ensure!(greeting.code == 220, "expected SMTP 220 greeting, got {}", greeting.code);
        let greeting_text = greeting.lines.join(" ");

        let ehlo_cmd = format!("EHLO {}\r\n", self.helo_domain);
        let mut ehlo = command(&mut io, &ehlo_cmd).await?;
        anyhow::ensure!(ehlo.code == 250, "SMTP EHLO failed with status {}", ehlo.code);

        if self.tls == SmtpTlsMode::Starttls {
            anyhow::ensure!(
                ehlo.lines.iter().skip(1).any(|line| line.trim().eq_ignore_ascii_case("STARTTLS")),
                "SMTP server did not advertise STARTTLS (refusing plaintext downgrade)"
            );
            let starttls = command(&mut io, "STARTTLS\r\n").await?;
            anyhow::ensure!(
                starttls.code == 220,
                "SMTP STARTTLS refused with status {}",
                starttls.code
            );
            // RFC 3207 section 4.2 / anti-response-injection: refuse any
            // buffered plaintext bytes received ahead of the TLS handshake.
            anyhow::ensure!(
                io.buffer().is_empty(),
                "unexpected buffered plaintext bytes before STARTTLS handshake"
            );
            let Transport::Plain(raw_tcp) = io.into_inner() else {
                anyhow::bail!("unexpected transport state during STARTTLS");
            };
            let tls_stream = self.tls_handshake(raw_tcp).await?;
            io = BufReader::new(Transport::Tls(Box::new(tls_stream)));
            ehlo = command(&mut io, &ehlo_cmd).await?;
            anyhow::ensure!(
                ehlo.code == 250,
                "post-STARTTLS SMTP EHLO failed with status {}",
                ehlo.code
            );
        }

        let capabilities = ehlo.lines.into_iter().skip(1).collect();
        Ok((io, greeting_text, capabilities))
    }

    async fn tls_handshake(&self, tcp: TcpStream) -> anyhow::Result<TlsStream<TcpStream>> {
        let server_name = ServerName::try_from(self.host.clone())
            .map_err(|_| anyhow::anyhow!("invalid TLS server name: {}", self.host))?;
        let connector = TlsConnector::from(self.tls_config.clone());
        connector.connect(server_name, tcp).await.map_err(Into::into)
    }

    async fn authenticate(
        &self,
        io: &mut BufReader<Transport>,
        capabilities: &[String],
    ) -> Result<bool, AuthOutcome> {
        let Some(creds) = &self.credentials else {
            return Ok(false);
        };
        let mechanism = match self.auth {
            SmtpAuthMode::None => return Ok(false),
            SmtpAuthMode::Plain => AuthMechanism::Plain,
            SmtpAuthMode::Login => AuthMechanism::Login,
            SmtpAuthMode::Auto => select_auth_mechanism(capabilities),
        };

        match mechanism {
            AuthMechanism::Plain => {
                let mut raw = Vec::with_capacity(creds.username.len() + creds.password.len() + 2);
                raw.push(0);
                raw.extend_from_slice(creds.username.as_bytes());
                raw.push(0);
                raw.extend_from_slice(creds.password.as_bytes());
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
                let line = format!("AUTH PLAIN {encoded}\r\n");
                let reply = command(io, &line).await.map_err(AuthOutcome::Transport)?;
                check_auth_reply(&reply)?;
            }
            AuthMechanism::Login => {
                let reply = command(io, "AUTH LOGIN\r\n").await.map_err(AuthOutcome::Transport)?;
                if reply.code != 334 {
                    return Err(map_auth_error(reply.code));
                }
                let user_b64 =
                    base64::engine::general_purpose::STANDARD.encode(creds.username.as_bytes());
                let reply = command(io, &format!("{user_b64}\r\n"))
                    .await
                    .map_err(AuthOutcome::Transport)?;
                if reply.code != 334 {
                    return Err(map_auth_error(reply.code));
                }
                let pass_b64 =
                    base64::engine::general_purpose::STANDARD.encode(creds.password.as_bytes());
                let reply = command(io, &format!("{pass_b64}\r\n"))
                    .await
                    .map_err(AuthOutcome::Transport)?;
                check_auth_reply(&reply)?;
            }
        }
        Ok(true)
    }
}

fn default_tls_config() -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring supports default TLS protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}
