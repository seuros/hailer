use std::sync::{Arc, Mutex};
use std::time::Duration;

use rama::crypto::dep::rcgen;
use rama::tls::rustls::dep::rustls::crypto::ring;
use rama::tls::rustls::dep::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rama::tls::rustls::dep::rustls::{ClientConfig, RootCertStore, ServerConfig};
use rama::tls::rustls::dep::tokio_rustls::TlsAcceptor;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::super::{SmtpCredentials, SmtpProvider};
use crate::config::{SmtpAuthMode, SmtpConfig, SmtpTlsMode};

#[derive(Clone, Default)]
pub(super) struct RecordedSession {
    pub(super) commands: Vec<String>,
    pub(super) data_body: String,
    pub(super) tls_upgraded: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MockTls {
    Plain,
    Starttls,
    Implicit,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MockFault {
    None,
    DropBeforeData,
    DropAfterData,
    HangAfterData,
}

#[derive(Clone)]
pub(super) struct ServerBehavior {
    pub(super) tls: MockTls,
    pub(super) auth_caps: String,
    pub(super) reject_auth_code: Option<u16>,
    pub(super) mail_from_reply: Option<String>,
    pub(super) rcpt_to_reply: Option<String>,
    pub(super) data_cmd_reply: Option<String>,
    pub(super) data_end_reply: String,
    pub(super) fault: MockFault,
}

impl Default for ServerBehavior {
    fn default() -> Self {
        Self {
            tls: MockTls::Plain,
            auth_caps: "AUTH PLAIN LOGIN".into(),
            reject_auth_code: None,
            mail_from_reply: None,
            rcpt_to_reply: None,
            data_cmd_reply: None,
            data_end_reply: "250 2.0.0 Ok: queued as Q-42".into(),
            fault: MockFault::None,
        }
    }
}

pub(super) struct MockSmtp {
    port: u16,
    sessions: Arc<Mutex<Vec<RecordedSession>>>,
    client_tls: Arc<ClientConfig>,
    task: JoinHandle<()>,
}

impl MockSmtp {
    pub(super) async fn start(behavior: ServerBehavior) -> Self {
        let (server_tls, client_tls) = make_localhost_tls();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let sessions_bg = sessions.clone();

        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let b = behavior.clone();
                let acceptor = TlsAcceptor::from(server_tls.clone());
                let sessions_conn = sessions_bg.clone();
                tokio::spawn(async move {
                    let mut rec = RecordedSession::default();
                    if b.tls == MockTls::Implicit {
                        if let Ok(tls) = acceptor.accept(stream).await {
                            rec.tls_upgraded = true;
                            drive_smtp(tls, &b, &mut rec, None).await;
                        }
                    } else {
                        drive_smtp(stream, &b, &mut rec, Some(acceptor)).await;
                    }
                    sessions_conn.lock().expect("sessions").push(rec);
                });
            }
        });

        Self { port, sessions, client_tls, task }
    }

    pub(super) fn sessions(&self) -> Vec<RecordedSession> {
        self.sessions.lock().expect("sessions").clone()
    }

    pub(super) async fn wait_sessions(&self, expected: usize) -> Vec<RecordedSession> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let current = self.sessions();
            if current.len() >= expected || tokio::time::Instant::now() >= deadline {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub(super) fn provider(
        &self,
        tls: SmtpTlsMode,
        auth: SmtpAuthMode,
        creds: Option<SmtpCredentials>,
    ) -> SmtpProvider {
        let host = if tls == SmtpTlsMode::None { "127.0.0.1" } else { "localhost" };
        let cfg = SmtpConfig {
            host: host.into(),
            port: Some(self.port),
            tls,
            auth,
            helo_domain: Some("hailer.test".into()),
        };
        SmtpProvider::with_tls_config(&cfg, creds, self.client_tls.clone())
    }

    pub(super) fn stop(self) {
        self.task.abort();
    }
}

pub(super) async fn start_raw_responder(payload: Vec<u8>) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let bytes = payload.clone();
            tokio::spawn(async move {
                let _ = stream.write_all(&bytes).await;
                let _ = stream.flush().await;
            });
        }
    });
    (port, task)
}

pub(super) fn make_localhost_tls() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("keypair");
    let params = rcgen::CertificateParams::new(vec!["localhost".into()]).expect("cert params");
    let cert = params.self_signed(&key).expect("self-signed");

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key.serialize_der()).expect("private key der");

    let provider = Arc::new(ring::default_provider());
    let server_cfg = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("server TLS versions")
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("server config");

    let mut roots = RootCertStore::empty();
    roots.add(cert_der).expect("add root");
    let client_cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("client TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();

    (Arc::new(server_cfg), Arc::new(client_cfg))
}

async fn drive_smtp<S>(
    stream: S,
    b: &ServerBehavior,
    rec: &mut RecordedSession,
    starttls_acceptor: Option<TlsAcceptor>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut io = BufReader::new(stream);
    if io.get_mut().write_all(b"220 localhost ESMTP MockSmtp\r\n").await.is_err() {
        return;
    }

    let mut line = String::new();
    loop {
        line.clear();
        let Ok(n) = io.read_line(&mut line).await else {
            return;
        };
        if n == 0 {
            return;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        rec.commands.push(trimmed.to_owned());
        let upper = trimmed.to_ascii_uppercase();

        if upper.starts_with("EHLO ") {
            let mut caps = vec!["250-localhost".to_owned()];
            if b.tls == MockTls::Starttls && !rec.tls_upgraded {
                caps.push("250-STARTTLS".to_owned());
            }
            if !b.auth_caps.is_empty() {
                caps.push(format!("250-{}", b.auth_caps));
            }
            caps.push("250 8BITMIME".to_owned());
            let resp = format!("{}\r\n", caps.join("\r\n"));
            if io.get_mut().write_all(resp.as_bytes()).await.is_err() {
                return;
            }
        } else if upper == "STARTTLS" {
            let Some(acceptor) = starttls_acceptor else {
                let _ = io.get_mut().write_all(b"502 not supported\r\n").await;
                return;
            };
            if io.get_mut().write_all(b"220 2.0.0 Ready to start TLS\r\n").await.is_err() {
                return;
            }
            let raw = io.into_inner();
            if let Ok(tls) = acceptor.accept(raw).await {
                rec.tls_upgraded = true;
                drive_smtp_tls(tls, b, rec).await;
            }
            return;
        } else if !handle_post_ehlo_command(&mut io, &upper, b, rec).await {
            return;
        }
    }
}

async fn drive_smtp_tls<S>(stream: S, b: &ServerBehavior, rec: &mut RecordedSession)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut io = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let Ok(n) = io.read_line(&mut line).await else {
            return;
        };
        if n == 0 {
            return;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        rec.commands.push(trimmed.to_owned());
        let upper = trimmed.to_ascii_uppercase();

        if upper.starts_with("EHLO ") {
            let mut caps = vec!["250-localhost".to_owned()];
            if !b.auth_caps.is_empty() {
                caps.push(format!("250-{}", b.auth_caps));
            }
            caps.push("250 8BITMIME".to_owned());
            let resp = format!("{}\r\n", caps.join("\r\n"));
            if io.get_mut().write_all(resp.as_bytes()).await.is_err() {
                return;
            }
        } else if !handle_post_ehlo_command(&mut io, &upper, b, rec).await {
            return;
        }
    }
}

async fn handle_post_ehlo_command<S>(
    io: &mut BufReader<S>,
    upper: &str,
    b: &ServerBehavior,
    rec: &mut RecordedSession,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if upper.starts_with("AUTH PLAIN ") {
        let resp = b.reject_auth_code.map_or_else(
            || "235 2.7.0 Authentication successful\r\n".to_owned(),
            |code| format!("{code} 5.7.8 Auth refused\r\n"),
        );
        io.get_mut().write_all(resp.as_bytes()).await.is_ok()
    } else if upper == "AUTH LOGIN" {
        if io.get_mut().write_all(b"334 VXNlcm5hbWU6\r\n").await.is_err() {
            return false;
        }
        let mut user = String::new();
        if io.read_line(&mut user).await.unwrap_or(0) == 0 {
            return false;
        }
        rec.commands.push(user.trim_end_matches(['\r', '\n']).to_owned());
        if io.get_mut().write_all(b"334 UGFzc3dvcmQ6\r\n").await.is_err() {
            return false;
        }
        let mut pass = String::new();
        if io.read_line(&mut pass).await.unwrap_or(0) == 0 {
            return false;
        }
        rec.commands.push(pass.trim_end_matches(['\r', '\n']).to_owned());
        let resp = b.reject_auth_code.map_or_else(
            || "235 2.7.0 Authentication successful\r\n".to_owned(),
            |code| format!("{code} 5.7.8 Auth refused\r\n"),
        );
        io.get_mut().write_all(resp.as_bytes()).await.is_ok()
    } else if upper.starts_with("MAIL FROM:") {
        if b.fault == MockFault::DropBeforeData {
            return false;
        }
        let resp = b
            .mail_from_reply
            .as_deref()
            .map_or_else(|| "250 2.1.0 OK\r\n".to_owned(), |r| format!("{r}\r\n"));
        io.get_mut().write_all(resp.as_bytes()).await.is_ok()
    } else if upper.starts_with("RCPT TO:") {
        let resp = b
            .rcpt_to_reply
            .as_deref()
            .map_or_else(|| "250 2.1.5 OK\r\n".to_owned(), |r| format!("{r}\r\n"));
        io.get_mut().write_all(resp.as_bytes()).await.is_ok()
    } else if upper == "DATA" {
        if let Some(reply) = &b.data_cmd_reply {
            let resp = format!("{reply}\r\n");
            return io.get_mut().write_all(resp.as_bytes()).await.is_ok();
        }
        if io.get_mut().write_all(b"354 End data with <CR><LF>.<CR><LF>\r\n").await.is_err() {
            return false;
        }
        let mut data_line = String::new();
        loop {
            data_line.clear();
            let Ok(n) = io.read_line(&mut data_line).await else {
                return false;
            };
            if n == 0 {
                return false;
            }
            if data_line == ".\r\n" {
                break;
            }
            rec.data_body.push_str(&data_line);
        }
        if b.fault == MockFault::DropAfterData {
            return false;
        }
        if b.fault == MockFault::HangAfterData {
            tokio::time::sleep(Duration::from_secs(10)).await;
            return false;
        }
        let resp = format!("{}\r\n", b.data_end_reply);
        io.get_mut().write_all(resp.as_bytes()).await.is_ok()
    } else if upper == "QUIT" {
        let _ = io.get_mut().write_all(b"221 2.0.0 Bye\r\n").await;
        false
    } else {
        io.get_mut().write_all(b"500 unrecognized\r\n").await.is_ok()
    }
}
