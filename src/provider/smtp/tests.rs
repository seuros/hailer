mod mock;

use std::assert_matches;
use std::sync::{Arc, Mutex};

use base64::Engine;
use rama::tls::rustls::dep::rustls::crypto::ring;
use rama::tls::rustls::dep::rustls::pki_types::CertificateDer;
use rama::tls::rustls::dep::rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::*;
use crate::config::{SmtpAuthMode, SmtpConfig, SmtpTlsMode};
use crate::outbound::Correlation;
use crate::provider::SEND_FLOOR;
use crate::provider::test_support::correlation;
use mock::{MockFault, MockSmtp, MockTls, ServerBehavior, make_localhost_tls, start_raw_responder};

fn sample_request() -> SendRequest {
    SendRequest {
        raw_mime: "From: \"Alice\" <alice@example.dev>\r\n\
                   To: bob@example.dev\r\n\
                   Message-ID: <msg-123@example.dev>\r\n\
                   Subject: Hello\r\n\r\n\
                   Line one\n.Dot line\r\nDone"
            .into(),
        from_email_address: None,
        to_addresses: vec!["bob@example.dev".into()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

#[tokio::test]
async fn plaintext_send_delivers_dot_stuffed_raw_mime_and_returns_queue_id() {
    let server = MockSmtp::start(ServerBehavior::default()).await;
    let creds = Some(SmtpCredentials { username: "u".into(), password: "p".into() });
    let provider = server.provider(SmtpTlsMode::None, SmtpAuthMode::Plain, creds);

    let id = provider.send(&sample_request(), Deadline::never()).await.expect("accepted");
    assert_eq!(id, "Q-42");

    let sessions = server.wait_sessions(1).await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].commands[0], "EHLO hailer.test");
    let expected_auth =
        format!("AUTH PLAIN {}", base64::engine::general_purpose::STANDARD.encode(b"\0u\0p"));
    assert_eq!(sessions[0].commands[1], expected_auth);
    assert_eq!(sessions[0].commands[2], "MAIL FROM:<alice@example.dev>");
    assert_eq!(sessions[0].commands[3], "RCPT TO:<bob@example.dev>");
    assert_eq!(sessions[0].commands[4], "DATA");
    // Bare LF normalized to CRLF and leading dot stuffed to `..`.
    assert!(sessions[0].data_body.contains("Line one\r\n..Dot line\r\nDone\r\n"));
    assert!(!sessions[0].data_body.contains("X-Hailer-Message-Id:"));
    server.stop();
}

#[tokio::test]
async fn queued_dispatch_adds_correlation_headers_and_falls_back_to_rfc_message_id() {
    let behavior = ServerBehavior { data_end_reply: "250 OK".into(), ..Default::default() };
    let server = MockSmtp::start(behavior).await;
    let provider = server.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);

    let corr = correlation();
    let outcome = provider.dispatch(&sample_request(), &corr).await;
    assert_eq!(outcome, DispatchOutcome::Accepted("msg-123@example.dev".into()));

    let sessions = server.wait_sessions(1).await;
    let body = &sessions[0].data_body;
    assert!(body.starts_with(&format!(
        "X-Hailer-Message-Id: {}\r\nX-Hailer-Attempt-Id: {}\r\n",
        corr.hailer_message_id, corr.attempt_id
    )));
    server.stop();
}

#[tokio::test]
async fn starttls_upgrades_connection_and_auto_selects_login_when_plain_is_absent() {
    let behavior = ServerBehavior {
        tls: MockTls::Starttls,
        auth_caps: "AUTH LOGIN".into(),
        data_end_reply: "250 OK (id=starttls-99)".into(),
        ..Default::default()
    };
    let server = MockSmtp::start(behavior).await;
    let creds = Some(SmtpCredentials { username: "mailer".into(), password: "secret".into() });
    let provider = server.provider(SmtpTlsMode::Starttls, SmtpAuthMode::Auto, creds);

    let id = provider.send(&sample_request(), Deadline::never()).await.expect("accepted");
    assert_eq!(id, "starttls-99");

    let sessions = server.wait_sessions(1).await;
    let session = &sessions[0];
    assert!(session.tls_upgraded, "must upgrade via STARTTLS");
    assert_eq!(session.commands[0], "EHLO hailer.test");
    assert_eq!(session.commands[1], "STARTTLS");
    assert_eq!(session.commands[2], "EHLO hailer.test");
    assert_eq!(session.commands[3], "AUTH LOGIN");
    assert_eq!(session.commands[4], base64::engine::general_purpose::STANDARD.encode("mailer"));
    assert_eq!(session.commands[5], base64::engine::general_purpose::STANDARD.encode("secret"));
    server.stop();
}

#[tokio::test]
async fn starttls_refuses_plaintext_downgrade_when_server_omits_starttls() {
    let behavior = ServerBehavior { tls: MockTls::Plain, ..Default::default() };
    let server = MockSmtp::start(behavior).await;
    let creds = Some(SmtpCredentials { username: "u".into(), password: "secret".into() });
    let provider = server.provider(SmtpTlsMode::Starttls, SmtpAuthMode::Plain, creds);

    assert_matches!(
        provider.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        provider.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );

    for session in server.wait_sessions(2).await {
        assert!(!session.commands.iter().any(|c| c.starts_with("AUTH")));
        assert!(!session.commands.iter().any(|c| c.starts_with("MAIL FROM")));
    }
    server.stop();
}

#[tokio::test]
async fn implicit_tls_connects_over_tls_immediately() {
    let behavior = ServerBehavior {
        tls: MockTls::Implicit,
        data_end_reply: "250 2.0.0 Ok: queued as IMPLICIT-7".into(),
        ..Default::default()
    };
    let server = MockSmtp::start(behavior).await;
    let creds = Some(SmtpCredentials { username: "u".into(), password: "p".into() });
    let provider = server.provider(SmtpTlsMode::Implicit, SmtpAuthMode::Plain, creds);

    let id = provider.send(&sample_request(), Deadline::never()).await.expect("accepted");
    assert_eq!(id, "IMPLICIT-7");

    let sessions = server.wait_sessions(1).await;
    let session = &sessions[0];
    assert!(session.tls_upgraded);
    assert!(!session.commands.iter().any(|c| c == "STARTTLS"));
    server.stop();
}

#[tokio::test]
async fn pre_data_and_post_data_failures_map_honestly() {
    // 1. Auth 535 -> Transport (sync), Unauthorized retryable (queued).
    let auth_fail =
        MockSmtp::start(ServerBehavior { reject_auth_code: Some(535), ..Default::default() }).await;
    let creds = Some(SmtpCredentials { username: "u".into(), password: "bad".into() });
    let p = auth_fail.provider(SmtpTlsMode::None, SmtpAuthMode::Plain, creds);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_unauthorized", retryable: true }
    );
    auth_fail.stop();

    // 2. RCPT 451 -> Transport (sync), Throttled retryable (queued).
    let temp_fail = MockSmtp::start(ServerBehavior {
        rcpt_to_reply: Some("451 4.7.1 Try again later".into()),
        ..Default::default()
    })
    .await;
    let p = temp_fail.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
    );
    temp_fail.stop();

    // 3. RCPT 550 -> Rejected (non-retryable, never echoes recipient address).
    let perm_fail = MockSmtp::start(ServerBehavior {
        rcpt_to_reply: Some("550 5.1.1 User unknown: bob@example.dev".into()),
        ..Default::default()
    })
    .await;
    let p = perm_fail.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    let err = p.send(&sample_request(), Deadline::never()).await.expect_err("rejected");
    assert_matches!(err, ProviderError::Rejected(ref msg) if !msg.contains('@'));
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
    );
    perm_fail.stop();

    // 4. Disconnect BEFORE DATA -> Unsent / NotDispatched (safe to retry).
    let drop_pre =
        MockSmtp::start(ServerBehavior { fault: MockFault::DropBeforeData, ..Default::default() })
            .await;
    let p = drop_pre.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );
    drop_pre.stop();

    // 5. Disconnect AFTER DATA -> Ambiguous / Unknown (never blindly retried).
    let drop_post =
        MockSmtp::start(ServerBehavior { fault: MockFault::DropAfterData, ..Default::default() })
            .await;
    let p = drop_post.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Ambiguous)
    );
    assert_eq!(p.dispatch(&sample_request(), &correlation()).await, DispatchOutcome::Unknown);
    drop_post.stop();
}

#[tokio::test]
async fn timeout_after_data_is_ambiguous() {
    let hang =
        MockSmtp::start(ServerBehavior { fault: MockFault::HangAfterData, ..Default::default() })
            .await;
    let p = hang.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    let tight = crate::budget::after(Duration::from_millis(2100));
    assert_matches!(p.send(&sample_request(), tight).await, Err(ProviderError::Ambiguous));
    hang.stop();
}

#[tokio::test]
async fn rejections_leave_circuit_closed_and_outages_open_it() {
    let perm_fail = MockSmtp::start(ServerBehavior {
        rcpt_to_reply: Some("550 5.1.1 Mailbox unavailable".into()),
        ..Default::default()
    })
    .await;
    let p = perm_fail.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    for _ in 0..8 {
        assert_matches!(
            p.send(&sample_request(), Deadline::never()).await,
            Err(ProviderError::Rejected(_))
        );
    }
    assert_eq!(p.circuit_state(), "closed");
    perm_fail.stop();

    let closed = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let dead_port = closed.local_addr().expect("addr").port();
    drop(closed);
    let cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(dead_port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let p = SmtpProvider::new(&cfg, None);
    for _ in 0..8 {
        assert!(p.send(&sample_request(), Deadline::never()).await.is_err());
    }
    assert_eq!(p.circuit_state(), "open");
}

#[tokio::test]
async fn smtp_check_reports_capabilities_without_leaking_credentials() {
    let server = MockSmtp::start(ServerBehavior {
        tls: MockTls::Starttls,
        auth_caps: "AUTH PLAIN LOGIN".into(),
        ..Default::default()
    })
    .await;
    let creds = Some(SmtpCredentials {
        username: "secret-user".into(),
        password: "super-secret-password".into(),
    });
    let p = server.provider(SmtpTlsMode::Starttls, SmtpAuthMode::Plain, creds);
    let report = p.check().await.expect("check");
    assert!(report.authenticated);
    assert_eq!(report.tls, SmtpTlsMode::Starttls);
    assert!(report.capabilities.iter().any(|c| c.contains("AUTH PLAIN")));
    let json = serde_json::to_string(&report).expect("json");
    assert!(!json.contains("super-secret-password"), "{json}");
    assert!(!json.contains("secret-user"), "{json}");
    server.stop();
}

#[tokio::test]
async fn budget_below_send_floor_and_missing_from_refuse_without_connecting() {
    let server = MockSmtp::start(ServerBehavior::default()).await;
    let p = server.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);

    let spent = crate::budget::after(SEND_FLOOR.saturating_sub(Duration::from_millis(1)));
    assert_matches!(p.send(&sample_request(), spent).await, Err(ProviderError::BudgetSpent));

    let mut no_from = sample_request();
    no_from.raw_mime = "To: bob@example.dev\r\nSubject: Hi\r\n\r\nNo from header".into();
    assert_matches!(p.send(&no_from, Deadline::never()).await, Err(ProviderError::InvalidRequest));
    assert_eq!(
        p.dispatch(&no_from, &correlation()).await,
        DispatchOutcome::NotAccepted { code: "invalid_message", retryable: false }
    );
    assert!(server.sessions().is_empty());
    server.stop();
}

#[tokio::test]
async fn starttls_refuses_buffered_plaintext_bytes_before_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut io = BufReader::new(stream);
                let _ = io.get_mut().write_all(b"220 localhost ESMTP\r\n").await;
                let mut line = String::new();
                while io.read_line(&mut line).await.unwrap_or(0) > 0 {
                    let upper = line.trim().to_ascii_uppercase();
                    line.clear();
                    if upper.starts_with("EHLO") {
                        let _ = io.get_mut().write_all(b"250-localhost\r\n250 STARTTLS\r\n").await;
                    } else if upper == "STARTTLS" {
                        // Response injection: append extra plaintext bytes right
                        // after the 220 ready line before TLS begins.
                        let _ = io.get_mut().write_all(b"220 Ready\r\n250 injected\r\n").await;
                        break;
                    }
                }
            });
        }
    });

    let (_server_tls, client_tls) = make_localhost_tls();
    let cfg = SmtpConfig {
        host: "localhost".into(),
        port: Some(port),
        tls: SmtpTlsMode::Starttls,
        auth: SmtpAuthMode::Plain,
        helo_domain: Some("hailer.test".into()),
    };
    let creds = Some(SmtpCredentials { username: "u".into(), password: "p".into() });
    let p = SmtpProvider::with_tls_config(&cfg, creds, client_tls);

    let err = p.check().await.expect_err("must reject buffered plaintext");
    assert!(err.to_string().contains("buffered plaintext"), "{err}");
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    task.abort();
}

#[tokio::test]
async fn corrupt_non_ascii_reply_does_not_panic_and_maps_to_unsent() {
    // Byte index 3 falls inside the 2-byte UTF-8 sequence 0xC3 0xA9.
    let (port, task) = start_raw_responder(b"aa\xC3\xA9\r\n".to_vec()).await;
    let cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let p = SmtpProvider::new(&cfg, None);

    let err = p.check().await.expect_err("must reject non-numeric reply");
    assert!(err.to_string().contains("non-numeric"), "{err}");
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );
    task.abort();
}

#[tokio::test]
async fn oversized_reply_line_and_excessive_reply_lines_are_rejected_cleanly() {
    let mut long_line = b"220 ".to_vec();
    long_line.resize(9000, b'x');
    long_line.extend_from_slice(b"\r\n");
    let (port_line, task_line) = start_raw_responder(long_line).await;
    let cfg_line = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(port_line),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let err =
        SmtpProvider::new(&cfg_line, None).check().await.expect_err("must reject oversized line");
    assert!(err.to_string().contains("too long"), "{err}");
    task_line.abort();

    let mut many_lines = Vec::new();
    for _ in 0..70 {
        many_lines.extend_from_slice(b"220-continuation\r\n");
    }
    many_lines.extend_from_slice(b"220 done\r\n");
    let (port_many, task_many) = start_raw_responder(many_lines).await;
    let cfg_many = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(port_many),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let err = SmtpProvider::new(&cfg_many, None)
        .check()
        .await
        .expect_err("must reject excessive reply lines");
    assert!(err.to_string().contains("line limit"), "{err}");
    task_many.abort();
}

#[tokio::test]
async fn partial_multi_recipient_multiline_550_aborts_before_data_without_leaking_address() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let seen_commands = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_bg = seen_commands.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let log = seen_bg.clone();
            tokio::spawn(async move {
                let mut io = BufReader::new(stream);
                let _ = io.get_mut().write_all(b"220 localhost ESMTP\r\n").await;
                let mut line = String::new();
                while io.read_line(&mut line).await.unwrap_or(0) > 0 {
                    let trimmed = line.trim_end_matches(['\r', '\n']).to_owned();
                    line.clear();
                    log.lock().expect("log").push(trimmed.clone());
                    let upper = trimmed.to_ascii_uppercase();
                    if upper.starts_with("EHLO") {
                        let _ = io.get_mut().write_all(b"250 localhost\r\n").await;
                    } else if upper.starts_with("MAIL FROM:") {
                        let _ = io.get_mut().write_all(b"250 OK\r\n").await;
                    } else if upper.starts_with("RCPT TO:") {
                        if trimmed.contains("bad-secret@example.dev") {
                            let _ = io
                                .get_mut()
                                .write_all(
                                    b"550-5.1.1 <bad-secret@example.dev> user unknown\r\n\
                                      550 Please check recipient address\r\n",
                                )
                                .await;
                        } else {
                            let _ = io.get_mut().write_all(b"250 OK\r\n").await;
                        }
                    }
                }
            });
        }
    });

    let cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: Some("hailer.test".into()),
    };
    let p = SmtpProvider::new(&cfg, None);
    let mut req = sample_request();
    req.to_addresses = vec!["good@example.dev".into(), "bad-secret@example.dev".into()];

    let err = p.send(&req, Deadline::never()).await.expect_err("must reject");
    match err {
        ProviderError::Rejected(msg) => {
            assert!(msg.contains("550"), "{msg}");
            assert!(!msg.contains("bad-secret@example.dev"), "leaked recipient: {msg}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
    let cmds = seen_commands.lock().expect("cmds").clone();
    assert!(!cmds.iter().any(|c| c.eq_ignore_ascii_case("DATA")), "DATA must not run: {cmds:?}");
    task.abort();
}

#[tokio::test]
async fn mail_from_4xx_and_5xx_and_unauthenticated_auto_mode_work_correctly() {
    let temp = MockSmtp::start(ServerBehavior {
        mail_from_reply: Some("452 4.5.3 Too many senders".into()),
        ..Default::default()
    })
    .await;
    let p_temp = temp.provider(SmtpTlsMode::None, SmtpAuthMode::Auto, None);
    assert_matches!(
        p_temp.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p_temp.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
    );
    temp.stop();

    let perm = MockSmtp::start(ServerBehavior {
        mail_from_reply: Some("550 5.7.1 Sender not authorized".into()),
        ..Default::default()
    })
    .await;
    let p_perm = perm.provider(SmtpTlsMode::None, SmtpAuthMode::Auto, None);
    assert_matches!(
        p_perm.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Rejected(_))
    );
    assert_eq!(
        p_perm.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
    );
    perm.stop();

    let open_relay = MockSmtp::start(ServerBehavior::default()).await;
    let p_open = open_relay.provider(SmtpTlsMode::None, SmtpAuthMode::Auto, None);
    let id = p_open.send(&sample_request(), Deadline::never()).await.expect("open relay send");
    assert_eq!(id, "Q-42");
    let sessions = open_relay.wait_sessions(1).await;
    assert!(!sessions[0].commands.iter().any(|c| c.starts_with("AUTH")));
    open_relay.stop();
}

#[tokio::test]
async fn happy_eyeballs_interleaves_address_families_and_falls_back_to_working_addr() {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

    let v6_a = SocketAddr::from((Ipv6Addr::LOCALHOST, 25));
    let v6_b = SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1], 25));
    let v4_a = SocketAddr::from((Ipv4Addr::LOCALHOST, 25));
    let v4_b = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 25));
    assert_eq!(
        client::interleave_addr_families(vec![v6_a, v6_b, v4_a, v4_b]),
        vec![v6_a, v4_a, v6_b, v4_b]
    );

    let closed = TcpListener::bind("127.0.0.1:0").await.expect("bind closed");
    let dead_addr = closed.local_addr().expect("dead addr");
    drop(closed);

    let live = TcpListener::bind("127.0.0.1:0").await.expect("bind live");
    let live_addr = live.local_addr().expect("live addr");
    let accept_task = tokio::spawn(async move {
        let _ = live.accept().await;
    });

    // Put an unreachable TEST-NET-1 IP + closed loopback ahead of the live listener;
    // staggered racing must fall back and connect well under 2 seconds.
    let blackhole_addr = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 25));
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        client::connect_addrs(vec![blackhole_addr, dead_addr, live_addr]),
    )
    .await
    .expect("must not stall on unreachable first address")
    .expect("must connect to working fallback address");
    assert_eq!(stream.peer_addr().expect("peer"), live_addr);
    accept_task.abort();
}

#[tokio::test]
async fn send_and_dispatch_deliver_to_all_to_cc_and_bcc_envelope_recipients() {
    let server = MockSmtp::start(ServerBehavior::default()).await;
    let provider = server.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    let mut req = sample_request();
    req.to_addresses = vec!["to@example.dev".into()];
    req.cc_addresses = vec!["cc@example.dev".into()];
    req.bcc_addresses = vec!["bcc@example.dev".into()];

    let id = provider.send(&req, Deadline::never()).await.expect("send accepted");
    assert_eq!(id, "Q-42");
    assert_eq!(
        provider.dispatch(&req, &correlation()).await,
        DispatchOutcome::Accepted("Q-42".into())
    );

    let sessions = server.wait_sessions(2).await;
    for session in &sessions {
        let rcpts: Vec<&str> =
            session.commands.iter().filter_map(|c| c.strip_prefix("RCPT TO:")).collect();
        assert_eq!(rcpts, vec!["<to@example.dev>", "<cc@example.dev>", "<bcc@example.dev>"]);
        assert!(!session.data_body.contains("bcc@example.dev"), "BCC leaked into DATA");
    }
    server.stop();
}

#[tokio::test]
async fn message_id_fallbacks_invalid_correlation_and_rogue_experiments() {
    use crate::rogue::{Armed, Experiments, Target};
    use rogue_machines::{Fault, Fraction};

    let server =
        MockSmtp::start(ServerBehavior { data_end_reply: "250 OK".into(), ..Default::default() })
            .await;
    let provider = server.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);

    let mut no_mid = sample_request();
    no_mid.raw_mime = "From: alice@example.dev\r\nTo: bob@example.dev\r\n\r\nNo msg id".into();

    // Fallback #3: queued dispatch without queue_id or Message-ID uses smtp:<attempt_id>.
    let corr = correlation();
    assert_eq!(
        provider.dispatch(&no_mid, &corr).await,
        DispatchOutcome::Accepted(format!("smtp:{}", corr.attempt_id))
    );

    // Fallback #4: sync send without queue_id or Message-ID uses smtp:<timestamp>.
    let sync_id = provider.send(&no_mid, Deadline::never()).await.expect("sync fallback id");
    assert!(sync_id.starts_with("smtp:"), "{sync_id}");
    assert!(sync_id.len() <= 256);

    // Invalid correlation refuses without connecting.
    let bad_corr = Correlation { hailer_message_id: String::new(), attempt_id: String::new() };
    assert_eq!(
        provider.dispatch(&no_mid, &bad_corr).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );
    assert_eq!(server.wait_sessions(2).await.len(), 2);

    // Rogue fault injection: Garble prefixes id with rogue-, Raise fails without connecting.
    let experiments = Arc::new(Experiments::default());
    experiments.apply(&[Armed {
        target: Target::Provider,
        fault: Fault::Garble,
        fraction: Fraction::ALL,
        remaining: Duration::from_secs(60),
    }]);
    let rogue_provider = server
        .provider(SmtpTlsMode::None, SmtpAuthMode::None, None)
        .with_experiments(experiments.clone());
    let garbled = rogue_provider.send(&sample_request(), Deadline::never()).await.expect("garble");
    assert_eq!(garbled, "rogue-msg-123@example.dev");

    experiments.apply(&[Armed {
        target: Target::Provider,
        fault: Fault::Raise,
        fraction: Fraction::ALL,
        remaining: Duration::from_secs(60),
    }]);
    assert_matches!(
        rogue_provider.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        rogue_provider.dispatch(&sample_request(), &corr).await,
        DispatchOutcome::NotDispatched("request_not_dispatched")
    );
    assert_eq!(server.wait_sessions(3).await.len(), 3);
    server.stop();
}

#[tokio::test]
async fn auth_4xx_data_4xx_5xx_legacy_auth_eq_and_debug_redaction() {
    let creds = SmtpCredentials { username: "mailer".into(), password: "top-secret-pw".into() };
    let debug = format!("{creds:?}");
    assert!(debug.contains("[REDACTED]"), "{debug}");
    assert!(!debug.contains("top-secret-pw"), "{debug}");

    // 1. AUTH 454 temporary refusal -> Transport / Throttled (retryable) + check() mentions 454.
    let auth_temp =
        MockSmtp::start(ServerBehavior { reject_auth_code: Some(454), ..Default::default() }).await;
    let p = auth_temp.provider(SmtpTlsMode::None, SmtpAuthMode::Plain, Some(creds.clone()));
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
    );
    let err = p.check().await.expect_err("auth 454 check must fail");
    assert!(err.to_string().contains("454"), "{err}");
    auth_temp.stop();

    // 2. Pre-body DATA 451 and 554.
    let data_temp = MockSmtp::start(ServerBehavior {
        data_cmd_reply: Some("451 4.3.0 Storage full".into()),
        ..Default::default()
    })
    .await;
    let p = data_temp.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
    );
    data_temp.stop();

    let data_perm = MockSmtp::start(ServerBehavior {
        data_cmd_reply: Some("554 5.5.1 No valid recipients".into()),
        ..Default::default()
    })
    .await;
    let p = data_perm.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Rejected(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
    );
    data_perm.stop();

    // 3. Post-DATA '.' 451 (retryable) and 554 (permanent).
    let post_temp = MockSmtp::start(ServerBehavior {
        data_end_reply: "451 4.7.1 Greylisted".into(),
        ..Default::default()
    })
    .await;
    let p = post_temp.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Transport(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_throttled", retryable: true }
    );
    post_temp.stop();

    let post_perm = MockSmtp::start(ServerBehavior {
        data_end_reply: "554 5.7.1 Message rejected as spam".into(),
        ..Default::default()
    })
    .await;
    let p = post_perm.provider(SmtpTlsMode::None, SmtpAuthMode::None, None);
    assert_matches!(
        p.send(&sample_request(), Deadline::never()).await,
        Err(ProviderError::Rejected(_))
    );
    assert_eq!(
        p.dispatch(&sample_request(), &correlation()).await,
        DispatchOutcome::NotAccepted { code: "provider_rejected", retryable: false }
    );
    post_perm.stop();

    // 4. Legacy AUTH=LOGIN capability syntax selects AUTH LOGIN in auto mode.
    let legacy =
        MockSmtp::start(ServerBehavior { auth_caps: "AUTH=LOGIN".into(), ..Default::default() })
            .await;
    let p = legacy.provider(SmtpTlsMode::None, SmtpAuthMode::Auto, Some(creds));
    let id = p.send(&sample_request(), Deadline::never()).await.expect("legacy AUTH=LOGIN");
    assert_eq!(id, "Q-42");
    let sessions = legacy.wait_sessions(1).await;
    assert!(sessions[0].commands.iter().any(|c| c == "AUTH LOGIN"));
    legacy.stop();
}

#[tokio::test]
async fn reply_parser_handles_bare_code_mismatched_multiline_and_bad_separator() {
    // 1. Bare 3-digit reply (no trailing space or text) is valid per RFC 5321.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let bare_port = listener.local_addr().expect("addr").port();
    let bare_task = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut io = BufReader::new(stream);
            let _ = io.get_mut().write_all(b"220\r\n").await;
            let mut line = String::new();
            while io.read_line(&mut line).await.unwrap_or(0) > 0 {
                let upper = line.trim().to_ascii_uppercase();
                line.clear();
                if upper.starts_with("EHLO") {
                    let _ = io.get_mut().write_all(b"250\r\n").await;
                } else if upper == "QUIT" {
                    let _ = io.get_mut().write_all(b"221\r\n").await;
                    break;
                }
            }
        }
    });
    let bare_cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(bare_port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let report = SmtpProvider::new(&bare_cfg, None).check().await.expect("bare 3-digit check");
    assert_eq!(report.greeting, "");
    bare_task.abort();

    // 2. Mismatched multiline reply code.
    let (mismatch_port, mismatch_task) =
        start_raw_responder(b"220-first\r\n250 mismatched\r\n".to_vec()).await;
    let mismatch_cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(mismatch_port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let err = SmtpProvider::new(&mismatch_cfg, None).check().await.expect_err("must reject");
    assert!(err.to_string().contains("mismatched"), "{err}");
    mismatch_task.abort();

    // 3. Invalid separator byte after 3-digit code.
    let (sep_port, sep_task) = start_raw_responder(b"220Xbad\r\n".to_vec()).await;
    let sep_cfg = SmtpConfig {
        host: "127.0.0.1".into(),
        port: Some(sep_port),
        tls: SmtpTlsMode::None,
        auth: SmtpAuthMode::None,
        helo_domain: None,
    };
    let err = SmtpProvider::new(&sep_cfg, None).check().await.expect_err("must reject");
    assert!(err.to_string().contains("separator"), "{err}");
    sep_task.abort();
}

#[tokio::test]
#[ignore = "needs HAILER_SMTP_HOST (e.g. Mailpit or staging relay)"]
async fn live_smtp_delivers_sync_and_queued_messages() {
    crate::install_crypto_provider().expect("TLS crypto provider");
    let host = std::env::var("HAILER_SMTP_HOST").expect("HAILER_SMTP_HOST");
    let port = std::env::var("HAILER_SMTP_PORT").ok().and_then(|v| v.parse::<u16>().ok());
    let tls = match std::env::var("HAILER_SMTP_TLS").unwrap_or_else(|_| "starttls".into()).as_str()
    {
        "implicit" => SmtpTlsMode::Implicit,
        "none" => SmtpTlsMode::None,
        _ => SmtpTlsMode::Starttls,
    };
    let auth = match std::env::var("HAILER_SMTP_AUTH").unwrap_or_else(|_| "auto".into()).as_str() {
        "plain" => SmtpAuthMode::Plain,
        "login" => SmtpAuthMode::Login,
        "none" => SmtpAuthMode::None,
        _ => SmtpAuthMode::Auto,
    };
    let creds = match (std::env::var("SMTP_USERNAME").ok(), std::env::var("SMTP_PASSWORD").ok()) {
        (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => {
            Some(SmtpCredentials { username, password })
        }
        _ => None,
    };
    let cfg = SmtpConfig { host, port, tls, auth, helo_domain: Some("hailer.test".into()) };
    let provider = if let Ok(ca_der_path) = std::env::var("HAILER_SMTP_CA_DER") {
        let der = std::fs::read(&ca_der_path).expect("read HAILER_SMTP_CA_DER");
        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(der)).expect("add CA cert");
        let tls_cfg = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("TLS versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        SmtpProvider::with_tls_config(&cfg, creds, Arc::new(tls_cfg))
    } else {
        SmtpProvider::new(&cfg, creds)
    };

    let report = provider.check().await.expect("live smtp-check");
    assert_ne!(report.greeting, "");

    let sync_id = provider.send(&sample_request(), Deadline::never()).await.expect("live sync");
    assert_ne!(sync_id, "");

    let corr = correlation();
    let outcome = provider.dispatch(&sample_request(), &corr).await;
    assert_matches!(outcome, DispatchOutcome::Accepted(ref id) if !id.is_empty());
}
