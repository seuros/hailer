mod support;

use std::process::Command;

#[tokio::test]
async fn suppression_import_needs_postgres_but_not_mothership_or_aws() {
    let database = support::database().await;
    let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
        .args(["import-suppressions", "--file"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/test/fixtures/suppressions.json"))
        .env("HAILER_DATABASE_URL", database.url())
        .env_remove("MS_PID")
        .env_remove("MS_SHIP")
        .env_remove("HAILER_CONFIG")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .expect("import command");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).expect("import result");
    assert_eq!(result["imported"], 2);
}

#[test]
fn explicit_missing_config_never_silently_disables_feedback() {
    let directory = tempfile::tempdir().expect("directory");
    let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
        .arg("--config")
        .arg(directory.path().join("missing.toml"))
        .env_remove("HAILER_CONFIG")
        .output()
        .expect("command");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("specified config file is missing"));
}

#[test]
fn missing_database_configuration_fails_without_echoing_a_secret() {
    let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
        .arg("migrate")
        .env_remove("HAILER_DATABASE_URL")
        .env_remove("HAILER_CONFIG")
        .output()
        .expect("command");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("set HAILER_DATABASE_URL"));
}

#[tokio::test]
async fn operator_commands_are_available_without_mothership_or_aws() {
    let database = support::database().await;
    for (arguments, expected) in [
        (
            vec!["message-status", "--id", "a74698dd-ecc2-45ac-a4ce-01abf5524578"],
            "message not found",
        ),
        (
            vec!["cancel-message", "--id", "a74698dd-ecc2-45ac-a4ce-01abf5524578", "--reason", ""],
            "nonempty audit reason",
        ),
        (
            vec![
                "resolve-message",
                "--id",
                "a74698dd-ecc2-45ac-a4ce-01abf5524578",
                "--outcome",
                "invalid",
                "--reason",
                "test",
            ],
            "outcome must be",
        ),
        (
            vec![
                "resolve-message",
                "--id",
                "a74698dd-ecc2-45ac-a4ce-01abf5524578",
                "--attempt-id",
                "invalid",
                "--outcome",
                "not-accepted",
                "--reason",
                "test",
            ],
            "invalid attempt ID",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
            .args(arguments)
            .env("HAILER_DATABASE_URL", database.url())
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("MS_PID")
            .env_remove("MS_SHIP")
            .env_remove("HAILER_CONFIG")
            .output()
            .expect("operator command");
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("must run under Mothership"));
    }
}

#[cfg(feature = "smtp")]
#[tokio::test]
async fn smtp_check_runs_without_postgres_or_mothership() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut io = BufReader::new(stream);
            let _ = io.get_mut().write_all(b"220 127.0.0.1 ESMTP Test\r\n").await;
            let mut line = String::new();
            while io.read_line(&mut line).await.unwrap_or(0) > 0 {
                let upper = line.trim().to_ascii_uppercase();
                line.clear();
                if upper.starts_with("EHLO") {
                    let _ = io.get_mut().write_all(b"250-127.0.0.1\r\n250 8BITMIME\r\n").await;
                } else if upper == "QUIT" {
                    let _ = io.get_mut().write_all(b"221 Bye\r\n").await;
                    break;
                }
            }
        }
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("hailer.toml");
    std::fs::write(
        &config_path,
        format!(
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = {port}\ntls = \"none\"\nauth = \"none\"\n"
        ),
    )
    .expect("write config");

    let bin = env!("CARGO_BIN_EXE_hailer");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin)
            .arg("--config")
            .arg(&config_path)
            .arg("smtp-check")
            .env_remove("HAILER_DATABASE_URL")
            .env_remove("HAILER_CONFIG")
            .env_remove("MS_PID")
            .env_remove("MS_SHIP")
            .output()
            .expect("smtp-check command")
    })
    .await
    .expect("join");

    server.abort();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json report");
    assert_eq!(report["host"], "127.0.0.1");
    assert_eq!(report["port"], port);
    assert_eq!(report["authenticated"], false);
    assert_eq!(report["capabilities"], serde_json::json!(["8BITMIME"]));
}

#[cfg(feature = "smtp")]
#[test]
fn smtp_check_validates_credentials_env_and_non_smtp_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bin = env!("CARGO_BIN_EXE_hailer");

    for (name, toml, user, pass, expected) in [
        (
            "plain_missing_user.toml",
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = 1\ntls = \"none\"\nauth = \"plain\"\n",
            None,
            None,
            "SMTP_USERNAME",
        ),
        (
            "login_empty_pass.toml",
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = 1\ntls = \"none\"\nauth = \"login\"\n",
            Some("user"),
            Some(""),
            "SMTP_PASSWORD",
        ),
        (
            "auto_only_user.toml",
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = 1\ntls = \"none\"\nauth = \"auto\"\n",
            Some("user"),
            None,
            "SMTP_PASSWORD",
        ),
        (
            "auto_only_pass.toml",
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = 1\ntls = \"none\"\nauth = \"auto\"\n",
            None,
            Some("secret"),
            "SMTP_USERNAME",
        ),
        ("ses_no_smtp.toml", "provider = \"ses\"\n", None, None, "required with provider"),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, toml).expect("write toml");
        let mut cmd = Command::new(bin);
        cmd.arg("--config")
            .arg(&path)
            .arg("smtp-check")
            .env_remove("HAILER_DATABASE_URL")
            .env_remove("HAILER_CONFIG")
            .env_remove("SMTP_USERNAME")
            .env_remove("SMTP_PASSWORD");
        if let Some(u) = user {
            cmd.env("SMTP_USERNAME", u);
        }
        if let Some(p) = pass {
            cmd.env("SMTP_PASSWORD", p);
        }
        let output = cmd.output().expect("run smtp-check");
        assert!(!output.status.success(), "{name} should fail");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{name}: expected {expected:?} in {stderr}");
    }
}

#[cfg(feature = "smtp")]
#[tokio::test]
async fn smtp_check_with_auth_none_ignores_env_credentials() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let dir = tempfile::tempdir().expect("tempdir");
    let bin = env!("CARGO_BIN_EXE_hailer");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut io = BufReader::new(stream);
            let _ = io.get_mut().write_all(b"220 127.0.0.1 ESMTP Test\r\n").await;
            let mut line = String::new();
            while io.read_line(&mut line).await.unwrap_or(0) > 0 {
                let upper = line.trim().to_ascii_uppercase();
                line.clear();
                if upper.starts_with("EHLO") {
                    let _ =
                        io.get_mut().write_all(b"250-127.0.0.1\r\n250 AUTH PLAIN LOGIN\r\n").await;
                } else if upper.starts_with("AUTH") {
                    let _ = io.get_mut().write_all(b"503 auth not allowed\r\n").await;
                } else if upper == "QUIT" {
                    let _ = io.get_mut().write_all(b"221 Bye\r\n").await;
                    break;
                }
            }
        }
    });

    let no_auth_path = dir.path().join("auth_none.toml");
    std::fs::write(
        &no_auth_path,
        format!(
            "provider = \"smtp\"\n[smtp]\nhost = \"127.0.0.1\"\nport = {port}\ntls = \"none\"\nauth = \"none\"\n"
        ),
    )
    .expect("write auth_none");

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin)
            .arg("--config")
            .arg(&no_auth_path)
            .arg("smtp-check")
            .env_remove("HAILER_DATABASE_URL")
            .env_remove("HAILER_CONFIG")
            .env("SMTP_USERNAME", "ignored-user")
            .env("SMTP_PASSWORD", "ignored-pass")
            .output()
            .expect("auth none check")
    })
    .await
    .expect("join");

    server.abort();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json report");
    assert_eq!(report["authenticated"], false);
}
