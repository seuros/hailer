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
