use std::process::Command;

#[test]
fn suppression_import_runs_offline_without_mothership_or_aws_credentials() {
    let directory = tempfile::tempdir().expect("directory");
    let state = directory.path().join("state.redb");
    let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
        .args(["--state-path"])
        .arg(&state)
        .args(["import-suppressions", "--file"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/test/fixtures/suppressions.json"))
        .env_remove("MS_PID")
        .env_remove("MS_SHIP")
        .env_remove("HAILER_CONFIG")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .expect("import command");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).expect("import result");
    assert_eq!(result["imported"], 2);
    assert!(state.is_file());
}

#[test]
fn explicit_missing_config_never_silently_disables_feedback_or_creates_empty_state() {
    let directory = tempfile::tempdir().expect("directory");
    let state = directory.path().join("state.redb");
    let output = Command::new(env!("CARGO_BIN_EXE_hailer"))
        .arg("--config")
        .arg(directory.path().join("missing.toml"))
        .arg("--state-path")
        .arg(&state)
        .output()
        .expect("command");
    assert!(!output.status.success());
    assert!(!state.exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("specified config file is missing"));
}
