use std::assert_matches;

use super::*;

fn files(config: &str, credentials: &str, profile: Option<&str>) -> (tempfile::TempDir, Env) {
    let dir = tempfile::tempdir().expect("dir");
    let config_path = dir.path().join("config");
    let credentials_path = dir.path().join("credentials");
    std::fs::write(&config_path, config).expect("config");
    std::fs::write(&credentials_path, credentials).expect("credentials");
    let mut pairs = vec![
        ("AWS_CONFIG_FILE", config_path.to_str().expect("path").to_owned()),
        ("AWS_SHARED_CREDENTIALS_FILE", credentials_path.to_str().expect("path").to_owned()),
    ];
    if let Some(profile) = profile {
        pairs.push(("AWS_PROFILE", profile.to_owned()));
    }
    let pairs: Vec<(&str, &str)> =
        pairs.iter().map(|(name, value)| (*name, value.as_str())).collect();
    let env = Env::fixed(&pairs);
    (dir, env)
}

#[test]
fn a_named_profile_merges_both_files() {
    let (_dir, env) = files(
        "# managed\n[default]\nregion = eu-west-1\n\n[profile pre]\nregion = us-east-2\ns3 =\n  max_concurrent_requests = 20\n",
        "[default]\naws_access_key_id = DEFAULT\naws_secret_access_key = d\n[pre]\naws_access_key_id = PRE\naws_secret_access_key = p\naws_session_token = t\n",
        Some("pre"),
    );
    let profile = Profile::load(&env).expect("loads").expect("profile");
    assert_eq!(profile.region(), Some("us-east-2"));
    let credentials = profile.credentials().expect("supported").expect("keys");
    assert_eq!(credentials.access_key_id, "PRE");
    assert_eq!(credentials.session_token.as_deref(), Some("t"));
}

#[test]
fn the_default_profile_is_optional_but_a_named_one_is_not() {
    let (_dir, env) = files("", "", None);
    assert!(Profile::load(&env).expect("loads").is_none());
    let (_dir, env) =
        files("", "[default]\naws_access_key_id = A\naws_secret_access_key = B\n", Some("nope"));
    assert_matches!(Profile::load(&env), Err(AwsError::MissingProfile(name)) if name == "nope");
}

#[test]
fn profiles_hailer_cannot_follow_are_refused() {
    for (config, key) in [
        (
            "[profile chained]\nrole_arn = arn:aws:iam::1:role/x\nsource_profile = default\n",
            "role_arn",
        ),
        ("[profile chained]\nsso_session = corp\n", "sso_session"),
        ("[profile chained]\ncredential_process = /bin/creds\n", "credential_process"),
    ] {
        let (_dir, env) = files(config, "", Some("chained"));
        let profile = Profile::load(&env).expect("loads").expect("profile");
        assert_matches!(
            profile.credentials(),
            Err(AwsError::UnsupportedProfile { key: found, .. }) if found == key,
            "{config}"
        );
    }
}
