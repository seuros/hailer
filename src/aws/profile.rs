//! The shared config and credentials files (`~/.aws/config`,
//! `~/.aws/credentials`). Static keys and `region` only: a profile that
//! needs SSO, a process or role chaining is refused, not half-followed.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::{AwsError, Credentials, Env};

const UNSUPPORTED: &[&str] = &[
    "credential_process",
    "credential_source",
    "role_arn",
    "source_profile",
    "sso_account_id",
    "sso_session",
    "sso_start_url",
    "web_identity_token_file",
];

/// One profile's settings, the credentials file over the config file.
pub struct Profile {
    name: String,
    values: HashMap<String, String>,
}

/// Never prints values: the credentials file holds secret keys.
impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profile").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Profile {
    /// `None` when no profile was asked for and the default one is absent.
    pub fn load(env: &Env) -> Result<Option<Self>, AwsError> {
        let explicit = env.get("AWS_PROFILE");
        let name = explicit.clone().unwrap_or_else(|| "default".to_owned());
        let home = env.get("HOME").map(PathBuf::from);
        let path = |variable: &str, file: &str| {
            env.get(variable)
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|home| home.join(".aws").join(file)))
        };
        let config_section =
            if name == "default" { "default".to_owned() } else { format!("profile {name}") };
        let mut values = HashMap::new();
        if let Some(config) = path("AWS_CONFIG_FILE", "config") {
            values.extend(section(&read(&config)?, &config_section));
        }
        if let Some(credentials) = path("AWS_SHARED_CREDENTIALS_FILE", "credentials") {
            values.extend(section(&read(&credentials)?, &name));
        }
        if values.is_empty() {
            return explicit.map_or(Ok(None), |name| Err(AwsError::MissingProfile(name)));
        }
        Ok(Some(Self { name, values }))
    }

    pub fn region(&self) -> Option<&str> {
        self.values.get("region").map(String::as_str)
    }

    pub fn credentials(&self) -> Result<Option<Credentials>, AwsError> {
        if let (Some(id), Some(secret)) =
            (self.values.get("aws_access_key_id"), self.values.get("aws_secret_access_key"))
        {
            let token = self.values.get("aws_session_token").cloned();
            return Ok(Some(Credentials::new(id, secret, token)));
        }
        UNSUPPORTED.iter().find(|key| self.values.contains_key(**key)).map_or(Ok(None), |key| {
            Err(AwsError::UnsupportedProfile { profile: self.name.clone(), key: (*key).to_owned() })
        })
    }
}

fn read(path: &Path) -> Result<String, AwsError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(AwsError::Credentials(format!("reading {}: {error}", path.display()))),
    }
}

/// The `key = value` pairs under `[name]`. Indented lines are nested
/// settings (`s3 =` blocks) and skipped.
fn section(text: &str, name: &str) -> HashMap<String, String> {
    let mut inside = false;
    let mut values = HashMap::new();
    for line in text.lines() {
        if line.starts_with([' ', '\t']) {
            continue;
        }
        let line = line.trim();
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(header) = line.strip_prefix('[').and_then(|line| line.strip_suffix(']')) {
            inside = header.split_whitespace().collect::<Vec<_>>().join(" ") == name;
        } else if inside && let Some((key, value)) = line.split_once('=') {
            values.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    values
}
