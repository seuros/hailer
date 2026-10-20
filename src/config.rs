//! TOML + environment configuration, validated at the process boundary
//! (`Config::validate`).
//!
//! The docking parameters (`MS_PID`/`MS_SHIP`/`MS_SOCKET_DIR`) are exported
//! by mothership for every bay it launches; the reader here mirrors
//! `orbitcast/src/config.rs::docking_from` exactly, since both ships dock
//! against the same supervisor contract.

use std::env;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing environment variable {0}")]
    MissingEnv(&'static str),
    #[error("environment variable {0} is invalid: {1}")]
    InvalidEnv(&'static str, &'static str),
    #[error("configuration {0} is invalid: {1}")]
    InvalidConfig(&'static str, &'static str),
}

/// Where mothership expects this ship to listen, and how it identifies
/// itself during the Dock/Moored handshake.
#[derive(Debug, Clone)]
pub struct DockingConfig {
    pub mothership_pid: u32,
    pub ship_name: String,
    pub socket_dir: PathBuf,
}

impl DockingConfig {
    /// The Unix socket this process must listen on for mothership to dial.
    pub fn socket_path(&self) -> PathBuf {
        self.socket_dir.join(format!("{}.sock", self.ship_name))
    }

    /// Read the docking parameters mothership exported for this process.
    ///
    /// `MS_SOCKET_DIR` falls back to `$XDG_RUNTIME_DIR/mothership` (or the
    /// system temp dir) when mothership did not set it, matching
    /// orbitcast's `default_socket_dir`.
    pub fn from_env() -> Result<Self, ConfigError> {
        let pid = env::var("MS_PID").map_err(|_| ConfigError::MissingEnv("MS_PID"))?;
        let ship_name = env::var("MS_SHIP").map_err(|_| ConfigError::MissingEnv("MS_SHIP"))?;
        let mothership_pid: u32 =
            pid.parse().map_err(|_| ConfigError::InvalidEnv("MS_PID", "expected u32"))?;
        let socket_dir = env::var("MS_SOCKET_DIR").map_or_else(
            |_| {
                env::var("XDG_RUNTIME_DIR")
                    .map_or_else(|_| env::temp_dir(), PathBuf::from)
                    .join("mothership")
            },
            PathBuf::from,
        );
        Ok(Self { mothership_pid, ship_name, socket_dir })
    }
}

/// Everything else hailer needs. SES region/credentials come from the
/// standard AWS credential chain (`aws-config`), not from this file — there
/// is deliberately little configuration surface here.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Docking links this process accepts. Must match `threads` in this
    /// ship's `[[bays.http]]` entry in `ship-manifest.toml` — mothership
    /// opens exactly that many connections, and a mismatch here only shows
    /// up as unused or refused links, so it's worth getting right.
    #[serde(default = "default_threads")]
    pub threads: u16,
    #[serde(default = "default_pool_size")]
    pub database_pool_size: u32,
    pub feedback: Option<FeedbackConfig>,
    pub suppression_sync: Option<SuppressionSyncConfig>,
    pub rate_limit: Option<RateLimitConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    pub recipients_per_second: f64,
    pub burst: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuppressionSyncConfig {
    #[serde(default = "default_sync_interval")]
    pub interval_seconds: u64,
}

const fn default_sync_interval() -> u64 {
    3600
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackConfig {
    pub queue_url: String,
    pub topic_arn: String,
}

const fn default_pool_size() -> u32 {
    5
}

const fn default_threads() -> u16 {
    2
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: default_threads(),
            database_pool_size: default_pool_size(),
            feedback: None,
            suppression_sync: None,
            rate_limit: None,
        }
    }
}

impl Config {
    /// Load from `path` if it exists, else fall back to defaults.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&raw)?;
        config.validate().map_err(|error| anyhow::anyhow!(error))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.threads == 0 {
            return Err(ConfigError::InvalidEnv("threads", "must be at least 1"));
        }
        if !(1..=64).contains(&self.database_pool_size) {
            return Err(ConfigError::InvalidConfig("database_pool_size", "must be in 1..64"));
        }
        if self.rate_limit.as_ref().is_some_and(|rate| {
            !rate.recipients_per_second.is_finite()
                || rate.recipients_per_second <= 0.0
                || rate.recipients_per_second > 1_000_000.0
                || !(1..=100_000).contains(&rate.burst)
        }) {
            return Err(ConfigError::InvalidConfig("rate_limit", "invalid rate or burst"));
        }
        if self
            .suppression_sync
            .as_ref()
            .is_some_and(|config| !(60..=86_400).contains(&config.interval_seconds))
        {
            return Err(ConfigError::InvalidConfig(
                "suppression_sync.interval_seconds",
                "must be in 60..86400",
            ));
        }
        if let Some(feedback) = &self.feedback {
            if !feedback.queue_url.starts_with("https://") {
                return Err(ConfigError::InvalidConfig(
                    "feedback.queue_url",
                    "expected HTTPS SQS URL",
                ));
            }
            let parts: Vec<_> = feedback.topic_arn.split(':').collect();
            if parts.len() != 6
                || parts[0] != "arn"
                || parts[2] != "sns"
                || parts[3].is_empty()
                || parts[4].len() != 12
                || !parts[4].bytes().all(|byte| byte.is_ascii_digit())
                || parts[5].is_empty()
            {
                return Err(ConfigError::InvalidConfig(
                    "feedback.topic_arn",
                    "expected SNS topic ARN",
                ));
            }
        }
        Ok(())
    }
}
