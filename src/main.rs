//! Hailer CLI/bootstrap.
//!
//! Hailer's HTTP ingress docks with Mothership as an HTTP bay
//! (`[[bays.http]]`) and answers private send/event requests over the Hangar
//! protocol (`bay.rs`). There is no standalone HTTP listener — unlike
//! orbitcast, which can also run without Mothership, hailer always runs
//! under a supervisor that exported `MS_PID`/`MS_SHIP`/`MS_SOCKET_DIR`.
//!
//! # Shutdown
//!
//! Feedback is committed before SQS acknowledgement. Interrupted/unacknowledged
//! receipts are retried by SQS. Shutdown stops admission and drains owned
//! operations within a bounded deadline. Sends interrupted at that deadline
//! can still have ambiguous outcomes and must not be blindly retried.
#![cfg_attr(
    not(all(feature = "ses", feature = "postmark")),
    allow(dead_code, reason = "a single-provider build leaves the other provider's half unused")
)]

#[cfg(not(any(feature = "ses", feature = "postmark")))]
compile_error!("hailer needs a mail provider: build with --features ses, postmark, or both");

#[cfg(feature = "ses")]
mod aws;
mod bay;
mod config;
mod control;
mod domain_block;
mod domain_intel;
mod feedback;
mod mail;
mod operator_cli;
mod outbound;
mod provider;
mod runtime;
mod state;
mod supervisor;
#[cfg(feature = "ses")]
mod suppression_sync;
#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;
mod verify;
mod web;

use std::sync::Arc;

#[cfg(feature = "postmark")]
use provider::postmark::PostmarkProvider;
#[cfg(feature = "ses")]
use provider::ses::SesProvider;
use tracing::{error, info, warn};

use crate::config::{
    Config, ConfigError, DockingConfig, DomainIntelConfig, EmailVerificationConfig, IntelKind,
    VerifierKind,
};
use crate::domain_intel::Intel;
use crate::domain_intel::rdap::RdapClient;
use crate::verify::Verifier;
use crate::verify::emailable::EmailableClient;

#[derive(usage_rs::Cli, Debug)]
#[usage(name = "hailer")]
#[usage(about = "Outbound mail delivery ship for Mothership: SES sender with pluggable backends")]
#[usage(version)]
#[usage(unknown_flags = "error", args_override_self = false)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[usage(short, long, global, env = "HAILER_LOG", default = "info")]
    log_level: String,
    /// Path to the TOML config file (optional; defaults apply when absent)
    #[usage(long, env = "HAILER_CONFIG")]
    config: Option<String>,
    #[usage(subcommand)]
    command: Option<Command>,
}

#[derive(usage_rs::Subcommands, Debug)]
enum Command {
    /// Apply Hailer's PostgreSQL schema migrations without docking
    Migrate,
    /// Import an exported JSON array of suppressions without docking
    ImportSuppressions(ImportArgs),
    /// Merge the current AWS account/region suppression list into PostgreSQL
    SyncSuppressions(SyncArgs),
    /// Inspect durable message status and attempt/recipient facts without AWS
    MessageStatus(MessageIdArgs),
    /// Prevent future sends of a message that has not entered dispatch
    CancelMessage(CancelArgs),
    /// Resolve an uncertain outcome; retry explicitly acknowledges duplicate risk
    ResolveMessage(ResolveArgs),
    /// Add an operator domain/pattern block (exact domain or `*.`-suffix glob)
    BanDomain(BanDomainArgs),
    /// Remove an operator-added block; a curated (repo-shipped) one cannot be removed this way
    UnbanDomain(UnbanDomainArgs),
    /// Let a quarantined message proceed; re-enters the normal dispatch queue immediately
    ReleaseQuarantine(CancelArgs),
    /// Submit up to 1000 addresses (one per line) for async bulk verification, before a send
    VerifyBatch(VerifyBatchArgs),
    /// Poll a submitted batch; caches each verdict once complete so a later real send reuses it
    BatchStatus(BatchStatusArgs),
    /// Check the Emailable account's owner email and remaining verification credits
    EmailableAccount,
    /// Check SES account sending quota, reputation, and enforcement status
    SesAccount,
    /// Check SES verification status for one or more email identities (domains)
    SesIdentities(SesIdentitiesArgs),
    /// Check the Postmark server token and name the server it belongs to
    PostmarkServer,
}

#[derive(usage_rs::Args, Debug)]
struct MessageIdArgs {
    #[usage(long)]
    id: String,
}

#[derive(usage_rs::Args, Debug)]
struct CancelArgs {
    #[usage(long)]
    id: String,
    #[usage(long)]
    reason: String,
}

#[derive(usage_rs::Args, Debug)]
struct ResolveArgs {
    #[usage(long)]
    id: String,
    /// Select a historical unknown attempt instead of the current held message
    #[usage(long)]
    attempt_id: Option<String>,
    #[usage(long)]
    reason: String,
    /// accepted, not-accepted, or retry
    #[usage(long)]
    outcome: String,
    #[usage(long)]
    provider_message_id: Option<String>,
    #[usage(long)]
    acknowledge_duplicate_risk: bool,
}

#[derive(usage_rs::Args, Debug)]
struct BanDomainArgs {
    /// Exact domain ("guerrillamail.com") or `*.`-prefixed suffix glob ("*.kp")
    #[usage(long)]
    pattern: String,
    #[usage(long)]
    reason: Option<String>,
}

#[derive(usage_rs::Args, Debug)]
struct UnbanDomainArgs {
    #[usage(long)]
    pattern: String,
}

#[derive(usage_rs::Args, Debug)]
struct ImportArgs {
    /// Path to the JSON export
    #[usage(long)]
    file: String,
}

#[derive(usage_rs::Args, Debug)]
struct VerifyBatchArgs {
    /// Path to a file with one address per line (blank lines ignored)
    #[usage(long)]
    file: String,
}

#[derive(usage_rs::Args, Debug)]
struct BatchStatusArgs {
    #[usage(long)]
    id: String,
    /// How long a cached verdict stays fresh once this batch completes
    #[usage(long, default = "604800")]
    cache_ttl_seconds: u64,
}

#[derive(usage_rs::Args, Debug)]
struct SyncArgs {
    /// Start a new full merge instead of resuming an interrupted token
    #[usage(long)]
    restart: bool,
}

#[derive(usage_rs::Args, Debug)]
struct SesIdentitiesArgs {
    /// Comma-separated email identities (domains) to check
    #[usage(long)]
    identities: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    install_crypto_provider()?;

    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(&args.log_level)
        .init();
    info!("hailer v{}", env!("CARGO_PKG_VERSION"));

    let config_path = std::path::Path::new(args.config.as_deref().unwrap_or("hailer.toml"));
    anyhow::ensure!(
        args.config.is_none() || config_path.exists(),
        "specified config file is missing"
    );
    let config = Config::load(config_path)?;
    config.validate()?;
    let database_url = std::env::var("HAILER_DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("set HAILER_DATABASE_URL to Hailer's PostgreSQL database"))?;
    let store = state::Store::connect(&database_url, config.database_pool_size).await?;
    if let Some(command) = &args.command
        && operator_cli::run(command, &store).await?
    {
        return Ok(());
    }
    if matches!(args.command, Some(Command::Migrate)) {
        println!("{}", serde_json::json!({"migrated": true}));
        return Ok(());
    }
    if let Some(Command::ImportSuppressions(ImportArgs { file })) = args.command {
        let records = serde_json::from_reader(std::fs::File::open(file)?)?;
        let count = store.import(records).await?;
        println!("{}", serde_json::json!({"imported": count}));
        return Ok(());
    }
    if let Some(command) = &args.command
        && provider_command(command, &store, &config).await?
    {
        return Ok(());
    }

    let docking = DockingConfig::from_env().map_err(|error| {
        error!(%error, "hailer must run under Mothership (MS_PID, MS_SHIP, MS_SOCKET_DIR)");
        anyhow::anyhow!(error)
    })?;

    if docking.mothership_pid == 0 {
        warn!("MS_PID reported 0; Mothership liveness cannot be checked");
    }

    info!(
        ship = %docking.ship_name,
        socket = %docking.socket_path().display(),
        threads = config.threads,
        "Starting hailer"
    );

    let control = Arc::new(
        control::Control::new(
            store.pool.clone(),
            config.rate_limit.as_ref(),
            config.suppression_sync.is_none(),
        )
        .await?,
    );
    let verifier = Arc::new(build_verifier(config.email_verification.as_ref())?);
    let intel = Arc::new(build_intel(config.domain_intel.as_ref()));
    match config.provider {
        #[cfg(feature = "ses")]
        config::ProviderKind::Ses => {
            let aws = Arc::new(aws::Aws::load().await?);
            let provider = Arc::new(SesProvider::new(aws, config.ses_configuration_set.as_deref()));
            supervisor::run(docking, config, provider, store, control, verifier, intel).await?;
        }
        #[cfg(feature = "postmark")]
        config::ProviderKind::Postmark => {
            let provider = Arc::new(postmark(&config)?);
            supervisor::run(docking, config, provider, store, control, verifier, intel).await?;
        }
        #[cfg(not(all(feature = "ses", feature = "postmark")))]
        missing => return Err(ConfigError::MissingFeature(missing.feature()).into()),
    }

    info!("hailer stopped");
    Ok(())
}

/// The commands that talk to a provider; `false` when `command` is not one.
async fn provider_command(
    command: &Command,
    #[cfg_attr(not(feature = "ses"), expect(unused_variables))] store: &state::Store,
    #[cfg_attr(not(feature = "postmark"), expect(unused_variables))] config: &Config,
) -> anyhow::Result<bool> {
    match command {
        #[cfg(feature = "ses")]
        Command::SyncSuppressions(SyncArgs { restart }) => {
            let aws = Arc::new(aws::Aws::load().await?);
            let source = suppression_sync::AwsSuppressions::new(aws).await?;
            let report = suppression_sync::run(&source, store, *restart).await?;
            println!("{}", serde_json::to_string(&report)?);
        }
        #[cfg(feature = "ses")]
        Command::SesAccount => print_ses_account().await?,
        #[cfg(feature = "ses")]
        Command::SesIdentities(SesIdentitiesArgs { identities }) => {
            print_ses_identities(identities).await?;
        }
        #[cfg(not(feature = "ses"))]
        Command::SyncSuppressions(_) | Command::SesAccount | Command::SesIdentities(_) => {
            return Err(ConfigError::MissingFeature("ses").into());
        }
        #[cfg(feature = "postmark")]
        Command::PostmarkServer => {
            let server = postmark(config)?.server().await?;
            println!("{}", serde_json::to_string(&server)?);
        }
        #[cfg(not(feature = "postmark"))]
        Command::PostmarkServer => {
            return Err(ConfigError::MissingFeature("postmark").into());
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// `POSTMARK_SERVER_TOKEN` comes from the environment, like `EMAILABLE_API_KEY`.
#[cfg(feature = "postmark")]
fn postmark(config: &Config) -> anyhow::Result<PostmarkProvider> {
    let token = std::env::var("POSTMARK_SERVER_TOKEN")
        .map_err(|_| ConfigError::MissingEnv("POSTMARK_SERVER_TOKEN"))?;
    Ok(PostmarkProvider::new(token, &config.postmark.clone().unwrap_or_default()))
}

/// `EMAILABLE_API_KEY` is read here, not from `hailer.toml` - matching how
/// AWS credentials come from the standard chain rather than this file.
fn build_verifier(config: Option<&EmailVerificationConfig>) -> anyhow::Result<Verifier> {
    let Some(config) = config else { return Ok(Verifier::Disabled) };
    match config.provider {
        VerifierKind::Emailable => {
            let api_key = std::env::var("EMAILABLE_API_KEY")
                .map_err(|_| ConfigError::MissingEnv("EMAILABLE_API_KEY"))?;
            Ok(Verifier::Emailable(EmailableClient::new(api_key, config.timeout_seconds)))
        }
    }
}

/// RDAP's bootstrap registry is public - unlike Emailable, no credential to
/// read from the environment.
fn build_intel(config: Option<&DomainIntelConfig>) -> Intel {
    match config.map(|config| config.provider) {
        Some(IntelKind::Rdap) => Intel::Rdap(RdapClient::new()),
        None => Intel::Disabled,
    }
}

#[cfg(feature = "ses")]
async fn print_ses_account() -> anyhow::Result<()> {
    let account = aws::ses::account(&aws::Aws::load().await?).await?;
    let quota = account.send_quota.as_ref();
    println!(
        "{}",
        serde_json::json!({
            "dedicated_ip_auto_warmup_enabled": account.dedicated_ip_auto_warmup_enabled,
            "production_access_enabled": account.production_access_enabled,
            "sending_enabled": account.sending_enabled,
            "enforcement_status": account.enforcement_status,
            "max_24_hour_send": quota.and_then(|quota| quota.max_24_hour_send),
            "max_send_rate": quota.and_then(|quota| quota.max_send_rate),
            "sent_last_24_hours": quota.and_then(|quota| quota.sent_last_24_hours),
        })
    );
    Ok(())
}

#[cfg(feature = "ses")]
async fn print_ses_identities(identities: &str) -> anyhow::Result<()> {
    let aws = aws::Aws::load().await?;
    let mut results = serde_json::Map::new();
    for identity in identities.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let value = match aws::ses::identity(&aws, identity).await {
            Ok(found) => serde_json::json!({
                "verified_for_sending_status": found.verified_for_sending_status,
                "verification_status": found.verification_status,
            }),
            Err(aws::AwsError::Service { code, message, .. }) => serde_json::json!({
                "error": message.unwrap_or_else(|| "request failed".to_owned()),
                "code": code,
            }),
            Err(error) => serde_json::json!({"error": error.to_string(), "code": null}),
        };
        results.insert(identity.to_string(), value);
    }
    println!("{}", serde_json::Value::Object(results));
    Ok(())
}

/// rustls is built with ring only; installing it explicitly keeps a future
/// second provider in the tree from making rustls refuse to choose.
fn install_crypto_provider() -> anyhow::Result<()> {
    use rama::tls::rustls::dep::rustls::crypto::{CryptoProvider, ring};
    if CryptoProvider::get_default().is_none() {
        ring::default_provider()
            .install_default()
            .map_err(|_| anyhow::anyhow!("could not install the TLS crypto provider"))?;
    }
    Ok(())
}

/// Resolve on SIGTERM or ctrl-c.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(error) => {
                warn!(%error, "Cannot listen for SIGTERM; ctrl-c only");
                ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = term.recv() => info!("SIGTERM received"),
            () = ctrl_c() => info!("Interrupt received"),
        }
    }

    #[cfg(not(unix))]
    ctrl_c().await;
}

async fn ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "Cannot listen for ctrl-c; an interrupt will not be handled");
        std::future::pending::<()>().await;
    }
}
