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

mod bay;
mod config;
mod control;
mod feedback;
mod mail;
mod provider;
mod runtime;
mod state;
mod supervisor;
mod suppression_sync;
#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

use std::sync::Arc;

use provider::MailProvider;
use provider::guard::SuppressionGuard;
use provider::ses::SesProvider;
use tracing::{error, info, warn};

use crate::config::{Config, DockingConfig};

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
    /// Copy a cleanly stopped 0.1 redb file into an empty PostgreSQL target
    MigrateRedb(ImportArgs),
    /// Import an exported JSON array of suppressions without docking
    ImportSuppressions(ImportArgs),
    /// Merge the current AWS account/region suppression list into PostgreSQL
    SyncSuppressions(SyncArgs),
}

#[derive(usage_rs::Args, Debug)]
struct ImportArgs {
    /// Path to the JSON export
    #[usage(long)]
    file: String,
}

#[derive(usage_rs::Args, Debug)]
struct SyncArgs {
    /// Start a new full merge instead of resuming an interrupted token
    #[usage(long)]
    restart: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

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
    if matches!(args.command, Some(Command::Migrate)) {
        println!("{}", serde_json::json!({"migrated": true}));
        return Ok(());
    }
    if let Some(Command::MigrateRedb(ImportArgs { file })) = args.command {
        let report = store.migrate_redb(std::path::Path::new(&file)).await?;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    if let Some(Command::ImportSuppressions(ImportArgs { file })) = args.command {
        let records = serde_json::from_reader(std::fs::File::open(file)?)?;
        let count = store.import(records).await?;
        println!("{}", serde_json::json!({"imported": count}));
        return Ok(());
    }
    if let Some(Command::SyncSuppressions(SyncArgs { restart })) = args.command {
        let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let source = suppression_sync::AwsSuppressions::new(&aws).await?;
        let report = suppression_sync::run(&source, &store, restart).await?;
        println!("{}", serde_json::to_string(&report)?);
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

    let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let control = Arc::new(control::Control::new(
        config.rate_limit.as_ref(),
        config.suppression_sync.is_none(),
    )?);
    let provider: Arc<dyn MailProvider> = Arc::new(SuppressionGuard {
        inner: Arc::new(SesProvider::new(&aws)),
        store: store.clone(),
        control: control.clone(),
    });
    supervisor::run(docking, config, aws, provider, store, control).await?;

    info!("hailer stopped");
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
