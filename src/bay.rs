//! Mothership's HTTP-bay (Hangar) protocol, ship side.
//!
//! No existing Rust code in the fleet implements this side of the protocol:
//! shuttlebay is the app-side precedent but embeds its native engine inside
//! the Ruby/Rack process via magnus, and orbitcast only implements the
//! simpler Socket-side Dock/Moored + Cargo loop
//! (`orbitcast/src/transport/docking.rs`). This module writes the Hangar
//! side directly from the protocol crate's frame types, following the same
//! Dock/Moored handshake shape orbitcast uses and the Hail/Haul/Reply/Ready
//! exchange `mothership/src/docking/hangar.rs` expects back.
//!
//! One Unix socket, bound once; mothership opens `threads` separate
//! connections to it (one per docking link), each served by its own task
//! for the life of the process.

mod exchange;
mod handshake;
mod reader;

use std::sync::Arc;

use tokio::net::{UnixListener, UnixStream};
use tracing::{error, info};

use crate::config::DockingConfig;
use crate::provider::MailProvider;
use crate::state::Store;

/// Bind the docking socket and serve links until the process exits. A link
/// that drops is not re-accepted under the same slot — mothership relaunches
/// the whole process if it loses more links than it can spare.
pub async fn serve(
    docking: DockingConfig,
    provider: Arc<dyn MailProvider>,
    store: Store,
) -> anyhow::Result<()> {
    let socket_path = docking.socket_path();
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(socket = %socket_path.display(), "Listening for Mothership connection");

    loop {
        let (stream, _) = listener.accept().await?;
        info!("Mothership opened a docking link");
        let ship_name = docking.ship_name.clone();
        let provider = Arc::clone(&provider);
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_link(stream, &ship_name, provider, store).await {
                error!(%error, "Docking link ended");
            }
        });
    }
}

async fn handle_link(
    mut stream: UnixStream,
    ship_name: &str,
    provider: Arc<dyn MailProvider>,
    store: Store,
) -> anyhow::Result<()> {
    let (moored, pending) =
        handshake::dock(&mut stream, ship_name, std::time::Duration::from_secs(10)).await?;
    info!(config_keys = ?moored.config.keys().collect::<Vec<_>>(), "Docking link established");
    exchange::serve_link(stream, pending, provider, store).await
}
