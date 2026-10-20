//! Dock -> Moored handshake, run once per accepted link.

use std::time::Duration;

use bytes::BytesMut;
use mothership_docking_protocol::{Dock, MessageType, Moored, VERSION, encode_dock};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::bay::reader::next_frame;

/// Send Dock, await Moored, under `deadline`. Returns Moored and whatever
/// followed it in the same read — mothership may pipeline a Hail right
/// after mooring — so the caller's exchange loop picks up where this left
/// off instead of discarding buffered bytes.
pub async fn dock<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    ship_name: &str,
    deadline: Duration,
) -> anyhow::Result<(Moored, BytesMut)> {
    tokio::time::timeout(deadline, dock_unbounded(stream, ship_name))
        .await
        .map_err(|_| anyhow::anyhow!("docking handshake did not complete within {deadline:?}"))?
}

async fn dock_unbounded<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    ship_name: &str,
) -> anyhow::Result<(Moored, BytesMut)> {
    let dock = Dock::new(ship_name).with_pid(std::process::id());
    stream.write_all(&encode_dock(&dock)).await?;
    info!(version = VERSION, ship = %ship_name, "Sent DOCK");

    let mut pending = BytesMut::with_capacity(4096);
    let (msg_type, payload) = next_frame(stream, &mut pending).await?;
    if msg_type != MessageType::Moored {
        anyhow::bail!("expected MOORED, got {msg_type:?}");
    }
    let moored: Moored = serde_json::from_slice(&payload)?;
    info!(config_keys = ?moored.config.keys().collect::<Vec<_>>(), "Docking complete - MOORED");
    Ok((moored, pending))
}
