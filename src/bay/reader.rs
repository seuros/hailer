//! Shared frame reader: splits one complete docking-protocol frame off a
//! growing read buffer, mirroring
//! `orbitcast/src/transport/docking.rs::take_frame` (the header length isn't
//! part of the protocol crate's public API, so it's redefined here the same
//! way orbitcast does).

use bytes::{Buf, Bytes, BytesMut};
use mothership_docking_protocol::{MAX_FRAME_PAYLOAD, MessageType, decode_header};
use tokio::io::{AsyncRead, AsyncReadExt};

const HEADER_LEN: usize = 5;

/// Split one complete frame off `pending`, if it holds one. The payload
/// shares the read buffer; nothing is copied.
fn take_frame(pending: &mut BytesMut) -> anyhow::Result<Option<(MessageType, Bytes)>> {
    if pending.len() < HEADER_LEN {
        return Ok(None);
    }
    let (msg_type, payload_len) = decode_header(pending)?;
    if payload_len > MAX_FRAME_PAYLOAD {
        anyhow::bail!(
            "{msg_type:?} frame of {payload_len} bytes exceeds the {MAX_FRAME_PAYLOAD}-byte limit"
        );
    }
    let total_len = HEADER_LEN + payload_len;
    if pending.len() < total_len {
        pending.reserve(total_len - pending.len());
        return Ok(None);
    }
    pending.advance(HEADER_LEN);
    Ok(Some((msg_type, pending.split_to(payload_len).freeze())))
}

/// Read from `stream` into `pending` until one full frame is available.
pub async fn next_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    pending: &mut BytesMut,
) -> anyhow::Result<(MessageType, Bytes)> {
    loop {
        if let Some(frame) = take_frame(pending)? {
            return Ok(frame);
        }
        if stream.read_buf(pending).await? == 0 {
            anyhow::bail!("docking link closed mid-frame");
        }
    }
}
