//! One Hangar link: Hail -> (Haul*) -> Reply -> (Haul*) -> Ready, repeated
//! for the life of the connection. A link carries exactly one request at a
//! time (the protocol crate's own module doc states this), so this loop
//! never interleaves two exchanges.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use bytes::BytesMut;
use mothership_docking_protocol::{
    MessageType, Reply, ReplyBody, decode_hail, decode_haul, encode_ready, encode_reply, push_haul,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

use crate::bay::reader::next_frame;
use crate::mail::{SendErrorBody, SendRequest, SendResponse};
use crate::provider::{MailProvider, ProviderError};
use crate::state::Store;

/// Private send and provider-neutral feedback feed routes.
const SEND_PATH: &str = "/internal/mail/send";
const EVENTS_PATH: &str = "/internal/mail/events";
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

pub async fn serve_link<S>(
    mut stream: S,
    mut pending: BytesMut,
    provider: Arc<dyn MailProvider>,
    store: Store,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let (msg_type, payload) = next_frame(&mut stream, &mut pending).await?;
        if msg_type != MessageType::Hail {
            anyhow::bail!("expected HAIL, got {msg_type:?}");
        }
        let hail = decode_hail(&payload)?;
        let request_id = hail.request_id;

        let body = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            read_body(&mut stream, &mut pending, request_id, hail.content_length),
        )
        .await
        .map_err(|_| anyhow::anyhow!("request body read timed out"))??;

        let method = param(&hail.params, b"REQUEST_METHOD").unwrap_or_default();
        let path = param(&hail.params, b"PATH_INFO").unwrap_or_default();
        debug!(
            request_id,
            method = %String::from_utf8_lossy(method),
            path = %String::from_utf8_lossy(path),
            "HAIL"
        );

        let (status, response_body) = if method == b"POST" && path == EVENTS_PATH.as_bytes() {
            event_page(&body, &store).await
        } else {
            handle(method, path, &body, &provider).await
        };
        write_reply(&mut stream, request_id, status, &response_body).await?;
        stream.write_all(&encode_ready(request_id)).await?;
    }
}

async fn read_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    pending: &mut BytesMut,
    request_id: u32,
    content_length: u64,
) -> anyhow::Result<Vec<u8>> {
    let length = usize::try_from(content_length)?;
    anyhow::ensure!(length <= MAX_REQUEST_BYTES, "request body exceeds size limit");
    let mut body = Vec::with_capacity(length.min(65_536));
    if length == 0 {
        return Ok(body);
    }
    loop {
        let (kind, payload) = next_frame(stream, pending).await?;
        anyhow::ensure!(kind == MessageType::Haul, "expected HAUL, got {kind:?}");
        let chunk = decode_haul(&payload)?;
        anyhow::ensure!(chunk.request_id == request_id, "HAUL request id does not match HAIL");
        anyhow::ensure!(
            chunk.data.len() <= length - body.len(),
            "HAUL exceeds declared body length"
        );
        body.extend_from_slice(chunk.data);
        if chunk.fin {
            anyhow::ensure!(body.len() == length, "FIN before declared body length");
            return Ok(body);
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EventQuery {
    #[serde(default)]
    after: u64,
    #[serde(default = "default_page_limit")]
    limit: usize,
}

const fn default_page_limit() -> usize {
    100
}

async fn event_page(body: &[u8], store: &Store) -> (u16, Vec<u8>) {
    let query: EventQuery = match serde_json::from_slice::<EventQuery>(body) {
        Ok(query) if (1..=100).contains(&query.limit) => query,
        _ => return (400, error_body("expected after cursor and limit in 1..100")),
    };
    match store.events(query.after, query.limit).await {
        Ok(page) => serde_json::to_vec(&page).map_or_else(
            |_| (503, error_body("feedback serialization failed")),
            |body| (200, body),
        ),
        Err(_) => (503, error_body("feedback state unavailable")),
    }
}

fn param<'a>(params: &'a [(Vec<u8>, Vec<u8>)], name: &[u8]) -> Option<&'a [u8]> {
    params.iter().find(|(key, _)| key.as_slice() == name).map(|(_, value)| value.as_slice())
}

async fn handle(
    method: &[u8],
    path: &[u8],
    body: &[u8],
    provider: &Arc<dyn MailProvider>,
) -> (u16, Vec<u8>) {
    if method != b"POST" || path != SEND_PATH.as_bytes() {
        return (404, br#"{"error":"not found"}"#.to_vec());
    }

    let request: SendRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return (400, error_body(&error.to_string())),
    };

    match provider.send(&request).await {
        Ok(message_id) => {
            let response = SendResponse { message_id };
            (200, serde_json::to_vec(&response).unwrap_or_default())
        }
        Err(error) => {
            warn!(%error, "send failed");
            let status = match error {
                ProviderError::InvalidAddress(_)
                | ProviderError::NoRecipients
                | ProviderError::Suppressed => 422,
                ProviderError::Rejected(_) => 502,
                ProviderError::Transport(_) => 503,
            };
            (status, error_body(&error.to_string()))
        }
    }
}

fn error_body(message: &str) -> Vec<u8> {
    serde_json::to_vec(&SendErrorBody { error: message.to_string() }).unwrap_or_default()
}

async fn write_reply<S: AsyncWrite + Unpin>(
    stream: &mut S,
    request_id: u32,
    status: u16,
    body: &[u8],
) -> anyhow::Result<()> {
    let headers = vec![
        (b"content-type".to_vec(), b"application/json".to_vec()),
        (b"content-length".to_vec(), body.len().to_string().into_bytes()),
    ];
    let reply_body = if body.is_empty() { ReplyBody::Empty } else { ReplyBody::Stream };
    let reply = Reply { request_id, status, headers, body: reply_body };
    stream.write_all(&encode_reply(&reply)?).await?;
    if !body.is_empty() {
        let mut buf = Vec::new();
        let mut chunks = body.chunks(65_536).peekable();
        while let Some(chunk) = chunks.next() {
            buf.clear();
            push_haul(&mut buf, request_id, chunks.peek().is_none(), chunk)?;
            stream.write_all(&buf).await?;
        }
    }
    Ok(())
}
