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
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bay::reader::next_frame;
use crate::mail::{SendErrorBody, SendRequest, SendResponse};
use crate::provider::MailProvider;
use crate::runtime::Runtime;
use crate::state::Store;

/// Private send and provider-neutral feedback feed routes.
const SEND_PATH: &str = "/internal/mail/send";
const EVENTS_PATH: &str = "/internal/mail/events";
const HEALTH_PATH: &str = "/internal/mail/health";
const READY_PATH: &str = "/internal/mail/ready";
const MESSAGES_PATH: &str = "/internal/mail/messages";
const LIFECYCLE_PATH: &str = "/internal/mail/messages/events";
const INTAKE_READY_PATH: &str = "/internal/mail/intake-ready";
const DISPATCH_READY_PATH: &str = "/internal/mail/dispatch-ready";
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

pub async fn serve_link<S, P: MailProvider>(
    mut stream: S,
    mut pending: BytesMut,
    provider: Arc<P>,
    store: Store,
    runtime: Arc<Runtime>,
    stop: CancellationToken,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let (msg_type, payload) = tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(()),
            result = next_frame(&mut stream, &mut pending) => result?,
        };
        if msg_type != MessageType::Hail {
            anyhow::bail!("expected HAIL, got {msg_type:?}");
        }
        let hail = decode_hail(&payload)?;
        let request_id = hail.request_id;
        let permit = runtime.enter();

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

        let (status, response_body) = if permit.is_none() {
            error_reply(503, "service draining")
        } else if method == b"GET"
            && (path == HEALTH_PATH.as_bytes() || path == READY_PATH.as_bytes())
        {
            let health = runtime.health(store.healthy().await, provider.circuit_state());
            let status = if path == READY_PATH.as_bytes() && !health.ready { 503 } else { 200 };
            (status, serde_json::to_vec(&health)?)
        } else if method == b"GET" && path == DISPATCH_READY_PATH.as_bytes() {
            let ready = runtime.dispatcher_ready(store.healthy().await, provider.circuit_state());
            json_response(if ready { 200 } else { 503 }, &serde_json::json!({"ready": ready}))
        } else if method == b"POST" && path == EVENTS_PATH.as_bytes() {
            event_page(&body, &store).await
        } else if let Some(response) = queued_route(method, path, &body, &store, &runtime).await {
            response
        } else {
            handle(method, path, &body, &provider).await
        };
        write_reply(&mut stream, request_id, status, &response_body).await?;
        stream.write_all(&encode_ready(request_id)).await?;
    }
}

async fn queued_route(
    method: &[u8],
    path: &[u8],
    body: &[u8],
    store: &Store,
    runtime: &Runtime,
) -> Option<(u16, Vec<u8>)> {
    if method == b"GET" && path == INTAKE_READY_PATH.as_bytes() {
        let ready = if let Some(budget) = &runtime.queued {
            !runtime.control.draining() && store.intake_ready(budget).await
        } else {
            false
        };
        return Some(json_response(
            if ready { 200 } else { 503 },
            &serde_json::json!({"ready": ready, "enabled": runtime.queued.is_some()}),
        ));
    }
    if method == b"POST" && path == MESSAGES_PATH.as_bytes() {
        let Some(budget) = &runtime.queued else {
            return Some(error_reply(503, "durable intake is not configured"));
        };
        let Ok(command) = serde_json::from_slice::<crate::outbound::Submission>(body) else {
            return Some(error_reply(400, "invalid submission JSON"));
        };
        return Some(match store.submit(command, budget).await {
            Ok(receipt) => json_response(202, &receipt),
            Err(error) => error_reply(error.status(), &error.to_string()),
        });
    }
    if method == b"POST" && path == LIFECYCLE_PATH.as_bytes() {
        let query = match serde_json::from_slice::<EventQuery>(body) {
            Ok(query) if (1..=100).contains(&query.limit) && i64::try_from(query.after).is_ok() => {
                query
            }
            _ => return Some(error_reply(400, "invalid lifecycle query")),
        };
        return Some(store.message_events(query.after, query.limit).await.map_or_else(
            |_| error_reply(503, "durable mail state unavailable"),
            |page| json_response(200, &page),
        ));
    }
    if method == b"GET"
        && let Some(id) = path.strip_prefix(b"/internal/mail/messages/")
    {
        // IDs are server-generated UUIDs. No arbitrary path or query input.
        if id.len() != 36
            || !id.iter().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    *byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
        {
            return Some(error_reply(404, "message not found"));
        }
        let Ok(id) = std::str::from_utf8(id) else {
            return Some(error_reply(404, "message not found"));
        };
        return Some(match store.message_status(id).await {
            Ok(Some(status)) => json_response(200, &status),
            Ok(None) => error_reply(404, "message not found"),
            Err(_) => error_reply(503, "durable mail state unavailable"),
        });
    }
    None
}

fn json_response(status: u16, value: &impl serde::Serialize) -> (u16, Vec<u8>) {
    serde_json::to_vec(value)
        .map_or_else(|_| error_reply(503, "response serialization failed"), |body| (status, body))
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
        _ => return error_reply(400, "expected after cursor and limit in 1..100"),
    };
    store.events(query.after, query.limit).await.map_or_else(
        |_| error_reply(503, "feedback state unavailable"),
        |page| {
            serde_json::to_vec(&page).map_or_else(
                |_| error_reply(503, "feedback serialization failed"),
                |body| (200, body),
            )
        },
    )
}

fn param<'a>(params: &'a [(Vec<u8>, Vec<u8>)], name: &[u8]) -> Option<&'a [u8]> {
    params.iter().find(|(key, _)| key.as_slice() == name).map(|(_, value)| value.as_slice())
}

async fn handle<P: MailProvider>(
    method: &[u8],
    path: &[u8],
    body: &[u8],
    provider: &Arc<P>,
) -> (u16, Vec<u8>) {
    if method != b"POST" || path != SEND_PATH.as_bytes() {
        return error_reply(404, "not found");
    }

    let request: SendRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return error_reply(400, &error.to_string()),
    };

    match provider.send(&request).await {
        Ok(message_id) => {
            let response = SendResponse { message_id };
            (200, serde_json::to_vec(&response).unwrap_or_default())
        }
        Err(error) => {
            let status = error.status();
            warn!(status, "send failed");
            let body = SendErrorBody { error: error.to_string(), code: error.code() };
            (status, serde_json::to_vec(&body).unwrap_or_default())
        }
    }
}

fn error_body(message: &str) -> Vec<u8> {
    serde_json::to_vec(&SendErrorBody { error: message.to_string(), code: None })
        .unwrap_or_default()
}

/// Every route handler's rejection path: a status paired with a
/// `{"error": message}` body in the shape the Ruby gem understands.
fn error_reply(status: u16, message: &str) -> (u16, Vec<u8>) {
    (status, error_body(message))
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
