use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::mail::SendRequest;
use crate::outbound::Correlation;
use crate::provider::{MailProvider, ProviderError};

/// MOCKED: a provider that accepts everything and records what it was sent.
#[derive(Default)]
pub struct FakeSender(Mutex<Vec<SendRequest>>);

impl FakeSender {
    pub fn sent(&self) -> Vec<SendRequest> {
        self.0.lock().expect("requests").clone()
    }
}

impl MailProvider for FakeSender {
    fn send(
        &self,
        request: &SendRequest,
    ) -> impl std::future::Future<Output = Result<String, ProviderError>> + Send {
        self.0.lock().expect("requests").push(request.clone());
        std::future::ready(Ok("ses-id".into()))
    }
}

pub fn request_to(to: &[&str]) -> SendRequest {
    SendRequest {
        raw_mime: "From: Alice <alice@example.com>\r\nTo: bob@example.com\r\n\r\nHello".into(),
        from_email_address: None,
        to_addresses: to.iter().map(ToString::to_string).collect(),
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

pub fn request() -> SendRequest {
    SendRequest {
        raw_mime: "From: alice@example.dev\r\nTo: bob@example.dev\r\n\r\nTest".into(),
        from_email_address: None,
        to_addresses: vec!["bob@example.dev".into()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

pub fn correlation() -> Correlation {
    Correlation {
        hailer_message_id: "a74698dd-ecc2-45ac-a4ce-01abf5524578".into(),
        attempt_id: "b74698dd-ecc2-45ac-a4ce-01abf5524578".into(),
    }
}

pub struct Recorded {
    pub headers: String,
    pub body: serde_json::Value,
}

pub type Captured = Arc<Mutex<Vec<Recorded>>>;

pub struct Endpoint {
    pub url: String,
    pub calls: Arc<AtomicUsize>,
    pub requests: Captured,
    pub task: tokio::task::JoinHandle<()>,
}

impl Endpoint {
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> std::sync::MutexGuard<'_, Vec<Recorded>> {
        self.requests.lock().expect("requests")
    }

    pub async fn stop(self) {
        self.task.abort();
        assert!(self.task.await.expect_err("fixture stopped").is_cancelled());
    }
}

/// A loopback HTTP server answering every JSON request with `status` and
/// `body`, or hanging up without a response when `disconnect` is set.
pub async fn endpoint(status: u16, body: &'static str, disconnect: bool) -> Endpoint {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener");
    let url = format!("http://{}", listener.local_addr().expect("fixture address"));
    let calls = Arc::new(AtomicUsize::new(0));
    let received = calls.clone();
    let requests: Captured = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("fixture accept");
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let mut head = None;
            loop {
                let count = socket.read(&mut chunk).await.expect("request");
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
                head = head.or_else(|| bytes.windows(4).position(|window| window == b"\r\n\r\n"));
                if let Some(end) = head {
                    let length = String::from_utf8_lossy(&bytes[..end])
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().expect("length"))
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let end = head.expect("headers");
            recorded.lock().expect("requests").push(Recorded {
                headers: String::from_utf8_lossy(&bytes[..end]).into_owned(),
                body: if bytes.len() == end + 4 {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&bytes[end + 4..]).expect("request JSON")
                },
            });
            received.fetch_add(1, Ordering::SeqCst);
            if !disconnect {
                socket.write_all(format!(
                    "HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await.expect("response");
            }
        }
    });
    Endpoint { url, calls, requests, task }
}
