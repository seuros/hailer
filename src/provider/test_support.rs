use std::sync::Mutex;

use deadline_machines::Deadline;

use crate::mail::SendRequest;
use crate::provider::{MailProvider, ProviderError};

#[cfg(any(feature = "ses", feature = "postmark"))]
mod http;
#[cfg(feature = "ses")]
pub use http::routed;
#[cfg(any(feature = "ses", feature = "postmark"))]
pub use http::{Endpoint, correlation, endpoint, request};

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
        _: Deadline,
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
