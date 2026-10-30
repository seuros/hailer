//! `GET https://api.emailable.com/v1/verify` - one address per call - plus
//! the async batch endpoints for bulk pre-screening a list before a send,
//! not the real-time send path. <https://emailable.com/docs/api/emails/>

#[cfg(test)]
mod tests;

use std::time::Duration;

use rama::http::BodyExtractExt;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};

use super::{State, Verdict, VerifyError};

/// Emailable sends an explicit JSON `null` for `disposable`/`role` when it
/// could not determine them (observed live for an undeliverable address,
/// not just absent-key omission) - `#[serde(default)]` alone does not cover
/// an explicit `null` against a non-`Option` `bool`, so this is required,
/// not optional defensiveness.
fn null_as_false<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(false))
}

const DEFAULT_BASE_URL: &str = "https://api.emailable.com/v1/verify";
const DEFAULT_BATCH_URL: &str = "https://api.emailable.com/v1/batch";
const DEFAULT_ACCOUNT_URL: &str = "https://api.emailable.com/v1/account";

/// Inline per-address results only come back for batches of at most 1,000
/// (Emailable's own `partial` ceiling); larger batches get a ZIP/CSV
/// `download_file` URL instead. This client deliberately does not fetch or
/// parse that file - no zip/csv dependency for v1 - so it refuses to submit
/// a batch this client could not read the results of.
const MAX_INLINE_BATCH_SIZE: usize = 1000;

pub struct EmailableClient {
    http: DefaultHttpWebClient,
    base_url: String,
    batch_url: String,
    account_url: String,
    api_key: String,
    timeout_seconds: u8,
}

#[derive(Debug, Deserialize)]
struct VerifyResponse {
    state: String,
    reason: String,
    #[serde(default, deserialize_with = "null_as_false")]
    disposable: bool,
    #[serde(default, deserialize_with = "null_as_false")]
    role: bool,
}

#[derive(Debug, Deserialize)]
struct BatchSubmitResponse {
    id: String,
}

/// `GET /v1/account` - owner identity and remaining verification credits.
/// Checked before [`EmailableClient::submit_batch`] submits anything, so an
/// out-of-credit account never wastes a request (or burns whatever credits
/// remain) on a batch that cannot complete.
#[derive(Debug, Clone, Deserialize)]
pub struct Account {
    pub owner_email: String,
    pub available_credits: u64,
}

#[derive(Debug, Deserialize)]
struct BatchEmailResult {
    email: String,
    state: String,
    reason: String,
    #[serde(default, deserialize_with = "null_as_false")]
    disposable: bool,
    #[serde(default, deserialize_with = "null_as_false")]
    role: bool,
}

#[derive(Debug, Deserialize)]
struct TotalCounts {
    #[serde(default)]
    processed: u32,
    #[serde(default)]
    total: u32,
}

/// `processed`/`total` are only observed top-level while still processing;
/// a completed batch's real response carries them nested under
/// `total_counts` instead, despite the docs implying both are always
/// top-level - verified live against the real API, not just its docs.
#[derive(Debug, Deserialize)]
struct BatchStatusResponse {
    #[serde(default)]
    processed: Option<u32>,
    #[serde(default)]
    total: Option<u32>,
    #[serde(default)]
    total_counts: Option<TotalCounts>,
    #[serde(default)]
    emails: Option<Vec<BatchEmailResult>>,
    #[serde(default)]
    download_file: Option<String>,
}

impl BatchStatusResponse {
    fn progress(&self) -> (u32, u32) {
        self.processed
            .zip(self.total)
            .or_else(|| self.total_counts.as_ref().map(|counts| (counts.processed, counts.total)))
            .unwrap_or((0, 0))
    }
}

/// A batch's lifecycle, as reported by `GET /v1/batch?id=...`. `emails` and
/// `download_file` are mutually exclusive and only appear once Emailable's
/// own "Batch verification completed" state is reached.
pub enum BatchStatus {
    Processing {
        processed: u32,
        total: u32,
    },
    Complete {
        verdicts: Vec<(String, Verdict)>,
    },
    /// Over 1,000 addresses: real, but this client does not read it.
    TooLargeForInlineResults {
        download_file: String,
        total: u32,
    },
}

impl EmailableClient {
    pub fn new(api_key: String, timeout_seconds: u8) -> Self {
        Self::with_urls(
            api_key,
            timeout_seconds,
            DEFAULT_BASE_URL.to_owned(),
            DEFAULT_BATCH_URL.to_owned(),
            DEFAULT_ACCOUNT_URL.to_owned(),
        )
    }

    /// Only the test suite overrides `base_url`, to point at a loopback
    /// fixture instead of the real Emailable API.
    #[cfg(test)]
    pub fn with_base_url(api_key: String, timeout_seconds: u8, base_url: String) -> Self {
        Self::with_urls(
            api_key,
            timeout_seconds,
            base_url,
            DEFAULT_BATCH_URL.to_owned(),
            DEFAULT_ACCOUNT_URL.to_owned(),
        )
    }

    pub fn with_urls(
        api_key: String,
        timeout_seconds: u8,
        base_url: String,
        batch_url: String,
        account_url: String,
    ) -> Self {
        Self {
            http: DefaultHttpWebClient::default(),
            base_url,
            batch_url,
            account_url,
            api_key,
            timeout_seconds,
        }
    }

    /// A couple of seconds of local headroom over Emailable's own `timeout`
    /// query param, so our own client times out second.
    fn timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.timeout_seconds) + 3)
    }

    /// `GET url?query`. The API key rides in the query, so a transport
    /// error is reported without the URL.
    async fn get(
        &self,
        url: &str,
        query: &[(&str, &str)],
    ) -> Result<rama::http::Response, VerifyError> {
        self.http
            .get(format!("{url}?{}", encode_query(query)))
            .send_with_timeout(self.timeout())
            .await
            .map_err(|_| VerifyError::Transport("emailable request failed".into()))
    }

    /// `GET /v1/account` - owner email and remaining credits.
    pub async fn account(&self) -> Result<Account, VerifyError> {
        let response = self.get(&self.account_url, &[("api_key", self.api_key.as_str())]).await?;
        if !response.status().is_success() {
            return Err(VerifyError::Transport(format!(
                "emailable account lookup returned HTTP {}",
                response.status()
            )));
        }
        json(response).await
    }

    pub async fn verify(&self, address: &str) -> Result<Verdict, VerifyError> {
        let timeout = self.timeout_seconds.to_string();
        let response = self
            .get(
                &self.base_url,
                &[("email", address), ("api_key", self.api_key.as_str()), ("timeout", &timeout)],
            )
            .await?;
        // Emailable's own "exceeded timeout, retry within 5 minutes" status.
        // Treated as a transport failure: the caller's policy decides
        // whether that means fail-open (Unknown) or fail-closed.
        if response.status().as_u16() == 249 {
            return Err(VerifyError::Transport("emailable timed out server-side (249)".into()));
        }
        if !response.status().is_success() {
            return Err(VerifyError::Transport(format!(
                "emailable returned HTTP {}",
                response.status()
            )));
        }
        let body: VerifyResponse = json(response).await?;
        Ok(Verdict {
            state: State::parse(&body.state),
            reason: body.reason,
            disposable: body.disposable,
            role: body.role,
        })
    }

    /// Submits up to [`MAX_INLINE_BATCH_SIZE`] addresses for async bulk
    /// verification. Returns the batch ID to poll with [`Self::batch_status`].
    pub async fn submit_batch(&self, addresses: &[String]) -> Result<String, VerifyError> {
        if addresses.is_empty() {
            return Err(VerifyError::InvalidRequest("batch must not be empty".into()));
        }
        if addresses.len() > MAX_INLINE_BATCH_SIZE {
            return Err(VerifyError::InvalidRequest(format!(
                "batch of {} exceeds this client's {MAX_INLINE_BATCH_SIZE}-address inline-results \
                 limit; split into smaller batches",
                addresses.len()
            )));
        }
        // Checked before submitting, not after a failed/partial batch: an
        // out-of-credit (or insufficient-for-this-batch) account never
        // reaches the server at all.
        let account = self.account().await?;
        if account.available_credits == 0 || account.available_credits < addresses.len() as u64 {
            return Err(VerifyError::InsufficientCredits {
                available: account.available_credits,
                needed: addresses.len(),
            });
        }
        let response = self
            .http
            .post(&self.batch_url)
            .json(&serde_json::json!({"emails": addresses, "api_key": self.api_key}))
            .send_with_timeout(self.timeout())
            .await
            .map_err(|_| VerifyError::Transport("emailable request failed".into()))?;
        if !response.status().is_success() {
            return Err(VerifyError::Transport(format!(
                "emailable batch submission returned HTTP {}",
                response.status()
            )));
        }
        let body: BatchSubmitResponse = json(response).await?;
        Ok(body.id)
    }

    /// One poll of a batch's status. `emails`/`download_file` only appear
    /// once Emailable's own "Batch verification completed" state is reached
    /// - until then this reports [`BatchStatus::Processing`].
    pub async fn batch_status(&self, id: &str) -> Result<BatchStatus, VerifyError> {
        let response =
            self.get(&self.batch_url, &[("id", id), ("api_key", self.api_key.as_str())]).await?;
        if !response.status().is_success() {
            return Err(VerifyError::Transport(format!(
                "emailable batch status returned HTTP {}",
                response.status()
            )));
        }
        let body: BatchStatusResponse = json(response).await?;
        let (processed, total) = body.progress();
        if let Some(emails) = body.emails {
            let verdicts = emails
                .into_iter()
                .map(|item| {
                    (
                        item.email,
                        Verdict {
                            state: State::parse(&item.state),
                            reason: item.reason,
                            disposable: item.disposable,
                            role: item.role,
                        },
                    )
                })
                .collect();
            return Ok(BatchStatus::Complete { verdicts });
        }
        if let Some(download_file) = body.download_file {
            return Ok(BatchStatus::TooLargeForInlineResults { download_file, total });
        }
        Ok(BatchStatus::Processing { processed, total })
    }
}

async fn json<T: DeserializeOwned + Send + 'static>(
    response: rama::http::Response,
) -> Result<T, VerifyError> {
    response.try_into_json().await.map_err(|error| VerifyError::InvalidResponse(error.to_string()))
}

/// `application/x-www-form-urlencoded` pairs: an address's `+` must not
/// arrive as a space.
fn encode_query(pairs: &[(&str, &str)]) -> String {
    let encode = |value: &str| {
        value
            .bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    char::from(byte).to_string()
                }
                _ => format!("%{byte:02X}"),
            })
            .collect::<String>()
    };
    pairs
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
