//! IANA bootstrap (<https://data.iana.org/rdap/dns.json>) maps a TLD to its
//! RDAP server; `GET {base}domain/{name}` then answers the registration
//! question. The bootstrap file changes rarely, so it is fetched once and
//! cached for [`BOOTSTRAP_TTL`], not re-fetched per domain lookup.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::time::Duration;

use jiff::Timestamp;
use rama::http::client::DefaultHttpWebClient;
use rama::http::service::client::HttpClientExt;
use rama::http::{BodyExtractExt, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::{DomainFacts, IntelError};

const DEFAULT_BOOTSTRAP_URL: &str = "https://data.iana.org/rdap/dns.json";
const BOOTSTRAP_TTL: Duration = Duration::from_hours(24);
const TIMEOUT: Duration = Duration::from_secs(10);

type BootstrapMap = HashMap<String, Vec<String>>;
type BootstrapCache = Mutex<Option<(Instant, BootstrapMap)>>;

#[derive(Debug, Deserialize)]
struct Bootstrap {
    services: Vec<(Vec<String>, Vec<String>)>,
}

#[derive(Debug, Deserialize)]
struct DomainResponse {
    #[serde(default)]
    events: Vec<Event>,
}

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(rename = "eventAction")]
    event_action: String,
    #[serde(rename = "eventDate")]
    event_date: String,
}

pub struct RdapClient {
    http: DefaultHttpWebClient,
    bootstrap_url: String,
    bootstrap: BootstrapCache,
}

impl RdapClient {
    pub fn new() -> Self {
        Self::with_bootstrap_url(DEFAULT_BOOTSTRAP_URL.to_owned())
    }

    /// Only the test suite overrides this, to point at a loopback fixture
    /// that serves both the bootstrap file and a canned domain response.
    pub fn with_bootstrap_url(bootstrap_url: String) -> Self {
        Self { http: DefaultHttpWebClient::default(), bootstrap_url, bootstrap: Mutex::new(None) }
    }

    async fn bootstrap_map(&self) -> Result<BootstrapMap, IntelError> {
        {
            let cached = self.bootstrap.lock().await;
            if let Some((fetched_at, map)) = cached.as_ref()
                && fetched_at.elapsed() < BOOTSTRAP_TTL
            {
                return Ok(map.clone());
            }
        }
        let response = self
            .http
            .get(&self.bootstrap_url)
            .send_with_timeout(TIMEOUT)
            .await
            .map_err(|error| IntelError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(IntelError::Transport(format!(
                "bootstrap registry returned HTTP {}",
                response.status()
            )));
        }
        let body: Bootstrap = response
            .try_into_json()
            .await
            .map_err(|error| IntelError::InvalidResponse(error.to_string()))?;
        let mut map = HashMap::new();
        for (tlds, urls) in body.services {
            for tld in tlds {
                map.insert(tld.to_ascii_lowercase(), urls.clone());
            }
        }
        let mut cached = self.bootstrap.lock().await;
        *cached = Some((Instant::now(), map.clone()));
        drop(cached);
        Ok(map)
    }

    /// A domain with no recognizable TLD, or a TLD with no RDAP coverage in
    /// the bootstrap registry, resolves to [`DomainFacts::UNKNOWN`] - not an
    /// error. Every "we could not determine this" case fails open the same
    /// way; only a genuine transport/parse failure is an [`IntelError`].
    pub async fn lookup(&self, domain: &str) -> Result<DomainFacts, IntelError> {
        let Some(tld) = domain.rsplit('.').next().filter(|tld| !tld.is_empty()) else {
            return Ok(DomainFacts::UNKNOWN);
        };
        let tld = tld.to_ascii_lowercase();
        let map = self.bootstrap_map().await?;
        let Some(base) = map.get(&tld).and_then(|urls| urls.first()) else {
            return Ok(DomainFacts::UNKNOWN);
        };
        let base = if base.ends_with('/') { base.clone() } else { format!("{base}/") };
        let response = self
            .http
            .get(format!("{base}domain/{domain}"))
            .send_with_timeout(TIMEOUT)
            .await
            .map_err(|error| IntelError::Transport(error.to_string()))?;
        if response.status() == StatusCode::NOT_FOUND {
            // Not registered (or the registry simply never heard of it) -
            // neither case is "new", so this is not treated as an error.
            return Ok(DomainFacts::UNKNOWN);
        }
        if !response.status().is_success() {
            return Err(IntelError::Transport(format!(
                "RDAP server returned HTTP {}",
                response.status()
            )));
        }
        let body: DomainResponse = response
            .try_into_json()
            .await
            .map_err(|error| IntelError::InvalidResponse(error.to_string()))?;
        let registered_at = body
            .events
            .iter()
            .find(|event| event.event_action == "registration")
            .and_then(|event| event.event_date.parse::<Timestamp>().ok());
        Ok(DomainFacts { registered_at })
    }
}
