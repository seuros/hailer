//! Read-only AWS reconciliation. This is never on the delivery hot path.

mod aws;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use anyhow::ensure;
use chrono_machines::Policy;
use serde::Serialize;
use sqlx::postgres::PgAdvisoryLock;
use sqlx::{Connection, Either};

use crate::state::{Store, Suppression};

pub use aws::AwsSuppressions;

#[derive(Debug)]
pub struct Page {
    pub records: Vec<Suppression>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    #[error("AWS suppression pagination token expired; rerun with --restart")]
    InvalidToken,
    #[error("AWS suppression list temporarily unavailable")]
    Unavailable,
    #[error("AWS suppression list access rejected")]
    Rejected,
    #[error("invalid AWS suppression page")]
    InvalidPage,
}

pub trait Source: Send + Sync {
    fn scope(&self) -> &str;
    fn page_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn page(
        &self,
        next_token: Option<&str>,
    ) -> impl Future<Output = Result<Page, ReadError>> + Send;
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub pages: u64,
    pub merged: u64,
    pub completed: bool,
}

pub async fn run(source: &impl Source, store: &Store, restart: bool) -> anyhow::Result<Report> {
    ensure!(
        source.scope().len() <= 256 && !source.scope().is_empty(),
        "invalid suppression source scope"
    );
    // Own the physical connection: if cancellation hits while acquiring the
    // session lock, dropping it closes the connection instead of returning a
    // possibly locked session to the pool.
    let connection = store.pool.acquire().await?.detach();
    let lock = PgAdvisoryLock::new(format!("hailer:suppression-sync:{}", source.scope()));
    let guard = match lock.try_acquire(connection).await? {
        Either::Left(guard) => guard,
        Either::Right(connection) => {
            connection.close().await?;
            anyhow::bail!("suppression synchronization already running for this source");
        }
    };
    let result = run_locked(source, store, restart).await;
    guard.release_now().await?.close().await?;
    result
}

async fn run_locked(source: &impl Source, store: &Store, restart: bool) -> anyhow::Result<Report> {
    let mut checkpoint = store.start_sync(source.scope(), restart).await?;
    let mut tokens = HashSet::new();
    if let Some(token) = &checkpoint.next_token {
        tokens.insert(token.clone());
    }
    for pages in 1..=100_000 {
        let page = read_page(source, checkpoint.next_token.as_deref()).await?;
        if let Some(next) = &page.next_token {
            ensure!(
                !next.is_empty() && next.len() <= 16_384,
                "invalid suppression pagination token"
            );
            ensure!(tokens.insert(next.clone()), "repeated AWS suppression pagination token");
        }
        checkpoint = store
            .import_page(
                source.scope(),
                checkpoint.next_token.as_deref(),
                page.records,
                page.next_token.as_deref(),
            )
            .await?;
        if checkpoint.completed {
            return Ok(Report { pages, merged: checkpoint.imported, completed: true });
        }
        tokio::time::sleep(source.page_interval()).await;
    }
    anyhow::bail!("suppression synchronization page limit reached; checkpoint retained")
}

async fn read_page(source: &impl Source, token: Option<&str>) -> Result<Page, ReadError> {
    let policy =
        Policy { max_attempts: 3, base_delay_ms: 1000, multiplier: 2.0, max_delay_ms: 3000 };
    for attempt in 1..=policy.max_attempts {
        match source.page(token).await {
            Err(ReadError::Unavailable) if attempt < policy.max_attempts => {
                tokio::time::sleep(Duration::from_millis(policy.calculate_delay(attempt, 0.25)))
                    .await;
            }
            result => return result,
        }
    }
    Err(ReadError::Unavailable)
}
