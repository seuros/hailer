//! Hailer owns its PostgreSQL schema. Feedback and its deduplication key commit
//! with suppression changes before acknowledging SQS.

mod bulk;
pub mod sync;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{ConnectOptions, PgPool, Postgres, Row, Transaction};

use crate::feedback::{FeedbackEvent, SuppressionReason};

#[derive(Clone)]
pub struct Store {
    pub(crate) pool: PgPool,
}

/// Also the JSON import format. Extra application export fields are ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suppression {
    pub email_address: String,
    pub reason: SuppressionReason,
    pub suppressed_at: String,
    pub originating_message_id: Option<String>,
    pub source_event_id: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredEvent {
    pub sequence: u64,
    pub event: FeedbackEvent,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventPage {
    pub events: Vec<StoredEvent>,
    pub next_cursor: u64,
}

impl Store {
    pub async fn connect(url: &str, max_connections: u32) -> anyhow::Result<Self> {
        ensure!((1..=64).contains(&max_connections), "database pool must be in 1..64");
        let options = PgConnectOptions::from_str(url)
            .map_err(|_| anyhow::anyhow!("invalid HAILER_DATABASE_URL"))?
            .application_name("hailer")
            .options([
                ("search_path", "hailer,public"),
                ("statement_timeout", "10000"),
                ("lock_timeout", "5000"),
            ])
            .disable_statement_logging();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(|_| anyhow::anyhow!("PostgreSQL connection unavailable"))?;
        let mut bootstrap = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(1751214437, 1)").execute(&mut *bootstrap).await?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS hailer").execute(&mut *bootstrap).await?;
        bootstrap.commit().await?;
        // With search_path set above, the migrator's tracking table belongs to
        // hailer too, not a Rails application's public schema.
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn healthy(&self) -> bool {
        matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool)
            )
            .await,
            Ok(Ok(1))
        )
    }

    /// Every state writer takes the same lock. Allocation remains locked until
    /// COMMIT: a later committed event can never hide an earlier pending one.
    pub(crate) async fn write(&self) -> anyhow::Result<Transaction<'_, Postgres>> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT value FROM hailer.event_counter WHERE id = 1 FOR UPDATE")
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }

    /// False means an already committed SNS event was redelivered.
    pub async fn record(&self, event: &FeedbackEvent) -> anyhow::Result<bool> {
        ensure!(!event.event_id.is_empty(), "missing event id");
        let records = event
            .suppression
            .map(|reason| {
                event
                    .recipients
                    .iter()
                    .map(|address| {
                        validate(Suppression {
                            email_address: address.clone(),
                            reason,
                            suppressed_at: event.occurred_at.clone(),
                            originating_message_id: event.message_id.clone(),
                            source_event_id: Some(event.event_id.clone()),
                            metadata: serde_json::Value::Null,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let mut transaction = self.write().await?;
        let duplicate: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hailer.events WHERE event_id = $1)")
                .bind(&event.event_id)
                .fetch_one(&mut *transaction)
                .await?;
        if duplicate {
            transaction.rollback().await?;
            return Ok(false);
        }
        let sequence: i64 = sqlx::query_scalar(
            "UPDATE hailer.event_counter SET value = value + 1 WHERE id = 1 RETURNING value",
        )
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query("INSERT INTO hailer.events(sequence, event_id, payload) VALUES ($1, $2, $3)")
            .bind(sequence)
            .bind(&event.event_id)
            .bind(Json(event))
            .execute(&mut *transaction)
            .await?;
        merge_all(&mut transaction, &records).await?;
        self.correlate_feedback(&mut transaction, event).await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Validate the complete export before opening its atomic import transaction.
    pub async fn import(&self, records: Vec<Suppression>) -> anyhow::Result<usize> {
        let records = records.into_iter().map(validate).collect::<anyhow::Result<Vec<_>>>()?;
        let mut transaction = self.write().await?;
        merge_all(&mut transaction, &records).await?;
        transaction.commit().await?;
        Ok(records.len())
    }

    pub async fn blocked(&self, addresses: &[String]) -> anyhow::Result<HashSet<String>> {
        let keys: Vec<_> = addresses.iter().filter_map(|address| normalize(address).ok()).collect();
        // One indexed, bounded-envelope query rather than loading the list.
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT email_address FROM hailer.suppressions WHERE email_address = ANY($1)",
        )
        .bind(keys)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    /// A cached verdict still fresh as of now, if one exists. Address
    /// verification costs money per lookup, so a cache hit skips the call
    /// entirely rather than re-checking an address a retry already checked.
    pub async fn cached_verdict(
        &self,
        address: &str,
    ) -> anyhow::Result<Option<crate::verify::Verdict>> {
        let Ok(key) = normalize(address) else { return Ok(None) };
        let row = sqlx::query(
            "SELECT state, reason, disposable, role FROM hailer.address_verifications \
             WHERE email_address = $1 AND expires_at > clock_timestamp()",
        )
        .bind(&key)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let state: String = row.try_get("state")?;
        Ok(Some(crate::verify::Verdict {
            state: crate::verify::State::parse(&state),
            reason: row.try_get("reason")?,
            disposable: row.try_get("disposable")?,
            role: row.try_get("role")?,
        }))
    }

    /// Upsert: a later check always wins, even if an older one has not yet
    /// expired (an operator may want a fresher verdict sooner).
    pub async fn cache_verdict(
        &self,
        address: &str,
        verdict: &crate::verify::Verdict,
        ttl: Duration,
    ) -> anyhow::Result<()> {
        let key = normalize(address)?;
        let ttl_seconds = i64::try_from(ttl.as_secs()).context("verification TTL overflow")?;
        sqlx::query(
            "INSERT INTO hailer.address_verifications \
                (email_address, state, reason, disposable, role, checked_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, clock_timestamp(), clock_timestamp() + $6 * INTERVAL '1 second') \
             ON CONFLICT (email_address) DO UPDATE SET \
                state = EXCLUDED.state, reason = EXCLUDED.reason, disposable = EXCLUDED.disposable, \
                role = EXCLUDED.role, checked_at = EXCLUDED.checked_at, expires_at = EXCLUDED.expires_at",
        )
        .bind(&key)
        .bind(verdict.state.as_str())
        .bind(&verdict.reason)
        .bind(verdict.disposable)
        .bind(verdict.role)
        .bind(ttl_seconds)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The whole blocklist. Small by design (curated + operator-added), so
    /// fetching it whole and matching in Rust (`domain_block::matches`) is
    /// simpler and more testable than encoding glob matching as SQL.
    pub async fn domain_block_patterns(&self) -> anyhow::Result<Vec<(String, Option<String>)>> {
        Ok(sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT pattern, reason FROM hailer.domain_blocks",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn ban_domain(&self, pattern: &str, reason: Option<&str>) -> anyhow::Result<()> {
        ensure!(!pattern.trim().is_empty(), "pattern must not be empty");
        sqlx::query(
            "INSERT INTO hailer.domain_blocks (pattern, source, reason) VALUES ($1, 'operator', $2) \
             ON CONFLICT (pattern) DO UPDATE SET reason = EXCLUDED.reason",
        )
        .bind(pattern.trim())
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Only an `operator`-sourced pattern can be removed this way - the
    /// curated list ships with the binary and only changes by upgrading it.
    pub async fn unban_domain(&self, pattern: &str) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "DELETE FROM hailer.domain_blocks WHERE pattern = $1 AND source = 'operator'",
        )
        .bind(pattern.trim())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn events(&self, after: u64, limit: usize) -> anyhow::Result<EventPage> {
        ensure!((1..=100).contains(&limit), "event page limit must be 1..100");
        let after_sql = i64::try_from(after).context("cursor exceeds supported range")?;
        let rows = sqlx::query(
            "SELECT sequence, payload FROM hailer.events WHERE sequence > $1 ORDER BY sequence LIMIT $2",
        )
        .bind(after_sql)
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        let events = rows
            .into_iter()
            .map(|row| {
                let sequence: i64 = row.try_get("sequence")?;
                let Json(event): Json<FeedbackEvent> = row.try_get("payload")?;
                Ok(StoredEvent { sequence: u64::try_from(sequence)?, event })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let next_cursor = events.last().map_or(after, |event| event.sequence);
        Ok(EventPage { events, next_cursor })
    }
}

pub fn validate(mut record: Suppression) -> anyhow::Result<Suppression> {
    record.email_address = normalize(&record.email_address)?;
    record.suppressed_at = record
        .suppressed_at
        .parse::<jiff::Timestamp>()
        .context("invalid suppression timestamp")?
        .to_string();
    Ok(record)
}

pub async fn merge_all(
    transaction: &mut Transaction<'_, Postgres>,
    records: &[Suppression],
) -> anyhow::Result<()> {
    bulk::merge(transaction, records).await
}

/// Explicit `CommonShare` matching policy; never rewrite a delivery address.
pub fn normalize(address: &str) -> anyhow::Result<String> {
    let address = address.trim().to_ascii_lowercase();
    let parsed = address
        .parse::<email_address::EmailAddress>()
        .map_err(|_| anyhow::anyhow!("invalid suppression address"))?;
    let local = parsed.local_part();
    let local = if local.starts_with('"') {
        local
    } else {
        local.split_once('+').map_or(local, |(base, _)| base)
    };
    ensure!(!local.is_empty(), "empty address local part");
    Ok(format!("{local}@{}", parsed.domain()))
}
