//! Hailer owns its PostgreSQL schema. Feedback and its deduplication key commit
//! with suppression changes before acknowledging SQS.

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

pub(crate) fn validate(mut record: Suppression) -> anyhow::Result<Suppression> {
    record.email_address = normalize(&record.email_address)?;
    record.suppressed_at = record
        .suppressed_at
        .parse::<jiff::Timestamp>()
        .context("invalid suppression timestamp")?
        .to_string();
    Ok(record)
}

pub(crate) async fn merge_all(
    transaction: &mut Transaction<'_, Postgres>,
    records: &[Suppression],
) -> anyhow::Result<()> {
    for record in records {
        let reason = match record.reason {
            SuppressionReason::Bounce => "bounce",
            SuppressionReason::Complaint => "complaint",
        };
        sqlx::query(
            "INSERT INTO hailer.suppressions AS current \
                (email_address, reason, suppressed_at, originating_message_id, source_event_id, metadata) \
             VALUES ($1, $2, $3::text::timestamptz, $4, $5, $6) \
             ON CONFLICT (email_address) DO UPDATE SET \
                reason = EXCLUDED.reason, \
                suppressed_at = LEAST(current.suppressed_at, EXCLUDED.suppressed_at), \
                originating_message_id = EXCLUDED.originating_message_id, \
                source_event_id = EXCLUDED.source_event_id, \
                metadata = CASE WHEN EXCLUDED.metadata = 'null'::jsonb \
                    THEN current.metadata ELSE EXCLUDED.metadata END \
             WHERE current.reason <> 'complaint'",
        )
        .bind(&record.email_address)
        .bind(reason)
        .bind(&record.suppressed_at)
        .bind(&record.originating_message_id)
        .bind(&record.source_event_id)
        .bind(Json(&record.metadata))
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

/// Explicit CommonShare matching policy; never rewrite a delivery address.
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
