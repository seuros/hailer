//! Explicit, stopped-service migration. redb is never a live fallback.

#[cfg(test)]
mod tests;

use std::path::Path;

use anyhow::{Context, ensure};
use redb::{
    ReadOnlyDatabase, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde::Serialize;
use sqlx::types::Json;
use sqlx::{Postgres, QueryBuilder, Transaction};

use super::{Store, StoredEvent, Suppression, merge_all, validate};
use crate::feedback::FeedbackEvent;

const EVENTS: TableDefinition<u64, &str> = TableDefinition::new("feedback_v1");
const SEEN: TableDefinition<&str, u64> = TableDefinition::new("feedback_seen_v1");
const SUPPRESSIONS: TableDefinition<&str, &str> = TableDefinition::new("suppressions_v1");
const BATCH: usize = 256;

#[derive(Debug, Serialize)]
pub struct Report {
    pub events: u64,
    pub suppressions: u64,
    pub last_sequence: u64,
}

impl Store {
    /// Source must be cleanly closed by 0.1. The target must contain no live state.
    /// Batches bound memory; all destination changes share one transaction.
    pub async fn migrate_redb(&self, path: &Path) -> anyhow::Result<Report> {
        ensure!(path.is_file(), "legacy source must be an existing regular file");
        let source = ReadOnlyDatabase::open(path).context("open cleanly stopped legacy source")?;
        let snapshot = source.begin_read()?;
        let events = snapshot.open_table(EVENTS)?;
        let seen = snapshot.open_table(SEEN)?;
        let suppressions = snapshot.open_table(SUPPRESSIONS)?;
        ensure!(events.len()? == seen.len()?, "legacy event/dedup counts differ");

        let mut transaction = self.write().await?;
        let empty: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS(SELECT 1 FROM hailer.events) \
             AND NOT EXISTS(SELECT 1 FROM hailer.suppressions) \
             AND NOT EXISTS(SELECT 1 FROM hailer.sync_checkpoints) \
             AND (SELECT value = 0 FROM hailer.event_counter WHERE id = 1)",
        )
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(empty, "legacy migration requires an empty PostgreSQL target");
        let mut batch = Vec::with_capacity(BATCH);
        let mut report = Report { events: 0, suppressions: 0, last_sequence: 0 };
        for entry in events.iter()? {
            let (sequence, payload) = entry?;
            let sequence = sequence.value();
            ensure!(sequence > report.last_sequence, "invalid legacy sequence");
            i64::try_from(sequence).context("legacy sequence exceeds PostgreSQL range")?;
            let event: FeedbackEvent = serde_json::from_str(payload.value())?;
            ensure!(!event.event_id.trim().is_empty(), "empty legacy event id");
            ensure!(
                seen.get(event.event_id.as_str())?.is_some_and(|entry| entry.value() == sequence),
                "legacy event/dedup mapping differs"
            );
            event
                .occurred_at
                .parse::<jiff::Timestamp>()
                .context("invalid legacy event timestamp")?;
            batch.push(StoredEvent { sequence, event });
            report.last_sequence = sequence;
            report.events += 1;
            if batch.len() == BATCH {
                insert_events(&mut transaction, &batch).await?;
                batch.clear();
            }
        }
        insert_events(&mut transaction, &batch).await?;
        let mut batch = Vec::with_capacity(BATCH);
        for entry in suppressions.iter()? {
            let (key, payload) = entry?;
            let record: Suppression = serde_json::from_str(payload.value())?;
            let record = validate(record)?;
            ensure!(key.value() == record.email_address, "legacy suppression key differs");
            batch.push(record);
            report.suppressions += 1;
            if batch.len() == BATCH {
                merge_all(&mut transaction, &batch).await?;
                batch.clear();
            }
        }
        merge_all(&mut transaction, &batch).await?;
        sqlx::query("UPDATE hailer.event_counter SET value = $1 WHERE id = 1")
            .bind(i64::try_from(report.last_sequence)?)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(report)
    }
}

async fn insert_events(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[StoredEvent],
) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let rows = events
        .iter()
        .map(|event| Ok((i64::try_from(event.sequence)?, &event.event)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut query = QueryBuilder::new("INSERT INTO hailer.events(sequence, event_id, payload) ");
    query.push_values(rows, |mut row, (sequence, event)| {
        row.push_bind(sequence).push_bind(&event.event_id).push_bind(Json(event));
    });
    query.build().execute(&mut **transaction).await?;
    Ok(())
}
