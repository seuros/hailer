//! A feedback event, its deduplication key, and its suppressions commit together.
//! Never delete an SQS message before this durable transaction succeeds.

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, ensure};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::feedback::{FeedbackEvent, SuppressionReason};

const EVENTS: TableDefinition<u64, &str> = TableDefinition::new("feedback_v1");
const SEEN: TableDefinition<&str, u64> = TableDefinition::new("feedback_seen_v1");
const SUPPRESSIONS: TableDefinition<&str, &str> = TableDefinition::new("suppressions_v1");

#[derive(Clone)]
pub struct Store(Arc<Database>);

/// Also the import format. Extra `ActiveRecord` export fields are ignored.
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
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let database = Database::create(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let transaction = database.begin_write()?;
        transaction.open_table(EVENTS)?;
        transaction.open_table(SEEN)?;
        transaction.open_table(SUPPRESSIONS)?;
        transaction.commit()?;
        Ok(Self(Arc::new(database)))
    }

    /// False means a previously committed event was redelivered.
    pub fn record(&self, event: &FeedbackEvent) -> anyhow::Result<bool> {
        let mut transaction = self.0.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut seen = transaction.open_table(SEEN)?;
            if seen.get(event.event_id.as_str())?.is_some() {
                return Ok(false);
            }
            let mut events = transaction.open_table(EVENTS)?;
            let sequence = events
                .last()?
                .map_or(0, |(key, _)| key.value())
                .checked_add(1)
                .context("event sequence exhausted")?;
            events.insert(sequence, serde_json::to_string(event)?.as_str())?;
            if let Some(reason) = event.suppression {
                let mut suppressions = transaction.open_table(SUPPRESSIONS)?;
                for address in &event.recipients {
                    let record = Suppression {
                        email_address: normalize(address)?,
                        reason,
                        suppressed_at: event.occurred_at.clone(),
                        originating_message_id: event.message_id.clone(),
                        source_event_id: Some(event.event_id.clone()),
                        metadata: serde_json::Value::Null,
                    };
                    merge(&mut suppressions, record)?;
                }
            }
            seen.insert(event.event_id.as_str(), sequence)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    /// Validate the entire batch first. A bad export must not partially import.
    pub fn import(&self, records: Vec<Suppression>) -> anyhow::Result<usize> {
        let records = records
            .into_iter()
            .map(|mut record| {
                record.email_address = normalize(&record.email_address)?;
                record.suppressed_at = record
                    .suppressed_at
                    .parse::<jiff::Timestamp>()
                    .context("invalid suppression timestamp")?
                    .to_string();
                Ok(record)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut transaction = self.0.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut table = transaction.open_table(SUPPRESSIONS)?;
            for record in &records {
                merge(&mut table, record.clone())?;
            }
        }
        transaction.commit()?;
        Ok(records.len())
    }

    pub fn blocked(&self, addresses: &[String]) -> anyhow::Result<HashSet<String>> {
        let transaction = self.0.begin_read()?;
        let table = transaction.open_table(SUPPRESSIONS)?;
        let mut blocked = HashSet::new();
        for address in addresses {
            // Invalid addresses are rejected by the provider, never repaired.
            if let Ok(key) = normalize(address)
                && table.get(key.as_str())?.is_some()
            {
                blocked.insert(key);
            }
        }
        Ok(blocked)
    }

    pub fn events(&self, after: u64, limit: usize) -> anyhow::Result<EventPage> {
        ensure!((1..=100).contains(&limit), "event page limit must be 1..100");
        let transaction = self.0.begin_read()?;
        let table = transaction.open_table(EVENTS)?;
        let events = table
            .range((Excluded(after), Unbounded))?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                Ok(StoredEvent {
                    sequence: key.value(),
                    event: serde_json::from_str(value.value())?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let next_cursor = events.last().map_or(after, |event| event.sequence);
        Ok(EventPage { events, next_cursor })
    }
}

fn merge(table: &mut redb::Table<&str, &str>, mut incoming: Suppression) -> anyhow::Result<()> {
    let existing = table
        .get(incoming.email_address.as_str())?
        .map(|value| serde_json::from_str::<Suppression>(value.value()))
        .transpose()?;
    if let Some(existing) = existing {
        // Neither an old bounce nor an import can downgrade a complaint.
        if existing.reason == SuppressionReason::Complaint {
            return Ok(());
        }
        if existing.suppressed_at.parse::<jiff::Timestamp>()?
            < incoming.suppressed_at.parse::<jiff::Timestamp>()?
        {
            incoming.suppressed_at = existing.suppressed_at;
        }
        if incoming.metadata.is_null() {
            incoming.metadata = existing.metadata;
        }
    }
    table.insert(incoming.email_address.as_str(), serde_json::to_string(&incoming)?.as_str())?;
    Ok(())
}

/// Preserve `CommonShare`'s existing suppression matching: case-insensitive and
/// plus aliases share a key. This never rewrites the actual delivery address.
pub fn normalize(address: &str) -> anyhow::Result<String> {
    let address = address.trim().to_ascii_lowercase();
    let parsed =
        address.parse::<email_address::EmailAddress>().context("invalid suppression address")?;
    let local = parsed.local_part();
    // Quoted local parts are literal mailboxes, not plus-address aliases.
    let local = if local.starts_with('"') {
        local
    } else {
        local.split_once('+').map_or(local, |(base, _)| base)
    };
    ensure!(!local.is_empty(), "empty address local part");
    Ok(format!("{local}@{}", parsed.domain()))
}
