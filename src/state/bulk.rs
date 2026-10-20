use std::collections::BTreeMap;

use sqlx::types::Json;
use sqlx::{Postgres, QueryBuilder, Transaction};

use super::Suppression;
use crate::feedback::SuppressionReason;

/// Coalesce aliases before a multi-row UPSERT: PostgreSQL cannot update the
/// same conflict key twice in one statement. Preserve sequential merge policy.
fn coalesce(records: &[Suppression]) -> anyhow::Result<Vec<Suppression>> {
    let mut unique: BTreeMap<&str, Suppression> = BTreeMap::new();
    for record in records {
        let mut incoming = record.clone();
        if let Some(existing) = unique.get(record.email_address.as_str()) {
            if existing.reason == SuppressionReason::Complaint {
                continue;
            }
            if existing.suppressed_at.parse::<jiff::Timestamp>()?
                < incoming.suppressed_at.parse::<jiff::Timestamp>()?
            {
                incoming.suppressed_at.clone_from(&existing.suppressed_at);
            }
            if incoming.metadata.is_null() {
                incoming.metadata = existing.metadata.clone();
            }
        }
        unique.insert(&record.email_address, incoming);
    }
    Ok(unique.into_values().collect())
}

pub(super) async fn merge(
    transaction: &mut Transaction<'_, Postgres>,
    records: &[Suppression],
) -> anyhow::Result<()> {
    let records = coalesce(records)?;
    for chunk in records.chunks(256) {
        let mut query = QueryBuilder::new(
            "INSERT INTO hailer.suppressions AS current \
             (email_address, reason, suppressed_at, originating_message_id, source_event_id, metadata) ",
        );
        query.push_values(chunk, |mut row, record| {
            row.push_bind(&record.email_address)
                .push_bind(match record.reason {
                    SuppressionReason::Bounce => "bounce",
                    SuppressionReason::Complaint => "complaint",
                })
                .push_bind(&record.suppressed_at)
                .push_unseparated("::text::timestamptz")
                .push_bind(&record.originating_message_id)
                .push_bind(&record.source_event_id)
                .push_bind(Json(&record.metadata));
        });
        query.push(
            " ON CONFLICT (email_address) DO UPDATE SET reason = EXCLUDED.reason, \
             suppressed_at = LEAST(current.suppressed_at, EXCLUDED.suppressed_at), \
             originating_message_id = EXCLUDED.originating_message_id, \
             source_event_id = EXCLUDED.source_event_id, \
             metadata = CASE WHEN EXCLUDED.metadata = 'null'::jsonb \
                THEN current.metadata ELSE EXCLUDED.metadata END \
             WHERE current.reason <> 'complaint'",
        );
        query.build().execute(&mut **transaction).await?;
    }
    Ok(())
}
