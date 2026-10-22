use anyhow::ensure;
use sqlx::types::Json;
use sqlx::{Postgres, Row, Transaction};

use super::lifecycle::Lifecycle;
use super::{
    IntakeError, LifecycleEvent, LifecyclePage, MessageStatus, Prepared, Receipt, SEND_WINDOW_MS,
    StoredLifecycleEvent, Submission,
};
use crate::config::QueuedConfig;
use crate::state::Store;

impl Store {
    pub async fn submit(
        &self,
        command: Submission,
        budget: &QueuedConfig,
    ) -> Result<Receipt, IntakeError> {
        let prepared = Prepared::new(command)?;
        self.submit_prepared(prepared, budget)
            .await
            .map_err(|error| error.downcast::<IntakeError>().unwrap_or(IntakeError::Unavailable))
    }

    async fn submit_prepared(
        &self,
        command: Prepared,
        budget: &QueuedConfig,
    ) -> anyhow::Result<Receipt> {
        let mut tx = self.outbound_write().await?;
        let previous = sqlx::query(
            "SELECT fingerprint, receipt FROM hailer.messages WHERE submission_key = $1",
        )
        .bind(&command.key)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = previous {
            let fingerprint: Vec<u8> = row.try_get("fingerprint")?;
            if fingerprint != command.fingerprint {
                return Err(IntakeError::Conflict.into());
            }
            let Json(receipt): Json<Receipt> = row.try_get("receipt")?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let now = now_ms(&mut tx).await?;
        let deadline = command.expiry.map_or(now + SEND_WINDOW_MS, jiff::Timestamp::as_millisecond);
        if deadline <= now || deadline > now + SEND_WINDOW_MS {
            return Err(IntakeError::Invalid.into());
        }
        let admitted = sqlx::query(
            "UPDATE hailer.outbound_counter SET live_bytes = live_bytes + $1, \
             live_messages = live_messages + 1 WHERE id = 1 \
             AND live_bytes <= $2 - $1 AND live_messages < $3",
        )
        .bind(command.bytes)
        .bind(budget.max_bytes)
        .bind(budget.max_messages)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if admitted != 1 {
            return Err(IntakeError::Capacity.into());
        }
        let id: String =
            sqlx::query_scalar("SELECT gen_random_uuid()::text").fetch_one(&mut *tx).await?;
        let receipt = Receipt {
            hailer_message_id: id.clone(),
            accepted_at: jiff::Timestamp::from_millisecond(now)?.to_string(),
            expires_at: jiff::Timestamp::from_millisecond(deadline)?.to_string(),
        };
        let envelope = serde_json::json!({
            "from_email_address": command.mail.from_email_address,
            "to_addresses": command.mail.to_addresses,
            "cc_addresses": command.mail.cc_addresses,
            "bcc_addresses": command.mail.bcc_addresses,
        });
        sqlx::query(
            "INSERT INTO hailer.messages \
             (id, submission_key, fingerprint, receipt, envelope, payload, payload_bytes, \
              status, lifecycle, accepted_at, expires_at, next_attempt_at, rfc_message_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', $8, \
              to_timestamp($9::double precision / 1000), \
              to_timestamp($10::double precision / 1000), \
              to_timestamp($9::double precision / 1000), $11)",
        )
        .bind(&id)
        .bind(command.key)
        .bind(command.fingerprint)
        .bind(Json(&receipt))
        .bind(Json(envelope))
        .bind(command.mail.raw_mime.as_bytes())
        .bind(command.bytes)
        .bind(Json(Lifecycle::queued().persist()?))
        .bind(now)
        .bind(deadline)
        .bind(super::original_message_id(&command.mail.raw_mime))
        .execute(&mut *tx)
        .await?;
        event(&mut tx, &id, "queued", serde_json::json!({})).await?;
        tx.commit().await?;
        Ok(receipt)
    }

    /// Acquiring a row-write lock also fails on a read-only connection.
    pub async fn intake_ready(&self, budget: &QueuedConfig) -> bool {
        let operation = async {
            let mut tx = self.outbound_write().await?;
            let ready: bool = sqlx::query_scalar(
                "SELECT live_bytes < $1 AND live_messages < $2 \
                 FROM hailer.outbound_counter WHERE id = 1",
            )
            .bind(budget.max_bytes)
            .bind(budget.max_messages)
            .fetch_one(&mut *tx)
            .await?;
            tx.rollback().await?;
            Ok::<_, anyhow::Error>(ready)
        };
        matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), operation).await,
            Ok(Ok(true))
        )
    }

    pub async fn message_status(&self, id: &str) -> anyhow::Result<Option<MessageStatus>> {
        let row = sqlx::query(
            "SELECT receipt, status, lifecycle, row_version, \
             payload IS NOT NULL AS body_present, provider_message_id, rfc_message_id, \
             coalesce((SELECT jsonb_agg(jsonb_build_object( \
               'attempt_id', a.id, 'ordinal', a.ordinal, 'phase', a.phase, \
               'provider_message_id', a.provider_message_id, 'outcome_code', a.outcome_code, \
               'envelope', a.envelope, 'excluded', a.excluded) ORDER BY a.ordinal) \
               FROM (SELECT * FROM hailer.attempts WHERE message_id = m.id \
                     ORDER BY ordinal DESC LIMIT 100) a), '[]'::jsonb) AS attempts, \
             coalesce((SELECT jsonb_agg(jsonb_build_object( \
               'event_id', f.event_id, 'attempt_id', f.attempt_id, 'recipient', f.recipient, \
               'kind', f.kind, 'occurred_at', f.occurred_at) ORDER BY f.occurred_at, f.event_id, f.recipient) \
               FROM (SELECT * FROM hailer.recipient_facts WHERE message_id = m.id \
                     ORDER BY occurred_at DESC, event_id DESC, recipient DESC LIMIT 100) f), '[]'::jsonb) AS recipient_outcomes, \
             EXISTS(SELECT 1 FROM hailer.attempts WHERE message_id = m.id OFFSET 100 LIMIT 1) AS attempts_truncated, \
             EXISTS(SELECT 1 FROM hailer.recipient_facts WHERE message_id = m.id OFFSET 100 LIMIT 1) AS recipient_outcomes_truncated \
             FROM hailer.messages m WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let status: String = row.try_get("status")?;
            let Json(saved): Json<serde_json::Value> = row.try_get("lifecycle")?;
            Lifecycle::restore(saved, &status)?;
            let Json(receipt): Json<Receipt> = row.try_get("receipt")?;
            let Json(attempts) = row.try_get("attempts")?;
            let Json(recipient_outcomes) = row.try_get("recipient_outcomes")?;
            Ok(MessageStatus {
                receipt,
                status,
                version: row.try_get("row_version")?,
                body_present: row.try_get("body_present")?,
                provider_message_id: row.try_get("provider_message_id")?,
                rfc_message_id: row.try_get("rfc_message_id")?,
                attempts,
                recipient_outcomes,
                attempts_truncated: row.try_get("attempts_truncated")?,
                recipient_outcomes_truncated: row.try_get("recipient_outcomes_truncated")?,
            })
        })
        .transpose()
    }

    pub async fn message_events(&self, after: u64, limit: usize) -> anyhow::Result<LifecyclePage> {
        ensure!((1..=100).contains(&limit), "invalid lifecycle page limit");
        let rows = sqlx::query(
            "SELECT sequence, message_id, kind, details, \
             (extract(epoch FROM occurred_at) * 1000)::bigint AS occurred_ms \
             FROM hailer.message_events WHERE sequence > $1 ORDER BY sequence LIMIT $2",
        )
        .bind(i64::try_from(after)?)
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        let events = rows
            .into_iter()
            .map(|row| {
                let Json(details) = row.try_get("details")?;
                Ok(StoredLifecycleEvent {
                    sequence: u64::try_from(row.try_get::<i64, _>("sequence")?)?,
                    event: LifecycleEvent {
                        hailer_message_id: row.try_get("message_id")?,
                        kind: row.try_get("kind")?,
                        occurred_at: jiff::Timestamp::from_millisecond(
                            row.try_get("occurred_ms")?,
                        )?
                        .to_string(),
                        details,
                    },
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let next_cursor = events.last().map_or(after, |entry| entry.sequence);
        Ok(LifecyclePage { events, next_cursor })
    }

    pub(super) async fn outbound_write(&self) -> anyhow::Result<Transaction<'_, Postgres>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM hailer.outbound_counter WHERE id = 1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        Ok(tx)
    }
}

pub(super) async fn now_ms(tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(&mut **tx)
        .await?)
}

pub(super) async fn event(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    kind: &str,
    details: serde_json::Value,
) -> anyhow::Result<()> {
    let sequence: i64 = sqlx::query_scalar(
        "UPDATE hailer.outbound_counter SET sequence = sequence + 1 WHERE id = 1 RETURNING sequence",
    )
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO hailer.message_events(sequence, message_id, kind, details) VALUES ($1, $2, $3, $4)",
    )
    .bind(sequence)
    .bind(id)
    .bind(kind)
    .bind(Json(details))
    .execute(&mut **tx)
    .await?;
    Ok(())
}
