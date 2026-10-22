use super::attempt::AttemptLifecycleEvent as A;
use super::dispatch::abandon_and_purge;
use super::journal;
use super::lifecycle::MessageLifecycleEvent as M;
use super::store::event;
use crate::state::Store;

impl Store {
    pub(super) async fn maintain_messages(&self, retention_seconds: u64) -> anyhow::Result<()> {
        let mut tx = self.outbound_write().await?;
        let attempts: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM hailer.attempts WHERE phase IN ('reserved', 'dispatching') \
             AND lease_until <= clock_timestamp() ORDER BY lease_until LIMIT 64",
        )
        .fetch_all(&mut *tx)
        .await?;
        for id in attempts {
            let attempt = journal::attempt(&mut tx, &id).await?;
            let message = journal::load(&mut tx, &attempt.message_id).await?;
            let current = message.current_attempt_id.as_deref() == Some(&id);
            let (command, kind) = if attempt.phase == "reserved" {
                (A::Abandon, "lease_reclaimed")
            } else {
                (A::Uncertain, "outcome_unknown")
            };
            journal::attempt_transition(&mut tx, &attempt, command, kind).await?;
            if current {
                if message.status == "claimed" {
                    journal::transition(&mut tx, &message, M::Reclaim).await?;
                    sqlx::query(
                        "UPDATE hailer.messages SET current_attempt_id = NULL WHERE id = $1",
                    )
                    .bind(&message.id)
                    .execute(&mut *tx)
                    .await?;
                } else if message.status == "dispatching" {
                    journal::transition(&mut tx, &message, M::Uncertain).await?;
                }
                journal::clear_lease(&mut tx, &message.id).await?;
            }
            event(&mut tx, &message.id, kind, serde_json::json!({"attempt_id": id})).await?;
        }
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM hailer.messages WHERE \
             (status IN ('queued','retry_waiting','claimed') AND expires_at <= clock_timestamp()) \
             OR (provider_message_id IS NOT NULL AND status IN ('queued','retry_waiting','claimed','held_unknown')) \
             OR (status = 'held_unknown' AND payload IS NOT NULL AND \
                 accepted_at + $1 * interval '1 second' <= clock_timestamp()) \
             ORDER BY accepted_at LIMIT 64",
        ).bind(i64::try_from(retention_seconds)?).fetch_all(&mut *tx).await?;
        for id in ids {
            let message = journal::load(&mut tx, &id).await?;
            if message.provider_message_id.is_some() {
                abandon_and_purge(&mut tx, &message, "already_accepted", M::Accept).await?;
            } else if message.status == "held_unknown" {
                // Content retention is independent of delivery certainty.
                journal::purge(&mut tx, &id).await?;
            } else {
                abandon_and_purge(&mut tx, &message, "expired", M::Expire).await?;
                event(&mut tx, &id, "expired", serde_json::json!({})).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
