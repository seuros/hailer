use anyhow::ensure;

use super::Correlation;
use super::attempt::AttemptLifecycleEvent as A;
use super::dispatch::{abandon_and_purge, record_outcome};
use super::journal;
use super::lifecycle::MessageLifecycleEvent as M;
use super::store::{event, now_ms};
use crate::provider::queued::DispatchOutcome;
use crate::state::Store;

pub enum Resolution {
    Accepted(String),
    NotAccepted,
    Retry { acknowledge_duplicate_risk: bool },
}

fn validate_reason(reason: &str) -> anyhow::Result<()> {
    ensure!(
        !reason.trim().is_empty() && reason.len() <= 1024 && !reason.contains('\0'),
        "a nonempty audit reason of at most 1024 bytes is required"
    );
    Ok(())
}

impl Store {
    pub async fn cancel_message(&self, id: &str, reason: &str) -> anyhow::Result<()> {
        validate_reason(reason)?;
        let mut tx = self.outbound_write().await?;
        let message = journal::load(&mut tx, id).await?;
        ensure!(
            ["queued", "retry_waiting", "claimed"].contains(&message.status.as_str())
                && message.provider_message_id.is_none(),
            "message can no longer be cancelled"
        );
        abandon_and_purge(&mut tx, &message, "operator_cancelled", M::Cancel).await?;
        event(&mut tx, id, "operator_cancelled", serde_json::json!({"reason": reason})).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn resolve_message(
        &self,
        id: &str,
        resolution: Resolution,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.resolve_unknown(id, None, resolution, reason).await
    }

    pub async fn resolve_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        resolution: Resolution,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.resolve_unknown(id, Some(attempt_id), resolution, reason).await
    }

    async fn resolve_unknown(
        &self,
        id: &str,
        selected: Option<&str>,
        resolution: Resolution,
        reason: &str,
    ) -> anyhow::Result<()> {
        validate_reason(reason)?;
        let mut tx = self.outbound_write().await?;
        let message = journal::load(&mut tx, id).await?;
        ensure!(
            selected.is_some() || message.status == "held_unknown",
            "message is not held for an unknown outcome"
        );
        let attempt_id = selected
            .or(message.current_attempt_id.as_deref())
            .ok_or_else(|| anyhow::anyhow!("unknown message lacks attempt"))?;
        let attempt = journal::attempt(&mut tx, attempt_id).await?;
        ensure!(
            attempt.message_id == id && attempt.phase == "unknown",
            "selected attempt is not an unknown attempt of this message"
        );
        let current_held = message.current_attempt_id.as_deref() == Some(attempt_id)
            && message.status == "held_unknown";
        let correlation =
            Correlation { hailer_message_id: id.to_owned(), attempt_id: attempt_id.to_owned() };
        let action = match resolution {
            Resolution::Accepted(provider_id) => {
                record_outcome(&mut tx, &correlation, &DispatchOutcome::Accepted(provider_id))
                    .await?;
                "accepted"
            }
            Resolution::NotAccepted => {
                journal::attempt_transition(&mut tx, &attempt, A::Refuse, "operator_not_accepted")
                    .await?;
                if current_held {
                    journal::transition_and_purge(&mut tx, &message, M::Reject).await?;
                }
                "not_accepted"
            }
            Resolution::Retry { acknowledge_duplicate_risk } => {
                ensure!(
                    acknowledge_duplicate_risk,
                    "explicit duplicate-risk acknowledgement required"
                );
                ensure!(
                    current_held
                        && message.body_present
                        && message.expires_ms > now_ms(&mut tx).await?
                        && message.provider_message_id.is_none(),
                    "retry requires an available, unexpired, unaccepted message"
                );
                // Do not erase the previous attempt's uncertainty.
                journal::transition(&mut tx, &message, M::AuthorizeRetry).await?;
                sqlx::query(
                    "UPDATE hailer.messages SET next_attempt_at = clock_timestamp() WHERE id = $1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
                "retry_duplicate_risk_acknowledged"
            }
        };
        event(
            &mut tx,
            id,
            "operator_resolved",
            serde_json::json!({
                "reason": reason, "action": action, "attempt_id": attempt_id,
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
