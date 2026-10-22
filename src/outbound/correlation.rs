use sqlx::types::Json;
use sqlx::{Postgres, Row, Transaction};

use super::dispatch::record_outcome;
use super::store::event;
use super::{Correlation, Envelope, valid_id};
use crate::feedback::FeedbackEvent;
use crate::provider::queued::DispatchOutcome;
use crate::state::Store;

impl Store {
    /// Called inside feedback's event/suppression transaction, before SQS ack.
    /// Lock order is always feedback counter -> outbound counter, never reverse.
    pub(crate) async fn correlate_feedback(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        feedback: &FeedbackEvent,
    ) -> anyhow::Result<()> {
        let correlation = if let Some(correlation) = &feedback.correlation {
            correlation.clone()
        } else {
            // A provider ID already captured from our own accepted SDK response
            // is also unique evidence. Never infer a mapping from recipients or
            // RFC Message-ID, and never invent one for an unknown attempt.
            let bound: Option<(String, String)> = sqlx::query_as(
                "SELECT message_id, id FROM hailer.attempts WHERE provider_message_id = $1 AND phase = 'accepted'",
            ).bind(&feedback.provider_message_id).fetch_optional(&mut **tx).await?;
            let Some((hailer_message_id, attempt_id)) = bound else { return Ok(()) };
            Correlation { hailer_message_id, attempt_id }
        };
        if !valid_id(&correlation.hailer_message_id) || !valid_id(&correlation.attempt_id) {
            return Ok(());
        }
        sqlx::query("SELECT id FROM hailer.outbound_counter WHERE id = 1 FOR UPDATE")
            .fetch_one(&mut **tx)
            .await?;
        let row = sqlx::query(
            "SELECT phase, envelope, provider_message_id FROM hailer.attempts WHERE id = $1 AND message_id = $2",
        ).bind(&correlation.attempt_id).bind(&correlation.hailer_message_id)
            .fetch_optional(&mut **tx).await?;
        let Some(row) = row else { return Ok(()) };
        let phase: String = row.try_get("phase")?;
        let provider_id: Option<String> = row.try_get("provider_message_id")?;
        let envelope: Option<Json<Envelope>> = row.try_get("envelope")?;
        let Some(Json(envelope)) = envelope else { return Ok(()) };
        if !["dispatching", "unknown", "accepted", "not_accepted"].contains(&phase.as_str())
            || provider_id.as_ref().is_some_and(|id| id != &feedback.provider_message_id)
            || !feedback
                .recipients
                .iter()
                .all(|recipient| envelope.recipients().any(|allowed| allowed == recipient))
        {
            return Ok(());
        }
        let collision: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM hailer.attempts WHERE provider_message_id = $1 AND id <> $2)",
        ).bind(&feedback.provider_message_id).bind(&correlation.attempt_id)
            .fetch_one(&mut **tx).await?;
        if collision {
            return Ok(());
        }
        record_outcome(
            tx,
            &correlation,
            &DispatchOutcome::Accepted(feedback.provider_message_id.clone()),
        )
        .await?;
        let kind = serde_json::to_value(feedback.kind)?;
        let kind = kind.as_str().ok_or_else(|| anyhow::anyhow!("invalid feedback kind"))?;
        for recipient in &feedback.recipients {
            sqlx::query(
                "INSERT INTO hailer.recipient_facts(event_id, attempt_id, recipient, kind, occurred_at, message_id) \
                 VALUES ($1, $2, $3, $4, $5::timestamptz, $6) ON CONFLICT DO NOTHING",
            ).bind(&feedback.event_id).bind(&correlation.attempt_id).bind(recipient)
                .bind(kind).bind(&feedback.occurred_at).bind(&correlation.hailer_message_id)
                .execute(&mut **tx).await?;
        }
        event(tx, &correlation.hailer_message_id, "recipient_observed", serde_json::json!({
            "attempt_id": correlation.attempt_id, "provider_message_id": feedback.provider_message_id,
            "kind": kind, "recipients": feedback.recipients,
        })).await
    }
}
