use anyhow::ensure;
use sqlx::types::Json;
use sqlx::{Postgres, Row, Transaction};

use super::attempt::{self, AttemptLifecycleEvent as A};
use super::journal::{self, LEASE_MS};
use super::lifecycle::MessageLifecycleEvent as M;
use super::store::{event, now_ms};
use super::{Correlation, Envelope};
use crate::control::{Control, Decision, Snapshot};
use crate::mail::SendRequest;
use crate::provider::queued::DispatchOutcome;
use crate::state::{Store, normalize};

pub(super) struct Claim {
    pub correlation: Correlation,
    pub version: i64,
}

pub(super) struct Ticket {
    pub correlation: Correlation,
    pub request: SendRequest,
}

pub(super) fn retry_delay(ordinal: i32) -> u64 {
    chrono_machines::Policy {
        max_attempts: u8::MAX,
        base_delay_ms: 1000,
        multiplier: 2.0,
        max_delay_ms: 600_000,
    }
    .calculate_delay(u8::try_from(ordinal).unwrap_or(u8::MAX), 0.25)
}

impl Store {
    pub(super) async fn claim_message(&self) -> anyhow::Result<Option<Claim>> {
        let mut tx = self.outbound_write().await?;
        let id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM hailer.messages WHERE status IN ('queued', 'retry_waiting') \
             AND provider_message_id IS NULL AND payload IS NOT NULL \
             AND expires_at > clock_timestamp() AND next_attempt_at <= clock_timestamp() \
             ORDER BY next_attempt_at, accepted_at FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(id) = id else {
            tx.commit().await?;
            return Ok(None);
        };
        let message = journal::load(&mut tx, &id).await?;
        let ordinal: i32 = sqlx::query_scalar(
            "SELECT coalesce(max(ordinal), 0) + 1 FROM hailer.attempts WHERE message_id = $1",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
        let attempt_id: String = sqlx::query_scalar(
            "INSERT INTO hailer.attempts(message_id, ordinal, phase, lifecycle, lease_until) \
             VALUES ($1, $2, 'reserved', $3, clock_timestamp() + $4 * interval '1 millisecond') RETURNING id",
        ).bind(&id).bind(ordinal).bind(Json(attempt::initial()?)).bind(LEASE_MS)
            .fetch_one(&mut *tx).await?;
        let version = journal::transition(&mut tx, &message, M::Claim).await?;
        sqlx::query(
            "UPDATE hailer.messages SET current_attempt_id = $1, \
             lease_until = (SELECT lease_until FROM hailer.attempts WHERE id = $1) WHERE id = $2",
        )
        .bind(&attempt_id)
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        event(&mut tx, &id, "claimed", serde_json::json!({"attempt_id": attempt_id})).await?;
        tx.commit().await?;
        Ok(Some(Claim { correlation: Correlation { hailer_message_id: id, attempt_id }, version }))
    }

    pub(super) async fn prepare_dispatch(
        &self,
        claim: &Claim,
        control: &Control,
    ) -> anyhow::Result<Option<Ticket>> {
        let mut tx = self.outbound_write().await?;
        let message = journal::load(&mut tx, &claim.correlation.hailer_message_id).await?;
        let now = now_ms(&mut tx).await?;
        let live: bool = sqlx::query_scalar(
            "SELECT coalesce(lease_until > clock_timestamp(), false) FROM hailer.messages WHERE id = $1",
        ).bind(&message.id).fetch_one(&mut *tx).await?;
        if message.status != "claimed"
            || message.row_version != claim.version
            || message.current_attempt_id.as_deref() != Some(&claim.correlation.attempt_id)
            || !live
        {
            return Ok(None);
        }
        if message.provider_message_id.is_some() {
            abandon_and_purge(&mut tx, &message, "already_accepted", M::Accept).await?;
            tx.commit().await?;
            return Ok(None);
        }
        if message.expires_ms <= now {
            abandon_and_purge(&mut tx, &message, "expired", M::Expire).await?;
            event(&mut tx, &message.id, "expired", serde_json::json!({})).await?;
            tx.commit().await?;
            return Ok(None);
        }
        let (request, excluded) = load_allowed(&mut tx, &message.id).await?;
        let allowed = request.recipients().count();
        let decision = control.decide(Snapshot {
            valid: true,
            storage_ready: true,
            service_ready: true,
            recipients_allowed: allowed > 0,
            rate_allowed: allowed > 0 && control.rate_admit(allowed).await,
        });
        let attempt = journal::attempt(&mut tx, &claim.correlation.attempt_id).await?;
        sqlx::query("UPDATE hailer.attempts SET envelope = $1, excluded = $2 WHERE id = $3")
            .bind(Json(Envelope::from_mail(&request)))
            .bind(Json(excluded))
            .bind(&attempt.id)
            .execute(&mut *tx)
            .await?;
        if decision == Decision::Suppressed {
            journal::attempt_transition(&mut tx, &attempt, A::Abandon, "suppressed").await?;
            journal::transition(&mut tx, &message, M::Suppress).await?;
            journal::clear_lease(&mut tx, &message.id).await?;
            journal::purge(&mut tx, &message.id).await?;
            event(
                &mut tx,
                &message.id,
                "suppressed",
                serde_json::json!({"attempt_id": attempt.id}),
            )
            .await?;
            tx.commit().await?;
            return Ok(None);
        }
        if decision != Decision::Allow {
            journal::attempt_transition(&mut tx, &attempt, A::Abandon, "local_admission_denied")
                .await?;
            schedule_retry(&mut tx, &message, attempt.ordinal, "local_admission_denied").await?;
            tx.commit().await?;
            return Ok(None);
        }
        journal::attempt_transition(&mut tx, &attempt, A::Dispatch, "dispatch_started").await?;
        journal::transition(&mut tx, &message, M::Dispatch).await?;
        sqlx::query(
            "UPDATE hailer.attempts SET dispatch_started_at = clock_timestamp() WHERE id = $1",
        )
        .bind(&attempt.id)
        .execute(&mut *tx)
        .await?;
        event(&mut tx, &message.id, "dispatching", serde_json::json!({"attempt_id": attempt.id}))
            .await?;
        tx.commit().await?;
        Ok(Some(Ticket { correlation: claim.correlation.clone(), request }))
    }

    pub(super) async fn dispatch_fence(&self, ticket: &Ticket) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM hailer.messages WHERE id = $1 AND current_attempt_id = $2 \
             AND status = 'dispatching' AND provider_message_id IS NULL \
             AND lease_until > clock_timestamp() AND expires_at > clock_timestamp())",
        )
        .bind(&ticket.correlation.hailer_message_id)
        .bind(&ticket.correlation.attempt_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub(super) async fn finish_dispatch(
        &self,
        correlation: &Correlation,
        outcome: &DispatchOutcome,
    ) -> anyhow::Result<()> {
        let mut tx = self.outbound_write().await?;
        record_outcome(&mut tx, correlation, outcome).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn load_allowed(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> anyhow::Result<(SendRequest, Vec<serde_json::Value>)> {
    let row = sqlx::query("SELECT envelope, payload FROM hailer.messages WHERE id = $1")
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    let Json(envelope): Json<Envelope> = row.try_get("envelope")?;
    let bytes: Vec<u8> = row.try_get("payload")?;
    let mut request = envelope.with_body(String::from_utf8(bytes)?);
    request.validate()?;
    let keys: Vec<String> = request.recipients().map(normalize).collect::<anyhow::Result<_>>()?;
    let blocked: Vec<String> = sqlx::query_scalar(
        "SELECT email_address FROM hailer.suppressions WHERE email_address = ANY($1)",
    )
    .bind(keys)
    .fetch_all(&mut **tx)
    .await?;
    let mut excluded = Vec::new();
    for group in [&mut request.to_addresses, &mut request.cc_addresses, &mut request.bcc_addresses]
    {
        group.retain(|address| {
            let deny = normalize(address).is_ok_and(|key| blocked.contains(&key));
            if deny {
                excluded.push(serde_json::json!({"recipient": address, "reason": "suppressed"}));
            }
            !deny
        });
    }
    Ok((request, excluded))
}

pub(super) async fn abandon_current(
    tx: &mut Transaction<'_, Postgres>,
    message: &journal::Message,
    code: &str,
) -> anyhow::Result<()> {
    if let Some(id) = &message.current_attempt_id {
        let attempt = journal::attempt(tx, id).await?;
        if attempt.phase == "reserved" {
            journal::attempt_transition(tx, &attempt, A::Abandon, code).await?;
        }
    }
    journal::clear_lease(tx, &message.id).await
}

/// Abandons whatever attempt is currently reserved, then applies the
/// terminal `command` and purges the payload. Shared by every "the message
/// is done, for this reason" path: already accepted by a stale attempt,
/// expired, and operator-cancelled.
pub(super) async fn abandon_and_purge(
    tx: &mut Transaction<'_, Postgres>,
    message: &journal::Message,
    code: &str,
    command: M,
) -> anyhow::Result<()> {
    abandon_current(tx, message, code).await?;
    journal::transition_and_purge(tx, message, command).await
}

pub(super) async fn schedule_retry(
    tx: &mut Transaction<'_, Postgres>,
    message: &journal::Message,
    ordinal: i32,
    code: &str,
) -> anyhow::Result<()> {
    if ordinal >= i32::from(u8::MAX) {
        journal::transition_and_purge(tx, message, M::Reject).await?;
        event(tx, &message.id, "rejected", serde_json::json!({"code": "retry_exhausted"})).await?;
    } else {
        journal::transition(tx, message, M::Retry).await?;
        sqlx::query(
            "UPDATE hailer.messages SET next_attempt_at = clock_timestamp() + $1 * interval '1 millisecond' WHERE id = $2",
        ).bind(i64::try_from(retry_delay(ordinal))?).bind(&message.id).execute(&mut **tx).await?;
        event(tx, &message.id, "retry_waiting", serde_json::json!({"code": code})).await?;
    }
    journal::clear_lease(tx, &message.id).await
}

pub(super) async fn record_outcome(
    tx: &mut Transaction<'_, Postgres>,
    correlation: &Correlation,
    outcome: &DispatchOutcome,
) -> anyhow::Result<()> {
    let attempt = journal::attempt(tx, &correlation.attempt_id).await?;
    ensure!(attempt.message_id == correlation.hailer_message_id, "attempt/message mismatch");
    if attempt.phase == "accepted" || attempt.phase == "abandoned" || attempt.phase == "reserved" {
        return Ok(());
    }
    if attempt.phase == "not_accepted" && !matches!(outcome, DispatchOutcome::Accepted(_))
        || attempt.phase == "unknown" && matches!(outcome, DispatchOutcome::Unknown)
    {
        return Ok(());
    }
    let message = journal::load(tx, &correlation.hailer_message_id).await?;
    let current = message.current_attempt_id.as_deref() == Some(&attempt.id);
    let (command, code) = match outcome {
        DispatchOutcome::Accepted(_) => (A::Accept, "provider_accepted"),
        DispatchOutcome::NotAccepted { code, .. } | DispatchOutcome::NotDispatched(code) => {
            (A::Refuse, *code)
        }
        DispatchOutcome::Unknown => (A::Uncertain, "outcome_unknown"),
    };
    journal::attempt_transition(tx, &attempt, command, code).await?;
    if let DispatchOutcome::Accepted(id) = outcome {
        ensure!(
            !id.trim().is_empty() && id.len() <= 256 && !id.contains('\0'),
            "invalid provider id"
        );
        sqlx::query("UPDATE hailer.attempts SET provider_message_id = $1 WHERE id = $2")
            .bind(id)
            .bind(&attempt.id)
            .execute(&mut **tx)
            .await?;
        // Irreversible acceptance latch also blocks future claims after a late
        // historical success; never overwrite a newer attempt's own journal.
        sqlx::query(
            "UPDATE hailer.messages SET provider_message_id = coalesce(provider_message_id, $1) WHERE id = $2",
        ).bind(id).bind(&message.id).execute(&mut **tx).await?;
        journal::purge(tx, &message.id).await?;
        if (current || message.status != "dispatching")
            && ["queued", "claimed", "retry_waiting", "dispatching", "held_unknown"]
                .contains(&message.status.as_str())
        {
            abandon_current(tx, &message, "superseded_by_acceptance").await?;
            journal::transition(tx, &message, M::Accept).await?;
        }
    } else if current && ["dispatching", "held_unknown"].contains(&message.status.as_str()) {
        if message.provider_message_id.is_some() {
            journal::transition(tx, &message, M::Accept).await?;
        } else {
            match outcome {
                DispatchOutcome::NotAccepted { retryable: false, .. } => {
                    journal::transition_and_purge(tx, &message, M::Reject).await?;
                }
                DispatchOutcome::NotDispatched(_)
                | DispatchOutcome::NotAccepted { retryable: true, .. } => {
                    schedule_retry(tx, &message, attempt.ordinal, code).await?;
                }
                DispatchOutcome::Unknown => {
                    journal::transition(tx, &message, M::Uncertain).await?;
                }
                DispatchOutcome::Accepted(_) => unreachable!(),
            }
        }
        journal::clear_lease(tx, &message.id).await?;
    }
    event(tx, &message.id, code, serde_json::json!({
        "attempt_id": attempt.id, "historical_attempt": !current,
        "provider_message_id": if let DispatchOutcome::Accepted(id) = outcome { Some(id) } else { None },
    })).await
}
