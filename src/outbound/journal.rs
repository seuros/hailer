//! Every caller holds `outbound_counter` until commit. Effects never run here.

use anyhow::ensure;
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};

use super::attempt::{self, AttemptLifecycleEvent};
use super::lifecycle::{Lifecycle, MessageLifecycleEvent};
use super::store::event;

pub(super) const LEASE_MS: i64 = 60_000;

#[derive(sqlx::FromRow)]
pub(super) struct Message {
    pub id: String,
    pub status: String,
    pub lifecycle: Json<serde_json::Value>,
    pub row_version: i64,
    pub current_attempt_id: Option<String>,
    pub body_present: bool,
    pub expires_ms: i64,
    pub provider_message_id: Option<String>,
}

pub(super) async fn load(tx: &mut Transaction<'_, Postgres>, id: &str) -> anyhow::Result<Message> {
    let message = sqlx::query_as::<_, Message>(
        "SELECT id, status, lifecycle, row_version, current_attempt_id, \
         payload IS NOT NULL AS body_present, provider_message_id, \
         (extract(epoch FROM expires_at) * 1000)::bigint AS expires_ms \
         FROM hailer.messages WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    Lifecycle::restore(message.lifecycle.0.clone(), &message.status)?;
    Ok(message)
}

pub(super) async fn transition(
    tx: &mut Transaction<'_, Postgres>,
    message: &Message,
    command: MessageLifecycleEvent,
) -> anyhow::Result<i64> {
    let mut lifecycle = Lifecycle::restore(message.lifecycle.0.clone(), &message.status)?;
    lifecycle.apply(command)?;
    let status = lifecycle.status();
    let changed = sqlx::query(
        "UPDATE hailer.messages SET status = $1, lifecycle = $2, row_version = row_version + 1 \
         WHERE id = $3 AND row_version = $4",
    )
    .bind(status)
    .bind(Json(lifecycle.persist()?))
    .bind(&message.id)
    .bind(message.row_version)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    ensure!(changed == 1, "message version changed");
    Ok(message.row_version + 1)
}

#[derive(sqlx::FromRow)]
pub(super) struct Attempt {
    pub id: String,
    pub message_id: String,
    pub phase: String,
    pub lifecycle: Json<serde_json::Value>,
    pub ordinal: i32,
}

pub(super) async fn attempt(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> anyhow::Result<Attempt> {
    Ok(sqlx::query_as::<_, Attempt>(
        "SELECT id, message_id, phase, lifecycle, ordinal FROM hailer.attempts WHERE id = $1 FOR UPDATE",
    ).bind(id).fetch_one(&mut **tx).await?)
}

pub(super) async fn attempt_transition(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &Attempt,
    command: AttemptLifecycleEvent,
    code: &str,
) -> anyhow::Result<()> {
    let (phase, saved) = attempt::advance(attempt.lifecycle.0.clone(), &attempt.phase, command)?;
    let changed = sqlx::query(
        "UPDATE hailer.attempts SET phase = $1, lifecycle = $2, outcome_code = $3, \
         finished_at = CASE WHEN $1 = 'dispatching' THEN NULL ELSE clock_timestamp() END \
         WHERE id = $4 AND phase = $5",
    )
    .bind(phase)
    .bind(Json(saved))
    .bind(code)
    .bind(&attempt.id)
    .bind(&attempt.phase)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    ensure!(changed == 1, "attempt phase changed");
    Ok(())
}

pub(super) async fn purge(tx: &mut Transaction<'_, Postgres>, id: &str) -> anyhow::Result<()> {
    let bytes: Option<i64> = sqlx::query_scalar(
        "UPDATE hailer.messages SET payload = NULL, body_purged_at = clock_timestamp() \
         WHERE id = $1 AND payload IS NOT NULL RETURNING payload_bytes",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(bytes) = bytes {
        sqlx::query(
            "UPDATE hailer.outbound_counter SET live_bytes = live_bytes - $1, \
             live_messages = live_messages - 1 WHERE id = 1",
        )
        .bind(bytes)
        .execute(&mut **tx)
        .await?;
        event(tx, id, "body_purged", serde_json::json!({})).await?;
    }
    Ok(())
}

pub(super) async fn clear_lease(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE hailer.messages SET lease_until = NULL WHERE id = $1")
        .bind(id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Applies a terminal lifecycle transition and purges the now-unreachable
/// payload in one step. Every path that retires a message (expiry, rejection,
/// cancellation, acceptance-from-a-stale-attempt) ends this way.
pub(super) async fn transition_and_purge(
    tx: &mut Transaction<'_, Postgres>,
    message: &Message,
    command: MessageLifecycleEvent,
) -> anyhow::Result<()> {
    transition(tx, message, command).await?;
    purge(tx, &message.id).await
}
