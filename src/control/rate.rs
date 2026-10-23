use sqlx::{PgPool, Row};
use throttle_machines::gate::Gate;
use throttle_machines::token_bucket::{TokenBucket, TokenBucketParams, TokenBucketState};

use crate::config::RateLimitConfig;

/// Backed by a single shared `hailer.send_rate_limiter` row, serialized with
/// `FOR UPDATE`, so every replica draws down the same budget instead of each
/// enforcing its own independent, process-local one.
pub(super) struct Limiter {
    pool: PgPool,
}

impl Limiter {
    pub(super) async fn new(pool: PgPool, config: &RateLimitConfig) -> anyhow::Result<Self> {
        sqlx::query(
            "INSERT INTO hailer.send_rate_limiter (id, tokens, last_refill, capacity, refill_rate) \
             VALUES (1, $1, extract(epoch from clock_timestamp())::double precision, $1, $2) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(f64::from(config.burst))
        .bind(config.recipients_per_second)
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    /// Reserves the entire envelope or consumes no tokens at all. State only
    /// commits on full admission, matching `TokenBucket::check`'s own
    /// behavior on denial (it reports a refill without being asked to persist it).
    pub(super) async fn admit(&self, recipients: usize) -> anyhow::Result<bool> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT tokens, last_refill, capacity, refill_rate, \
             extract(epoch from clock_timestamp())::double precision AS now \
             FROM hailer.send_rate_limiter WHERE id = 1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let params = TokenBucketParams {
            capacity: row.try_get("capacity")?,
            refill_rate: row.try_get("refill_rate")?,
        };
        let now: f64 = row.try_get("now")?;
        let mut state = TokenBucketState {
            tokens: row.try_get("tokens")?,
            last_refill: row.try_get("last_refill")?,
        };
        let mut allowed = true;
        for _ in 0..recipients {
            let decision = TokenBucket::check(state, now, params);
            if !decision.allowed {
                allowed = false;
                break;
            }
            state = decision.state;
        }
        if allowed {
            sqlx::query(
                "UPDATE hailer.send_rate_limiter SET tokens = $1, last_refill = $2 WHERE id = 1",
            )
            .bind(state.tokens)
            .bind(state.last_refill)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(allowed)
    }

    /// Called by the AWS-discovery reconcile loop. Capacity tracks the
    /// discovered rate 1:1 (the burst allowance stops being an independent
    /// operator knob once AWS owns the number), never mutating `tokens`
    /// directly: a rate drop takes effect at the next refill, not instantly.
    pub(super) async fn set_rate(&self, recipients_per_second: f64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE hailer.send_rate_limiter \
             SET refill_rate = $1, capacity = $1 WHERE id = 1",
        )
        .bind(recipients_per_second)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
