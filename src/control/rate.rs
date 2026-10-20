use std::sync::Mutex;
use std::time::Instant;

use throttle_machines::gate::Gate;
use throttle_machines::token_bucket::{TokenBucket, TokenBucketParams, TokenBucketState};

use crate::config::RateLimitConfig;

pub(super) struct Limiter {
    state: Mutex<TokenBucketState>,
    params: TokenBucketParams,
    epoch: Instant,
}

impl Limiter {
    pub(super) fn new(config: &RateLimitConfig) -> Self {
        Self {
            state: Mutex::new(TokenBucketState {
                tokens: f64::from(config.burst),
                last_refill: 0.0,
            }),
            params: TokenBucketParams {
                capacity: f64::from(config.burst),
                refill_rate: config.recipients_per_second,
            },
            epoch: Instant::now(),
        }
    }

    pub(super) fn admit(&self, recipients: usize) -> bool {
        self.admit_at(recipients, self.epoch.elapsed().as_secs_f64())
    }

    pub(super) fn admit_at(&self, recipients: usize, now: f64) -> bool {
        let Ok(mut state) = self.state.lock() else { return false };
        let mut proposed = *state;
        // Reserve the entire envelope or consume no tokens at all.
        for _ in 0..recipients {
            let decision = TokenBucket::check(proposed, now, self.params);
            if !decision.allowed {
                return false;
            }
            proposed = decision.state;
        }
        *state = proposed;
        true
    }
}
