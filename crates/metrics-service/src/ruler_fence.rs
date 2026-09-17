use std::{sync::Mutex, time::Instant};

use krabka_promql::RulerWalError;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RulerFenceStatus {
    pub active: bool,
    pub owner: String,
    pub epoch: i32,
    pub renew_failures: u64,
    pub failover_duration_ms: u64,
}

struct State {
    active: bool,
    owner: String,
    epoch: i32,
    renewed_at: Instant,
    lost_at: Option<Instant>,
    renew_failures: u64,
    failover_duration_ms: u64,
}

/// Broker-generation fence shared by every ruler output sink.
pub struct RulerFence {
    timeout: std::time::Duration,
    state: Mutex<State>,
    metrics: Option<krabka_promql::metrics::ServiceMetrics>,
}

impl RulerFence {
    #[must_use]
    pub fn new(timeout: std::time::Duration) -> Self {
        let now = Instant::now();
        Self {
            timeout,
            state: Mutex::new(State {
                active: false,
                owner: String::new(),
                epoch: -1,
                renewed_at: now,
                lost_at: Some(now),
                renew_failures: 0,
                failover_duration_ms: 0,
            }),
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: krabka_promql::metrics::ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn renew(&self, owner: &str, epoch: i32) {
        self.renew_at(owner, epoch, Instant::now());
    }

    fn renew_at(&self, owner: &str, epoch: i32, now: Instant) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if epoch < state.epoch {
            state.renew_failures += 1;
            if let Some(metrics) = &self.metrics {
                metrics.ruler_fence_renew_failures.inc();
            }
            return;
        }
        if !state.active || state.owner != owner || state.epoch != epoch {
            state.failover_duration_ms = state.lost_at.map_or(0, |lost_at| {
                u64::try_from(now.saturating_duration_since(lost_at).as_millis())
                    .unwrap_or(u64::MAX)
            });
        }
        state.active = true;
        state.owner.clear();
        state.owner.push_str(owner);
        state.epoch = epoch;
        state.renewed_at = now;
        state.lost_at = None;
        if let Some(metrics) = &self.metrics {
            metrics.ruler_fence_active.set(1);
            metrics.ruler_fence_epoch.set(i64::from(epoch));
            metrics
                .ruler_fence_failover_duration_seconds
                .set(std::time::Duration::from_millis(state.failover_duration_ms).as_secs_f64());
        }
    }

    pub fn revoke(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active {
            state.active = false;
            state.lost_at = Some(Instant::now());
        }
        state.renew_failures += 1;
        if let Some(metrics) = &self.metrics {
            metrics.ruler_fence_active.set(0);
            metrics.ruler_fence_renew_failures.inc();
        }
    }

    /// # Errors
    /// Returns an append error while this replica lacks the current lease.
    pub fn check(&self) -> Result<(), RulerWalError> {
        self.check_at(Instant::now())
    }

    fn check_at(&self, now: Instant) -> Result<(), RulerWalError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active && now.saturating_duration_since(state.renewed_at) <= self.timeout {
            return Ok(());
        }
        Err(RulerWalError::Append(format!(
            "ruler output fenced: owner={:?} epoch={} active={} lease_age_ms={}",
            state.owner,
            state.epoch,
            state.active,
            now.saturating_duration_since(state.renewed_at).as_millis(),
        )))
    }

    #[must_use]
    pub fn status(&self) -> RulerFenceStatus {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RulerFenceStatus {
            active: state.active,
            owner: state.owner.clone(),
            epoch: state.epoch,
            renew_failures: state.renew_failures,
            failover_duration_ms: state.failover_duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn stale_epochs_and_expired_owners_are_fenced() {
        let fence = RulerFence::new(std::time::Duration::from_secs(5));
        let start = Instant::now();
        fence.renew_at("owner-a", 7, start);
        assert!(
            fence
                .check_at(start + std::time::Duration::from_secs(5))
                .is_ok()
        );
        assert!(
            fence
                .check_at(start + std::time::Duration::from_secs(6))
                .is_err()
        );

        fence.renew_at("stale", 6, start + std::time::Duration::from_secs(7));
        assert!(fence.status().owner == "owner-a");
        assert!(fence.status().renew_failures == 1);
        fence.renew_at("owner-b", 8, start + std::time::Duration::from_secs(8));
        assert!(fence.status().owner == "owner-b");
        assert!(fence.status().epoch == 8);
    }
}
