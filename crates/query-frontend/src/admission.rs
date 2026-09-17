use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, OnceLock},
};

use tokio::sync::Notify;

/// Cross-request query work limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdmissionLimits {
    pub max_concurrent_requests: usize,
    pub max_concurrent_requests_per_tenant: usize,
    pub max_concurrent_subqueries: usize,
    pub max_concurrent_subqueries_per_tenant: usize,
    /// Reservation used when a signal planner cannot estimate a subquery.
    pub estimated_bytes_per_subquery: usize,
    pub max_estimated_bytes: usize,
    pub max_estimated_bytes_per_tenant: usize,
    pub max_queued_requests: usize,
    pub max_queued_requests_per_tenant: usize,
    pub retry_after_seconds: u64,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 256,
            max_concurrent_requests_per_tenant: 32,
            max_concurrent_subqueries: 2_048,
            max_concurrent_subqueries_per_tenant: 256,
            estimated_bytes_per_subquery: 8 * 1024 * 1024,
            max_estimated_bytes: 8 * 1024 * 1024 * 1024,
            max_estimated_bytes_per_tenant: 1024 * 1024 * 1024,
            max_queued_requests: 1_024,
            max_queued_requests_per_tenant: 128,
            retry_after_seconds: 1,
        }
    }
}

/// Sparse runtime override for [`AdmissionLimits`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdmissionLimitsOverride {
    pub max_concurrent_requests: Option<usize>,
    pub max_concurrent_requests_per_tenant: Option<usize>,
    pub max_concurrent_subqueries: Option<usize>,
    pub max_concurrent_subqueries_per_tenant: Option<usize>,
    pub estimated_bytes_per_subquery: Option<usize>,
    pub max_estimated_bytes: Option<usize>,
    pub max_estimated_bytes_per_tenant: Option<usize>,
    pub max_queued_requests: Option<usize>,
    pub max_queued_requests_per_tenant: Option<usize>,
    pub retry_after_seconds: Option<u64>,
}

impl AdmissionLimits {
    #[must_use]
    pub fn merge(self, partial: AdmissionLimitsOverride) -> Self {
        Self {
            max_concurrent_requests: partial
                .max_concurrent_requests
                .unwrap_or(self.max_concurrent_requests),
            max_concurrent_requests_per_tenant: partial
                .max_concurrent_requests_per_tenant
                .unwrap_or(self.max_concurrent_requests_per_tenant),
            max_concurrent_subqueries: partial
                .max_concurrent_subqueries
                .unwrap_or(self.max_concurrent_subqueries),
            max_concurrent_subqueries_per_tenant: partial
                .max_concurrent_subqueries_per_tenant
                .unwrap_or(self.max_concurrent_subqueries_per_tenant),
            estimated_bytes_per_subquery: partial
                .estimated_bytes_per_subquery
                .unwrap_or(self.estimated_bytes_per_subquery),
            max_estimated_bytes: partial
                .max_estimated_bytes
                .unwrap_or(self.max_estimated_bytes),
            max_estimated_bytes_per_tenant: partial
                .max_estimated_bytes_per_tenant
                .unwrap_or(self.max_estimated_bytes_per_tenant),
            max_queued_requests: partial
                .max_queued_requests
                .unwrap_or(self.max_queued_requests),
            max_queued_requests_per_tenant: partial
                .max_queued_requests_per_tenant
                .unwrap_or(self.max_queued_requests_per_tenant),
            retry_after_seconds: partial
                .retry_after_seconds
                .unwrap_or(self.retry_after_seconds),
        }
    }
}

/// A query rejected before backend work started.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionError {
    #[error("query admission queue is full; retry after {retry_after_seconds}s")]
    QueueFull { retry_after_seconds: u64 },
    #[error("query exceeds the admission work limit; retry after {retry_after_seconds}s")]
    RequestTooLarge { retry_after_seconds: u64 },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Usage {
    requests: usize,
    subqueries: usize,
    estimated_bytes: usize,
}

#[derive(Clone, Debug)]
struct Waiting {
    id: u64,
    tenant: String,
    subqueries: usize,
    estimated_bytes: usize,
    tenant_limits: AdmissionLimits,
}

struct State {
    limits: AdmissionLimits,
    next_id: u64,
    active: Usage,
    tenants: BTreeMap<String, Usage>,
    queue: VecDeque<Waiting>,
}

/// Fair, cancellable admission shared by all query frontend requests.
pub struct AdmissionController {
    state: Mutex<State>,
    changed: Notify,
}

impl AdmissionController {
    #[must_use]
    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<AdmissionController>> = OnceLock::new();
        Arc::clone(GLOBAL.get_or_init(|| Self::new(AdmissionLimits::default())))
    }

    #[must_use]
    pub fn new(limits: AdmissionLimits) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                limits,
                next_id: 0,
                active: Usage::default(),
                tenants: BTreeMap::new(),
                queue: VecDeque::new(),
            }),
            changed: Notify::new(),
        })
    }

    /// Replace limits without dropping active work or queued requests.
    pub fn reload(&self, limits: AdmissionLimits) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits = limits;
        self.changed.notify_waiters();
    }

    /// # Errors
    /// Returns [`AdmissionError`] when the work exceeds a limit or the queue is full.
    pub async fn acquire(
        self: &Arc<Self>,
        tenant: String,
        subqueries: usize,
        estimated_bytes: usize,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let tenant_limits = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits;
        self.acquire_with_limits(tenant, subqueries, estimated_bytes, tenant_limits)
            .await
    }

    /// Acquire work using the tenant's currently resolved runtime override.
    ///
    /// # Errors
    /// Returns [`AdmissionError`] when the work exceeds a limit or the queue is full.
    pub async fn acquire_with_limits(
        self: &Arc<Self>,
        tenant: String,
        subqueries: usize,
        estimated_bytes: usize,
        tenant_limits: AdmissionLimits,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let id = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let retry_after_seconds = tenant_limits.retry_after_seconds.max(1);
            if state
                .limits
                .max_concurrent_requests
                .min(tenant_limits.max_concurrent_requests)
                == 0
                || tenant_limits.max_concurrent_requests_per_tenant == 0
                || subqueries
                    > state
                        .limits
                        .max_concurrent_subqueries
                        .min(tenant_limits.max_concurrent_subqueries)
                || subqueries > tenant_limits.max_concurrent_subqueries_per_tenant
                || estimated_bytes
                    > state
                        .limits
                        .max_estimated_bytes
                        .min(tenant_limits.max_estimated_bytes)
                || estimated_bytes > tenant_limits.max_estimated_bytes_per_tenant
            {
                return Err(AdmissionError::RequestTooLarge {
                    retry_after_seconds,
                });
            }
            if state.queue.len()
                >= state
                    .limits
                    .max_queued_requests
                    .min(tenant_limits.max_queued_requests)
                || state
                    .queue
                    .iter()
                    .filter(|waiting| waiting.tenant == tenant)
                    .count()
                    >= tenant_limits.max_queued_requests_per_tenant
            {
                return Err(AdmissionError::QueueFull {
                    retry_after_seconds,
                });
            }
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.queue.push_back(Waiting {
                id,
                tenant,
                subqueries,
                estimated_bytes,
                tenant_limits,
            });
            id
        };
        let mut queued = QueuedRequest {
            controller: Arc::clone(self),
            id: Some(id),
        };
        loop {
            let notified = self.changed.notified();
            if let Some(work) = self.try_admit(id) {
                queued.id = None;
                return Ok(AdmissionPermit {
                    controller: Arc::clone(self),
                    work: Some(work),
                });
            }
            notified.await;
        }
    }

    fn try_admit(&self, id: u64) -> Option<Waiting> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let position = state.queue.iter().position(|waiting| waiting.id == id)?;
        let work = &state.queue[position];
        if !fits(&state, work)
            || state
                .queue
                .iter()
                .take(position)
                .any(|earlier| earlier.tenant == work.tenant || fits(&state, earlier))
        {
            return None;
        }
        let work = state.queue.remove(position)?;
        add_usage(&mut state.active, &work);
        add_usage(state.tenants.entry(work.tenant.clone()).or_default(), &work);
        Some(work)
    }

    fn remove_waiter(&self, id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.queue.retain(|waiting| waiting.id != id);
        drop(state);
        self.changed.notify_waiters();
    }

    fn release(&self, work: &Waiting) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subtract_usage(&mut state.active, work);
        if let Some(tenant) = state.tenants.get_mut(&work.tenant) {
            subtract_usage(tenant, work);
            if *tenant == Usage::default() {
                state.tenants.remove(&work.tenant);
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

fn fits(state: &State, work: &Waiting) -> bool {
    let tenant = state.tenants.get(&work.tenant).copied().unwrap_or_default();
    state.active.requests
        < state
            .limits
            .max_concurrent_requests
            .min(work.tenant_limits.max_concurrent_requests)
        && tenant.requests < work.tenant_limits.max_concurrent_requests_per_tenant
        && state.active.subqueries.saturating_add(work.subqueries)
            <= state
                .limits
                .max_concurrent_subqueries
                .min(work.tenant_limits.max_concurrent_subqueries)
        && tenant.subqueries.saturating_add(work.subqueries)
            <= work.tenant_limits.max_concurrent_subqueries_per_tenant
        && state
            .active
            .estimated_bytes
            .saturating_add(work.estimated_bytes)
            <= state
                .limits
                .max_estimated_bytes
                .min(work.tenant_limits.max_estimated_bytes)
        && tenant.estimated_bytes.saturating_add(work.estimated_bytes)
            <= work.tenant_limits.max_estimated_bytes_per_tenant
}

fn add_usage(usage: &mut Usage, work: &Waiting) {
    usage.requests += 1;
    usage.subqueries += work.subqueries;
    usage.estimated_bytes += work.estimated_bytes;
}

fn subtract_usage(usage: &mut Usage, work: &Waiting) {
    usage.requests -= 1;
    usage.subqueries -= work.subqueries;
    usage.estimated_bytes -= work.estimated_bytes;
}

struct QueuedRequest {
    controller: Arc<AdmissionController>,
    id: Option<u64>,
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.controller.remove_waiter(id);
        }
    }
}

pub struct AdmissionPermit {
    controller: Arc<AdmissionController>,
    work: Option<Waiting>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            self.controller.release(&work);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    fn limits() -> AdmissionLimits {
        AdmissionLimits {
            max_concurrent_requests: 2,
            max_concurrent_requests_per_tenant: 1,
            max_concurrent_subqueries: 4,
            max_concurrent_subqueries_per_tenant: 2,
            estimated_bytes_per_subquery: 10,
            max_estimated_bytes: 100,
            max_estimated_bytes_per_tenant: 50,
            max_queued_requests: 1,
            max_queued_requests_per_tenant: 1,
            retry_after_seconds: 3,
        }
    }

    #[tokio::test]
    async fn queue_is_bounded_and_releases_capacity() {
        let controller = AdmissionController::new(limits());
        let active = controller.acquire("a".into(), 1, 10).await.unwrap();
        let waiting_controller = Arc::clone(&controller);
        let waiting =
            tokio::spawn(async move { waiting_controller.acquire("a".into(), 1, 10).await });
        tokio::task::yield_now().await;

        assert!(matches!(
            controller.acquire("a".into(), 1, 10).await,
            Err(AdmissionError::QueueFull {
                retry_after_seconds: 3,
            })
        ));
        drop(active);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_busy_tenant_does_not_block_another_tenant() {
        let mut fair_limits = limits();
        fair_limits.max_queued_requests = 2;
        let controller = AdmissionController::new(fair_limits);
        let active = controller.acquire("a".into(), 1, 10).await.unwrap();
        let waiting_controller = Arc::clone(&controller);
        let waiting =
            tokio::spawn(async move { waiting_controller.acquire("a".into(), 1, 10).await });
        tokio::task::yield_now().await;

        let tenant_b = tokio::time::timeout(
            Duration::from_secs(1),
            controller.acquire("b".into(), 1, 10),
        )
        .await
        .unwrap()
        .unwrap();
        drop(tenant_b);
        drop(active);
        assert!(waiting.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn one_tenant_cannot_fill_the_global_queue() {
        let mut fair_limits = limits();
        fair_limits.max_queued_requests = 2;
        let controller = AdmissionController::new(fair_limits);
        let active = controller.acquire("a".into(), 1, 10).await.unwrap();
        let waiting_controller = Arc::clone(&controller);
        let waiting =
            tokio::spawn(async move { waiting_controller.acquire("a".into(), 1, 10).await });
        tokio::task::yield_now().await;

        assert!(matches!(
            controller.acquire("a".into(), 1, 10).await,
            Err(AdmissionError::QueueFull { .. })
        ));
        let tenant_b = controller.acquire("b".into(), 1, 10).await.unwrap();
        drop(tenant_b);
        drop(active);
        assert!(waiting.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn canceling_a_waiter_removes_it_from_the_queue() {
        let controller = AdmissionController::new(limits());
        let active = controller.acquire("a".into(), 1, 10).await.unwrap();
        let canceled_controller = Arc::clone(&controller);
        let canceled =
            tokio::spawn(async move { canceled_controller.acquire("a".into(), 1, 10).await });
        tokio::task::yield_now().await;
        canceled.abort();
        assert!(matches!(canceled.await, Err(error) if error.is_cancelled()));

        let replacement_controller = Arc::clone(&controller);
        let replacement =
            tokio::spawn(async move { replacement_controller.acquire("a".into(), 1, 10).await });
        tokio::task::yield_now().await;
        drop(active);
        assert!(replacement.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn oversized_work_is_rejected_before_queueing() {
        let controller = AdmissionController::new(limits());
        assert!(matches!(
            controller.acquire("a".into(), 3, 10).await,
            Err(AdmissionError::RequestTooLarge {
                retry_after_seconds: 3,
            })
        ));
    }

    #[tokio::test]
    async fn a_runtime_tenant_override_changes_only_that_tenants_budget() {
        let controller = AdmissionController::new(limits());
        let mut tenant_limits = limits();
        tenant_limits.max_concurrent_subqueries_per_tenant = 1;
        tenant_limits.retry_after_seconds = 9;

        assert!(matches!(
            controller
                .acquire_with_limits("small".into(), 2, 10, tenant_limits)
                .await,
            Err(AdmissionError::RequestTooLarge {
                retry_after_seconds: 9,
            })
        ));
        assert!(controller.acquire("default".into(), 2, 10).await.is_ok());
    }
}
