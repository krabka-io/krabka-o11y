use super::{BTreeMap, Instant, TenantId, TenantSeries};

/// Everything the series tracker keeps under its one lock.
#[derive(Debug)]
pub(crate) struct SeriesTrackerState {
    pub(crate) tenants: BTreeMap<TenantId, TenantSeries>,
    /// Instant at which the next idle sweep may run.
    pub(crate) next_sweep: Instant,
}
