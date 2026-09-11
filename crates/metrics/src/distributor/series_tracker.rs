use super::{
    BTreeMap, Instant, Mutex, MutexGuard, SERIES_SWEEP_INTERVAL, SeriesTrackerState, TenantId,
    TenantSeries, Time, TimeExt,
};

/// Per-tenant series state for the ingest gates that need it.
///
/// One map serves both the active-series cap and the out-of-order window,
/// because both are keyed by the same `(tenant, fingerprint)` pair. One map
/// also means one eviction path, so the two gates cannot drift apart on which
/// series exist.
///
/// The map is bounded on both axes. A tenant keeps a series only while it
/// writes to it within `Limits::active_series_idle_timeout`, and the number of
/// tracked tenants is capped, with the least-recently-written tenant evicted
/// to make room.
///
/// **The state is per process.** With more than one distributor replica, each
/// replica counts only the series it received, so the effective active-series
/// cap is the configured value times the replica count, and each replica
/// enforces the out-of-order window against its own view of a series. Upstream
/// Mimir makes both global by routing a series to one ingester through the
/// hash ring. Krabka has no ring.
#[derive(Debug)]
pub(crate) struct SeriesTracker {
    state: Mutex<SeriesTrackerState>,
    max_tenants: usize,
}

impl SeriesTracker {
    /// Builds a tracker that holds at most `max_tenants` tenants. A cap of `0`
    /// clamps to `1`.
    pub(crate) fn new(max_tenants: usize) -> Self {
        Self {
            state: Mutex::new(SeriesTrackerState {
                tenants: BTreeMap::new(),
                next_sweep: Instant::now(),
            }),
            max_tenants: max_tenants.max(1),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, SeriesTrackerState> {
        self.state.lock().expect("series tracker poisoned")
    }

    /// Returns `tenant`'s state, stamped as written to at `now`.
    ///
    /// This is the tracker's only cold-path work. It runs the idle sweep when
    /// one is due, and it evicts the least-recently-written tenants while the
    /// map is over its cap. Both walk the tenant map, which the cap bounds, and
    /// neither runs per sample: the sweep is rate-limited to one pass per
    /// [`SERIES_SWEEP_INTERVAL`], and the eviction runs only when a new tenant
    /// arrives at a full map.
    pub(crate) fn enter<'a>(
        &self,
        state: &'a mut SeriesTrackerState,
        tenant: &TenantId,
        idle_timeout: Time,
        now: Instant,
    ) -> &'a mut TenantSeries {
        if now >= state.next_sweep {
            Self::sweep(&mut state.tenants, now);
            state.next_sweep = now + SERIES_SWEEP_INTERVAL.to_std();
        }

        let entry = state
            .tenants
            .entry(tenant.clone())
            .or_insert_with(|| TenantSeries::new(now, idle_timeout));
        entry.last_seen = now;
        entry.idle_timeout = idle_timeout;

        self.evict_if_over_cap(&mut state.tenants, tenant);
        state
            .tenants
            .get_mut(tenant)
            .expect("the tenant entered above is kept out of the eviction scan")
    }

    /// The tenants the tracker holds state for, in order.
    #[cfg(test)]
    pub(crate) fn tenants(&self) -> Vec<String> {
        self.lock()
            .tenants
            .keys()
            .map(|tenant| tenant.as_str().to_owned())
            .collect()
    }

    /// The series `tenant` counts as active.
    #[cfg(test)]
    pub(crate) fn active_series(&self, tenant: &str) -> usize {
        self.lock()
            .tenants
            .iter()
            .find(|(name, _)| name.as_str() == tenant)
            .map_or(0, |(_, tracked)| tracked.series.len())
    }

    /// Drops every tenant's idle series, and then every tenant left with none.
    fn sweep(tenants: &mut BTreeMap<TenantId, TenantSeries>, now: Instant) {
        for tenant in tenants.values_mut() {
            tenant.sweep(now);
        }
        tenants.retain(|_, tenant| !tenant.series.is_empty());
    }

    /// Evicts the least-recently-written tenants until the map is within the
    /// cap. The tenant of the request in hand is never the one evicted, so the
    /// caller always gets an entry back.
    ///
    /// This runs only on the cold path, where a new tenant arrives while the
    /// map is already at capacity. `max_tenants` bounds the linear scan, and
    /// the scan never runs on the steady-state hot path.
    fn evict_if_over_cap(&self, tenants: &mut BTreeMap<TenantId, TenantSeries>, keep: &TenantId) {
        while tenants.len() > self.max_tenants {
            let oldest = tenants
                .iter()
                .filter(|(name, _)| *name != keep)
                .min_by_key(|(_, tenant)| tenant.last_seen)
                .map(|(name, _)| name.clone());
            match oldest {
                Some(name) => {
                    tenants.remove(&name);
                }
                None => break,
            }
        }
    }
}
