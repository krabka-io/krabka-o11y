use super::{
    Arc, AtomicU64, DashMap, DeltaAccumulator, Mutex, Ordering, TenantDeltaAccumulator, TenantId,
};

/// The OTLP delta accumulators, one per tenant.
///
/// A single process-wide accumulator would fold two tenants' deltas for the
/// same attribute set into one cumulative value, which is both a cross-tenant
/// leak and a wrong value for both. It would also serialise the OTLP decode of
/// every tenant behind one lock. One accumulator per tenant fixes all three,
/// and gives each tenant its own place to bound.
#[derive(Debug)]
pub struct TenantDeltaAccumulators {
    accumulators: DashMap<TenantId, TenantDeltaAccumulator>,
    /// Maximum number of distinct tenants that hold an accumulator.
    max_tenants: usize,
    /// Monotonic logical clock that stamps touches for LRU eviction.
    touch_clock: AtomicU64,
}

impl TenantDeltaAccumulators {
    /// Builds a holder for at most `max_tenants` tenants. A cap of `0` clamps
    /// to `1`.
    #[must_use]
    pub fn new(max_tenants: usize) -> Self {
        Self {
            accumulators: DashMap::new(),
            max_tenants: max_tenants.max(1),
            touch_clock: AtomicU64::new(0),
        }
    }

    /// Returns `tenant`'s accumulator, and creates one when the tenant is new.
    #[must_use]
    pub(crate) fn for_tenant(&self, tenant: &TenantId) -> Arc<Mutex<DeltaAccumulator>> {
        let stamp = self.touch_clock.fetch_add(1, Ordering::Relaxed);
        let entry =
            self.accumulators
                .entry(tenant.clone())
                .or_insert_with(|| TenantDeltaAccumulator {
                    accumulator: Arc::new(Mutex::new(DeltaAccumulator::default())),
                    last_touch: AtomicU64::new(stamp),
                });
        // Stamp this access for LRU eviction, then drop the dashmap entry guard
        // before scanning so the eviction sweep never contends with the shard
        // lock this tenant lives in.
        entry.last_touch.store(stamp, Ordering::Relaxed);
        let accumulator = Arc::clone(&entry.accumulator);
        drop(entry);
        self.evict_if_over_cap();
        accumulator
    }

    /// Evicts the least-recently-touched tenants until the map is within the
    /// cap.
    ///
    /// This runs only on the cold path, where a new tenant arrives while the
    /// map is already at capacity. `max_tenants` bounds the linear scan, and
    /// the scan never runs on the steady-state hot path.
    fn evict_if_over_cap(&self) {
        while self.accumulators.len() > self.max_tenants {
            let oldest = self
                .accumulators
                .iter()
                .min_by_key(|entry| entry.value().last_touch.load(Ordering::Relaxed))
                .map(|entry| entry.key().clone());
            match oldest {
                Some(key) => {
                    self.accumulators.remove(&key);
                }
                None => break,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn tenant_count(&self) -> usize {
        self.accumulators.len()
    }
}
