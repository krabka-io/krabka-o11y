use super::{BTreeMap, NonZeroUsize, TenantId};

/// A per-tenant map that holds at most `capacity` tenants.
///
/// Every read and every insert marks the tenant as the most recently used. An
/// insert past the capacity first removes the least recently used tenant. The
/// map therefore cannot grow with the number of tenant names a client sends.
#[derive(Debug)]
pub(crate) struct TenantLru<V> {
    pub(crate) capacity: NonZeroUsize,
    pub(crate) entries: BTreeMap<TenantId, (u64, V)>,
    pub(crate) recency: BTreeMap<u64, TenantId>,
    pub(crate) clock: u64,
}

impl<V> TenantLru<V> {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            clock: 0,
        }
    }

    /// The tenant's value, marked as the most recently used.
    pub(crate) fn get_mut(&mut self, tenant: &TenantId) -> Option<&mut V> {
        self.clock += 1;
        let tick = self.clock;
        let (used, value) = self.entries.get_mut(tenant)?;
        self.recency.remove(used);
        *used = tick;
        self.recency.insert(tick, tenant.clone());
        Some(value)
    }

    /// The tenant's value, inserted from `make` when absent, and marked as the
    /// most recently used.
    pub(crate) fn get_or_insert_with(
        &mut self,
        tenant: &TenantId,
        make: impl FnOnce() -> V,
    ) -> &mut V {
        if !self.entries.contains_key(tenant)
            && self.entries.len() >= self.capacity.get()
            && let Some((_, evicted)) = self.recency.pop_first()
        {
            self.entries.remove(&evicted);
        }
        self.clock += 1;
        let tick = self.clock;
        let (used, value) = self
            .entries
            .entry(tenant.clone())
            .or_insert_with(|| (tick, make()));
        self.recency.remove(used);
        *used = tick;
        self.recency.insert(tick, tenant.clone());
        value
    }
}
