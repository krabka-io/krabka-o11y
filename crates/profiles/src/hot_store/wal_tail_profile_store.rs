use super::*;

#[derive(Clone)]
pub struct WalTailProfileStore {
    /// Copy-on-write snapshot of the queryable store. A query clones the inner
    /// `Arc`, a cheap refcount bump, instead of a deep clone of every sample. A
    /// write mutates through `Arc::make_mut`, which deep-copies only while a
    /// snapshot is still outstanding.
    pub(crate) inner: Arc<RwLock<Arc<InMemoryProfileStore>>>,
    /// Source records retained within the retention window. The store uses them
    /// to rebuild the queryable store after an eviction, because the inner store
    /// exposes no row-level prune API.
    pub(crate) retained: Arc<RwLock<RetainedState>>,
    pub(crate) retention: RetentionConfig,
}

impl Default for WalTailProfileStore {
    fn default() -> Self {
        Self::new()
    }
}

impl WalTailProfileStore {
    #[must_use]
    pub fn new() -> Self {
        Self::with_retention(RetentionConfig::default())
    }

    #[must_use]
    pub fn with_retention(retention: RetentionConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(InMemoryProfileStore::new()))),
            retained: Arc::new(RwLock::new(RetainedState::default())),
            retention,
        }
    }

    /// Appends one WAL record.
    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn append_record(&self, record: ProfileRecord) -> Result<(), ProfilesError> {
        self.append_records(std::iter::once(record))
    }

    /// Appends a whole WAL poll's worth of records.
    ///
    /// This is what the WAL tail calls, and the reason it exists is the
    /// copy-on-write clone below: `Arc::make_mut` copies the queryable store
    /// whenever a query holds a snapshot, so a record-at-a-time loop copied it
    /// once per record. A batch copies it at most once however long it is.
    ///
    /// The batch is published atomically on the success path: the write guard
    /// is held across the whole loop, so a query sees the poll's records all at
    /// once or not at all. A record that fails to intern fails before it pushes
    /// a sample, so an error leaves the head holding a clean prefix of the
    /// batch rather than a half-applied record, and the tail stops.
    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn append_records(
        &self,
        records: impl IntoIterator<Item = ProfileRecord>,
    ) -> Result<(), ProfilesError> {
        let mut fresh = Vec::new();
        {
            let mut guard = self
                .inner
                .write()
                .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
            let store = Arc::make_mut(&mut guard);
            for record in records {
                let max_ts_ms = record
                    .samples
                    .iter()
                    .map(|sample| profile_timestamp_ms(sample.timestamp_ns))
                    .max();
                apply_record(store, &record)?;
                // A record with no samples carries no timestamp and needs no
                // retention bookkeeping; it contributed nothing to the store
                // either.
                if let Some(max_ts_ms) = max_ts_ms {
                    fresh.push(Retained { max_ts_ms, record });
                }
            }
        }

        if fresh.is_empty() {
            return Ok(());
        }

        let mut retained = self
            .retained
            .write()
            .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
        retained.records.extend(fresh);
        Self::prune(&self.retention, &mut retained);
        if Self::should_rebuild(&retained) {
            self.rebuild(&retained.records)?;
            retained.evicted_since_rebuild = 0;
        }
        Ok(())
    }

    /// Drop retained records that fall outside the retention window. The method
    /// counts how many records it has evicted since the last rebuild.
    pub(crate) fn prune(retention: &RetentionConfig, state: &mut RetainedState) {
        // `max_ts_ms` is an epoch-millisecond instant; only the retention window
        // is an extent, so it converts here and the subtraction stays exact
        // integer arithmetic (and saturates rather than overflowing).
        let newest = state.records.back().map_or(i64::MIN, |item| item.max_ts_ms);
        let horizon = newest.saturating_sub(retention.max_age.millis_i64());
        while state
            .records
            .front()
            .is_some_and(|item| item.max_ts_ms < horizon)
        {
            state.records.pop_front();
            state.evicted_since_rebuild += 1;
        }
        while state.records.len() > retention.max_records {
            state.records.pop_front();
            state.evicted_since_rebuild += 1;
        }
    }

    /// Rebuild only once evictions reach `1 / REBUILD_AMORTIZE_FACTOR` of the
    /// live window, or once the window has fully drained. A steady-state append
    /// that evicts one record then does not cause an O(window) rebuild every
    /// time.
    pub(crate) fn should_rebuild(state: &RetainedState) -> bool {
        state.evicted_since_rebuild > 0
            && state
                .evicted_since_rebuild
                .saturating_mul(REBUILD_AMORTIZE_FACTOR)
                >= state.records.len()
    }

    /// Rebuild the queryable store from the surviving retained records. Re-interning
    /// is deterministic, so the rebuilt store is equivalent to the un-evicted tail.
    pub(crate) fn rebuild(&self, retained: &VecDeque<Retained>) -> Result<(), ProfilesError> {
        let mut fresh = InMemoryProfileStore::new();
        for item in retained {
            apply_record(&mut fresh, &item.record)?;
        }
        let mut guard = self
            .inner
            .write()
            .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
        *guard = Arc::new(fresh);
        Ok(())
    }

    /// Cheap copy-on-write snapshot: clones the inner `Arc`, not the samples.
    ///
    /// The returned store is immutable and unaffected by any later append, so a
    /// query reads a consistent view for as long as it needs one.
    pub(crate) fn snapshot(&self) -> Result<Arc<InMemoryProfileStore>, ProfileError> {
        self.inner
            .read()
            .map_err(|_| ProfileError::Store("hot profile store lock poisoned".to_string()))
            .map(|guard| Arc::clone(&guard))
    }
}

#[async_trait::async_trait]
impl ProfileStore for WalTailProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        self.snapshot()?
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .label_names(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .profile_types(tenant, start_ms, end_ms)
            .await
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        self.snapshot()?
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        self.snapshot()?.stats(tenant, start_ms, end_ms).await
    }
}
