use super::*;

#[derive(Clone)]
pub(crate) struct DynamicIndexCache {
    pub(crate) cache_ttl: Time,
    pub(crate) shard_cache_ttl: Time,
    pub(crate) shard_fetch_concurrency: NonZeroUsize,
    pub(crate) entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    pub(crate) shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    pub(crate) shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
}

impl DynamicIndexCache {
    pub(crate) fn clear(&self) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .clear();
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .clear();
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .clear();
    }

    pub(crate) fn get(&self, key: &DynamicIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic index cache lock poisoned");
        let entry = entries.get(key)?;
        // `>` is a permanent mutation survivor against `>=` in all three of
        // these lookups: they differ only for an entry whose age equals its TTL
        // to the nanosecond, which a monotonic clock does not hand out.
        if entry.loaded_at.elapsed().as_time() > self.cache_ttl {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    pub(crate) fn insert(
        &self,
        key: DynamicIndexCacheKey,
        label_index: LabelIndex,
        block_index: BlockIndex,
    ) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }

    pub(crate) fn get_shard_ranges(
        &self,
        key: &DynamicShardRangesCacheKey,
        required_from_ns: i64,
    ) -> Option<Vec<TimeRange>> {
        let mut ranges = self
            .shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned");
        let entry = ranges.get(key)?;
        // The TTL comparison is a permanent survivor for the reason given at
        // `DynamicIndexCache::get`.
        if entry.loaded_at.elapsed().as_time() > self.cache_ttl
            || entry.listed_from_ns > required_from_ns
        {
            ranges.remove(key);
            return None;
        }
        Some(entry.ranges.clone())
    }

    pub(crate) fn insert_shard_ranges(
        &self,
        key: DynamicShardRangesCacheKey,
        listed_from_ns: i64,
        ranges: Vec<TimeRange>,
    ) {
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .insert(
                key,
                CachedShardRanges {
                    loaded_at: Instant::now(),
                    listed_from_ns,
                    ranges,
                },
            );
    }

    pub(crate) fn get_shard_index(
        &self,
        key: &DynamicShardIndexCacheKey,
    ) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned");
        let entry = entries.get(key)?;
        // The TTL comparison is a permanent survivor for the reason given at
        // `DynamicIndexCache::get`.
        if entry.loaded_at.elapsed().as_time() > self.shard_cache_ttl {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    pub(crate) fn insert_shard_index(
        &self,
        key: DynamicShardIndexCacheKey,
        label_index: LabelIndex,
        block_index: BlockIndex,
    ) {
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }
}

impl Default for DynamicIndexCache {
    fn default() -> Self {
        Self {
            cache_ttl: secs(5),
            shard_cache_ttl: minutes(5),
            shard_fetch_concurrency: NonZeroUsize::new(32)
                .expect("default shard fetch concurrency is nonzero"),
            entries: Arc::default(),
            shard_ranges: Arc::default(),
            shard_indexes: Arc::default(),
        }
    }
}
