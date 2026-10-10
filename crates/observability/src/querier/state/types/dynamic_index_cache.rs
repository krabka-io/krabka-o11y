use super::{
    Arc, BTreeMap, BlockIndex, CachedDynamicIndex, CachedShardRanges, DynamicIndexCacheKey,
    DynamicShardIndexCacheKey, DynamicShardRangesCacheKey, Instant, LabelIndex, Mutex,
    NonZeroUsize, StdDurationExt, Time, TimeRange, minutes, secs,
};
use crate::SharedCompactionFrontier;

#[derive(Clone, Default)]
struct IndexCacheMaps {
    entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
}

pub(crate) struct FrontierIndexCache {
    owner: SharedCompactionFrontier,
    version: u64,
    maps: IndexCacheMaps,
}

#[derive(Clone)]
pub(crate) struct DynamicIndexCache {
    pub(crate) cache_ttl: Time,
    pub(crate) shard_cache_ttl: Time,
    pub(crate) shard_fetch_concurrency: NonZeroUsize,
    pub(crate) entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    pub(crate) shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    pub(crate) shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
    pub(crate) frontier_generation: Arc<Mutex<Option<FrontierIndexCache>>>,
}

impl DynamicIndexCache {
    pub(crate) fn for_frontier(&self, owner: &SharedCompactionFrontier, version: u64) -> Self {
        let mut current = self
            .frontier_generation
            .lock()
            .expect("index generation lock poisoned");
        let same_owner = current
            .as_ref()
            .is_some_and(|cache| Arc::ptr_eq(&cache.owner.frontier, &owner.frontier));
        let maps = if same_owner
            && current
                .as_ref()
                .is_some_and(|cache| cache.version == version)
        {
            current.as_ref().expect("generation exists").maps.clone()
        } else {
            let maps = IndexCacheMaps::default();
            // An old request can finish after a newer generation starts. Its loads
            // stay detached and cannot refill or replace the current cache.
            if !same_owner || current.as_ref().is_none_or(|cache| cache.version < version) {
                let retired = current.replace(FrontierIndexCache {
                    owner: owner.clone(),
                    version,
                    maps: maps.clone(),
                });
                drop(current);
                drop(retired);
            }
            maps
        };
        Self {
            entries: maps.entries,
            shard_ranges: maps.shard_ranges,
            shard_indexes: maps.shard_indexes,
            ..self.clone()
        }
    }

    pub(crate) fn clear(&self) {
        let retired = self
            .frontier_generation
            .lock()
            .expect("index generation lock poisoned")
            .take();
        drop(retired);
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

    pub(crate) fn get(
        &self,
        key: &DynamicIndexCacheKey,
    ) -> Option<(Arc<LabelIndex>, Arc<BlockIndex>)> {
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
        Some((
            Arc::clone(&entry.label_index),
            Arc::clone(&entry.block_index),
        ))
    }

    pub(crate) fn insert(
        &self,
        key: DynamicIndexCacheKey,
        label_index: impl Into<Arc<LabelIndex>>,
        block_index: impl Into<Arc<BlockIndex>>,
    ) {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic index cache lock poisoned");
        // Moving windows and retired shards may never request the same key
        // again. Reclaim their expired snapshots when admitting a new one.
        entries.retain(|_, entry| entry.loaded_at.elapsed().as_time() <= self.cache_ttl);
        entries.insert(
            key,
            CachedDynamicIndex {
                loaded_at: Instant::now(),
                label_index: label_index.into(),
                block_index: block_index.into(),
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
        let mut entries = self
            .shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned");
        // Moving windows and retired shards may never request the same key
        // again. Reclaim their expired snapshots when admitting a new one.
        entries.retain(|_, entry| entry.loaded_at.elapsed().as_time() <= self.cache_ttl);
        entries.insert(
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
    ) -> Option<(Arc<LabelIndex>, Arc<BlockIndex>)> {
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
        Some((
            Arc::clone(&entry.label_index),
            Arc::clone(&entry.block_index),
        ))
    }

    pub(crate) fn insert_shard_index(
        &self,
        key: DynamicShardIndexCacheKey,
        label_index: impl Into<Arc<LabelIndex>>,
        block_index: impl Into<Arc<BlockIndex>>,
    ) {
        let mut entries = self
            .shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned");
        // Moving windows and retired shards may never request the same key
        // again. Reclaim their expired snapshots when admitting a new one.
        entries.retain(|_, entry| entry.loaded_at.elapsed().as_time() <= self.shard_cache_ttl);
        entries.insert(
            key,
            CachedDynamicIndex {
                loaded_at: Instant::now(),
                label_index: label_index.into(),
                block_index: block_index.into(),
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
            frontier_generation: Arc::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::TimeExt as _;

    use super::*;

    #[test]
    fn captured_indexes_survive_cache_replacement_and_clear() {
        use krabka_blockstore::{BlockDescriptor, BlockKey, labels};

        let cache = DynamicIndexCache::default();
        let key = DynamicIndexCacheKey::TenantManifest {
            tenant: "tenant".to_string(),
        };
        let mut first_labels = LabelIndex::default();
        let api = first_labels.insert_series("tenant", labels([("app", "api")]));
        let mut first_blocks = BlockIndex::default();
        first_blocks.insert(BlockDescriptor::new(
            BlockKey::new("tenant", 0, 1, 2, TimeRange::new(10, 20).unwrap()),
            [api].into(),
        ));
        cache.insert(key.clone(), first_labels.clone(), first_blocks.clone());
        let captured = cache.get(&key).unwrap();

        let mut second_labels = LabelIndex::default();
        let worker = second_labels.insert_series("tenant", labels([("app", "worker")]));
        let mut second_blocks = BlockIndex::default();
        second_blocks.insert(BlockDescriptor::new(
            BlockKey::new("tenant", 0, 3, 4, TimeRange::new(30, 40).unwrap()),
            [worker].into(),
        ));
        cache.insert(key.clone(), second_labels.clone(), second_blocks.clone());
        let replacement = cache.get(&key).unwrap();
        cache.clear();

        assert2::assert!(cache.get(&key).is_none());
        assert2::assert!(
            (captured.0.as_ref(), captured.1.as_ref()) == (&first_labels, &first_blocks)
        );
        assert2::assert!(
            (replacement.0.as_ref(), replacement.1.as_ref()) == (&second_labels, &second_blocks)
        );
    }

    #[test]
    fn admission_reclaims_unrequested_expired_keys_and_keeps_live_snapshots() {
        let cache = DynamicIndexCache::default();
        let labels = krabka_blockstore::labels([("app", "api")]);
        let mut index = LabelIndex::default();
        index.insert_series("tenant-a", labels);
        let range = TimeRange::new(10, 19).unwrap();
        let query_key = |id| DynamicIndexCacheKey::TenantShards {
            tenant: "tenant-a".into(),
            start_ns: id,
            end_ns: id + 10,
        };
        let shard_key = |id| DynamicShardIndexCacheKey {
            tenant: "tenant-a".into(),
            start_ns: id,
            end_ns: id + 10,
        };
        let range_key = |id| DynamicShardRangesCacheKey {
            tenant: format!("tenant-{id}"),
        };
        for id in 0..32 {
            cache.insert(query_key(id), index.clone(), BlockIndex::default());
            cache.insert_shard_index(shard_key(id), index.clone(), BlockIndex::default());
            cache.insert_shard_ranges(range_key(id), 0, vec![range]);
        }
        // Age entries directly: no wall-clock sleeps or requests to old keys.
        let expired = Instant::now().checked_sub(minutes(6).to_std()).unwrap();
        for (key, entry) in cache.entries.lock().unwrap().iter_mut() {
            if *key != query_key(31) {
                entry.loaded_at = expired;
            }
        }
        for (key, entry) in cache.shard_indexes.lock().unwrap().iter_mut() {
            if *key != shard_key(31) {
                entry.loaded_at = expired;
            }
        }
        for (key, entry) in cache.shard_ranges.lock().unwrap().iter_mut() {
            if *key != range_key(31) {
                entry.loaded_at = expired;
            }
        }
        let captured = cache.get(&query_key(31)).unwrap().0;
        cache.insert(query_key(32), index.clone(), BlockIndex::default());
        cache.insert_shard_index(shard_key(32), index, BlockIndex::default());
        cache.insert_shard_ranges(range_key(32), 0, vec![range]);
        assert2::assert!(cache.entries.lock().unwrap().len() == 2);
        assert2::assert!(cache.shard_indexes.lock().unwrap().len() == 2);
        assert2::assert!(cache.shard_ranges.lock().unwrap().len() == 2);
        assert2::assert!(
            cache
                .get(&query_key(31))
                .unwrap()
                .0
                .label_values("tenant-a", "app")
                == captured.label_values("tenant-a", "app")
        );
        assert2::assert!(
            captured.label_values("tenant-a", "app")
                == std::collections::BTreeSet::from(["api".to_string()])
        );
        assert2::assert!(cache.get_shard_index(&shard_key(31)).is_some());
        assert2::assert!(cache.get_shard_ranges(&range_key(31), 0) == Some(vec![range]));
        assert2::assert!(cache.get_shard_ranges(&range_key(31), -1).is_none());
    }
}
