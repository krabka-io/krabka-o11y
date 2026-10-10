use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use krabka_units::ByteSize;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

use super::{
    AddressFallbackResolver, Arc, AsArray, BTreeMap, BTreeSet, ChainedResolver, CompositeSymbols,
    DebuginfodConfig, DebuginfodResolver, ExternalPartition, FileSystemResolver, HashMap,
    Int64Type, LabelMatcher, LazySymbolizer, LocalPartition, MemTable, Mutex, NativeResolver,
    ObjectStore, ObjectStoreExt, ParquetRecordBatchReaderBuilder, Path, ProfileError, ProfileIndex,
    ProfileQueryStats, ProfileScan, ProfileStats, ProfileStore, RecordBatch, RwLock,
    SeriesFingerprint, SymbolDb, UInt64Type, VecDeque, batch_fingerprints_overlap,
    block_partition_map, filter_and_remap_batch, is_unbounded_metadata_range,
    local_native_resolver, profile_samples_schema,
};

const SYMBOL_DB_CACHE_TTL: Duration = Duration::from_secs(30);
type SymbolDbCache = (HashMap<String, (Arc<SymbolDb>, Instant)>, VecDeque<String>);

#[derive(Clone)]
pub struct ColdProfileStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    // The block index is loaded from object storage and must be REFRESHED as the
    // block-builder writes new blocks — otherwise blocks created after the querier
    // started are invisible (a query only sees the startup snapshot). Held behind a
    // lock so a background task can swap in a freshly-loaded index; readers clone the
    // inner `Arc` out and never hold the guard across an await.
    pub(crate) index: Arc<RwLock<Arc<ProfileIndex>>>,
    pub(crate) resolver: Arc<ChainedResolver>,
    pub(crate) symdb_cache: Arc<Mutex<SymbolDbCache>>,
    index_snapshot: Option<(String, ByteSize)>,
}

fn cached_symbol_db(cache: &mut SymbolDbCache, block_key: &str, now: Instant) -> Option<SymbolDb> {
    if let Some((symbols, cached_at)) = cache.0.get(block_key)
        && now.saturating_duration_since(*cached_at) < SYMBOL_DB_CACHE_TTL
    {
        return Some((**symbols).clone());
    }
    cache.0.remove(block_key);
    cache.1.retain(|key| key != block_key);
    None
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod symdb_cache_tests {
    use super::*;

    #[test]
    fn expired_symbol_database_is_reloaded() {
        let now = Instant::now();
        let mut cache = SymbolDbCache::default();
        cache.0.insert(
            "block".into(),
            (
                Arc::new(SymbolDb::new()),
                now.checked_sub(SYMBOL_DB_CACHE_TTL).unwrap(),
            ),
        );
        cache.1.push_back("block".into());

        assert!(cached_symbol_db(&mut cache, "block", now).is_none());
        assert!(cache.0.is_empty());
        assert!(cache.1.is_empty());
    }
}

impl ColdProfileStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, index: Arc<ProfileIndex>) -> Self {
        Self {
            store,
            index: Arc::new(RwLock::new(index)),
            resolver: local_native_resolver(),
            symdb_cache: Arc::default(),
            index_snapshot: None,
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn new_with_debuginfod_urls(
        store: Arc<dyn ObjectStore>,
        index: Arc<ProfileIndex>,
        urls: Vec<String>,
    ) -> Result<Self, ProfileError> {
        Self::new_with_debuginfod_config(store, index, urls, DebuginfodConfig::default())
    }

    /// Create a cold profile store with explicit debuginfod resource policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured debuginfod URL is invalid or its HTTP
    /// client cannot be built.
    pub fn new_with_debuginfod_config(
        store: Arc<dyn ObjectStore>,
        index: Arc<ProfileIndex>,
        urls: Vec<String>,
        config: DebuginfodConfig,
    ) -> Result<Self, ProfileError> {
        let mut resolvers: Vec<Arc<dyn NativeResolver>> =
            vec![Arc::new(FileSystemResolver::default())];
        if !urls.is_empty() {
            let debuginfod =
                DebuginfodResolver::with_config(urls, config).map_err(ProfileError::Store)?;
            resolvers.push(Arc::new(debuginfod));
        }
        resolvers.push(Arc::new(AddressFallbackResolver));
        Ok(Self {
            store,
            index: Arc::new(RwLock::new(index)),
            resolver: Arc::new(ChainedResolver::new(resolvers)),
            symdb_cache: Arc::default(),
            index_snapshot: None,
        })
    }

    /// Reloads this durable index when compaction retires a cached block.
    #[must_use]
    pub fn with_index_snapshot(mut self, key: String, max_bytes: ByteSize) -> Self {
        self.index_snapshot = Some((key, max_bytes));
        self
    }

    /// Current block index snapshot. The method clones the inner `Arc`, which is
    /// cheap, so it releases the lock immediately and never holds it across an
    /// `.await`.
    ///
    /// # Panics
    /// Panics if another thread poisoned the profile index lock.
    #[must_use]
    pub(crate) fn current_index(&self) -> Arc<ProfileIndex> {
        Arc::clone(&self.index.read().expect("profile index lock poisoned"))
    }

    /// Swap in a freshly-loaded block index so blocks written since the querier
    /// started become queryable. The periodic refresh task of the querier calls
    /// this method.
    ///
    /// # Panics
    /// Panics if another thread poisoned the profile index lock.
    pub fn replace_index(&self, index: Arc<ProfileIndex>) {
        *self.index.write().expect("profile index lock poisoned") = index;
    }

    fn index_block_keys(
        index: &ProfileIndex,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(Vec<String>, BTreeSet<SeriesFingerprint>), ProfileError> {
        let fps = index
            .select_fingerprints(tenant, profile_type, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok((Vec::new(), fps));
        }
        let blocks = index.candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        Ok((blocks, fps))
    }
    async fn select_from_index(
        &self,
        index: &ProfileIndex,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let (blocks, fps) =
            Self::index_block_keys(index, tenant, profile_type, matchers, start_ms, end_ms)?;
        let mut batches = Vec::new();
        let mut symbols = CompositeSymbols::default();
        // Keep partition identities and output order stable while overlapping
        // object-store waits. Four reads bound each query's transient decoding
        // memory independently of how many cold blocks its index selects.
        let mut loaded = futures::stream::iter(blocks.into_iter().enumerate().map(
            |(block_idx, block_key)| {
                let fps = &fps;
                async move {
                    // Dense local rebasing prevents collisions when a compacted
                    // block already uses high bits for its stored partitions.
                    let stored_partitions = index.stacktrace_partitions(&block_key);
                    let partition_map = block_partition_map(block_idx, &stored_partitions)?;
                    let symdb = self.load_symdb(&block_key).await?;
                    let batches = self
                        .load_block_batches(
                            &block_key,
                            &partition_map,
                            fps,
                            profile_type,
                            start_ms,
                            end_ms,
                        )
                        .await?;
                    Ok::<_, ProfileError>((partition_map, symdb, batches))
                }
            },
        ))
        .buffered(4);
        while let Some(result) = loaded.next().await {
            let (partition_map, symdb, block_batches) = result?;
            let source = Arc::new(LazySymbolizer::new(symdb, Arc::clone(&self.resolver)));
            for (source_partition, external) in &partition_map {
                symbols.insert(
                    ExternalPartition(*external),
                    source.clone(),
                    LocalPartition(*source_partition),
                );
            }
            batches.extend(block_batches);
        }

        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(profile_samples_schema()));
        }
        let table = MemTable::try_new(profile_samples_schema(), vec![batches])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = krabka_pprof::profile_session_context();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(symbols),
        })
    }
}

#[async_trait::async_trait]
impl ProfileStore for ColdProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let mut index = self.current_index();
        let mut retries = 2;
        loop {
            let failure = match self
                .select_from_index(&index, tenant, profile_type, matchers, start_ms, end_ms)
                .await
            {
                Ok(scan) => return Ok(scan),
                Err(error) => error,
            };
            let Some((key, max_bytes)) = &self.index_snapshot else {
                return Err(failure);
            };
            if retries == 0 {
                return Err(failure);
            }
            let latest =
                ProfileIndex::load_latest_snapshot_with_max_bytes(&self.store, key, *max_bytes)
                    .await
                    .map_err(|error| ProfileError::Store(error.to_string()))?;
            let (old_blocks, _) =
                Self::index_block_keys(&index, tenant, profile_type, matchers, start_ms, end_ms)?;
            let (new_blocks, _) =
                Self::index_block_keys(&latest, tenant, profile_type, matchers, start_ms, end_ms)?;
            // Replan the whole query only when its selected blocks were retired.
            // A failing block still in the durable index remains an error.
            if !old_blocks.iter().any(|block| !new_blocks.contains(block)) {
                return Err(failure);
            }
            index = Arc::new(latest);
            self.replace_index(Arc::clone(&index));
            retries -= 1;
        }
    }

    async fn query_stats(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileQueryStats, ProfileError> {
        let index = self.current_index();
        let selected = if profile_type.is_empty() {
            index.matching_fingerprints(tenant, matchers)
        } else {
            index.select_fingerprints(tenant, profile_type, matchers)
        }
        .map_err(|error| ProfileError::Store(error.to_string()))?;
        // Planning is time and tenant scoped. The selector controls queried
        // series, while physical cost includes each complete planned block.
        let blocks: Vec<_> = index
            .all_blocks()
            .into_iter()
            .filter(|block| {
                block.tenant == tenant && block.min_ts <= end_ms && block.max_ts >= start_ms
            })
            .collect();
        let mut stats = ProfileQueryStats::default();
        let mut queried_storage_fingerprints = BTreeSet::new();
        let mut scope = krabka_pprof::ProfileQueryScope {
            component_type: "Long term storage",
            component_count: u64::from(!blocks.is_empty()),
            block_count: blocks.len() as u64,
            ..Default::default()
        };
        for block in blocks {
            let bytes = self
                .store
                .get(&Path::from(block.object_key.clone()))
                .await
                .map_err(|error| ProfileError::Store(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| ProfileError::Store(error.to_string()))?;
            if bytes.len() < 8 {
                return Err(ProfileError::Store("invalid profile Parquet footer".into()));
            }
            let footer_length = u64::from(u32::from_le_bytes(
                bytes[bytes.len() - 8..bytes.len() - 4].try_into().map_err(
                    |error: std::array::TryFromSliceError| ProfileError::Store(error.to_string()),
                )?,
            )) + 8;
            let body_length = (bytes.len() as u64)
                .checked_sub(footer_length)
                .ok_or_else(|| {
                    ProfileError::Store("invalid profile Parquet footer length".into())
                })?;
            scope.index_bytes = scope.index_bytes.saturating_add(footer_length);
            scope.profile_bytes = scope.profile_bytes.saturating_add(body_length);
            scope.symbol_bytes = scope.symbol_bytes.saturating_add(
                self.store
                    .head(&Path::from(format!("{}.symdb", block.object_key)))
                    .await
                    .map_err(|error| ProfileError::Store(error.to_string()))?
                    .size,
            );
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|error| ProfileError::Store(error.to_string()))?
                .build()
                .map_err(|error| ProfileError::Store(error.to_string()))?;
            let mut block_series = BTreeSet::new();
            let mut block_profiles = BTreeSet::new();
            for batch in reader {
                let batch = batch.map_err(|error| ProfileError::Store(error.to_string()))?;
                let fingerprints = batch
                    .column_by_name(krabka_blockstore::COL_FINGERPRINT)
                    .ok_or_else(|| ProfileError::Store("missing fingerprint".into()))?
                    .as_primitive::<UInt64Type>();
                let timestamps = batch
                    .column_by_name("timestamp")
                    .ok_or_else(|| ProfileError::Store("missing timestamp".into()))?
                    .as_primitive::<Int64Type>();
                let types = arrow::compute::cast(
                    batch
                        .column_by_name("profile_type")
                        .ok_or_else(|| ProfileError::Store("missing profile type".into()))?,
                    &arrow::datatypes::DataType::Utf8,
                )
                .map_err(|error| ProfileError::Store(error.to_string()))?;
                let types = types.as_string::<i32>();
                for row in 0..batch.num_rows() {
                    let fp = fingerprints.value(row);
                    block_series.insert(fp);
                    block_profiles.insert((
                        fp,
                        types.value(row).to_string(),
                        timestamps.value(row),
                    ));
                    if selected.contains(&fp) {
                        queried_storage_fingerprints.insert(fp);
                    }
                }
                scope.sample_count = scope.sample_count.saturating_add(batch.num_rows() as u64);
            }
            scope.series_count = scope.series_count.saturating_add(block_series.len() as u64);
            scope.profile_count = scope
                .profile_count
                .saturating_add(block_profiles.len() as u64);
            stats.deduplication_needed |= !stats.profiles.is_disjoint(&block_profiles);
            stats.profiles.extend(block_profiles);
        }
        for labels in index.series_for_fingerprints(tenant, &queried_storage_fingerprints, &[]) {
            let labels = krabka_blockstore::Labels::from_pairs(
                labels
                    .into_iter()
                    .filter(|(name, _)| name != "__profile_id__"),
            );
            stats.fingerprints.insert(labels.fingerprint());
        }
        stats.block_count = scope.block_count;
        stats.profile_count = scope.profile_count;
        stats.sample_count = scope.sample_count;
        stats.scopes.push(scope);
        Ok(stats)
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .label_names_for(tenant, matchers)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .label_names_for_fingerprints(tenant, &active))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .label_values_for(tenant, name, matchers)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .label_values_for_fingerprints(tenant, name, &active))
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return Ok(self.current_index().profile_types(tenant));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, &[], start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .profile_types_for_fingerprints(tenant, &active))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .series(tenant, matchers, label_names)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .series_for_fingerprints(tenant, &active, label_names))
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        // Derive the tenant's profile-time bounds from the per-block `min_ts`/
        // `max_ts` the index already tracks instead of loading and scanning every
        // candidate block's sample rows. `GetProfileStats` is unbounded
        // (`[0, i64::MAX]`), so a row scan reads the entire dataset on every
        // Grafana Profiles-Drilldown load; the index aggregate is in-memory and
        // O(blocks). Block bounds intersected with `[start_ms, end_ms]` are clamped
        // to the requested window so a narrower query never reports times outside
        // it, and `data_ingested` is true iff the tenant has any overlapping block.
        let bounds = self
            .current_index()
            .block_time_bounds(tenant, start_ms, end_ms)
            .map(|(block_min, block_max)| (block_min.max(start_ms), block_max.min(end_ms)));
        Ok(ProfileStats {
            data_ingested: bounds.is_some(),
            oldest_profile_time: bounds.map(|(oldest, _)| oldest),
            newest_profile_time: bounds.map(|(_, newest)| newest),
        })
    }
}

impl ColdProfileStore {
    pub(crate) async fn active_fingerprints_for_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeSet<SeriesFingerprint>, ProfileError> {
        let fps = self
            .current_index()
            .matching_fingerprints(tenant, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok(BTreeSet::new());
        }
        let blocks = self
            .current_index()
            .candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        let mut active = BTreeSet::new();
        for block_key in blocks {
            for batch in self
                .load_block_batches_for_fingerprints(&block_key, &fps)
                .await?
            {
                let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
                let timestamps = batch.column(1).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    let fp = fingerprints.value(row);
                    if timestamps.value(row) >= start_ms && timestamps.value(row) <= end_ms {
                        active.insert(fp);
                    }
                }
            }
        }
        Ok(active)
    }

    async fn object_bytes(&self, path: &Path) -> Result<Bytes, ProfileError> {
        self.store
            .get(path)
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))
    }

    async fn open_block_reader(
        &self,
        block_key: &str,
    ) -> Result<ParquetRecordBatchReader, ProfileError> {
        let bytes = self.object_bytes(&Path::from(block_key)).await?;
        ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .build()
            .map_err(|err| ProfileError::Store(err.to_string()))
    }

    pub(crate) async fn load_block_batches_for_fingerprints(
        &self,
        block_key: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Result<Vec<RecordBatch>, ProfileError> {
        let reader = self.open_block_reader(block_key).await?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|err| ProfileError::Store(err.to_string()))?;
            if batch_fingerprints_overlap(&batch, fps) {
                out.push(batch);
            }
        }
        Ok(out)
    }

    pub(crate) async fn load_symdb(&self, block_key: &str) -> Result<SymbolDb, ProfileError> {
        {
            let mut cache = self.symdb_cache.lock().expect("symbol cache lock poisoned");
            if let Some(symbols) = cached_symbol_db(&mut cache, block_key, Instant::now()) {
                return Ok(symbols);
            }
        }
        let key = format!("{block_key}.symdb");
        let bytes = self.object_bytes(&Path::from(key)).await?;
        let symbols = SymbolDb::decode(&bytes)?;
        let mut cache = self.symdb_cache.lock().expect("symbol cache lock poisoned");
        if !cache.0.contains_key(block_key) {
            while cache.0.len() >= 128 {
                if let Some(oldest) = cache.1.pop_front() {
                    cache.0.remove(&oldest);
                }
            }
            cache.0.insert(
                block_key.to_string(),
                (Arc::new(symbols.clone()), Instant::now()),
            );
            cache.1.push_back(block_key.to_string());
        }
        Ok(symbols)
    }

    pub(crate) async fn load_block_batches(
        &self,
        block_key: &str,
        partition_map: &BTreeMap<u64, u64>,
        fps: &std::collections::BTreeSet<SeriesFingerprint>,
        profile_type: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RecordBatch>, ProfileError> {
        let reader = self.open_block_reader(block_key).await?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|err| ProfileError::Store(err.to_string()))?;
            let filtered =
                filter_and_remap_batch(&batch, partition_map, fps, profile_type, start_ms, end_ms)?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }
}
