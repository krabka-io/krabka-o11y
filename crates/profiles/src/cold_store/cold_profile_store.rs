use super::{
    AddressFallbackResolver, Arc, AsArray, BTreeMap, BTreeSet, ChainedResolver, CompositeSymbols,
    DebuginfodConfig, DebuginfodResolver, ExternalPartition, FileSystemResolver, Int64Type,
    LabelMatcher, LazySymbolizer, LocalPartition, MemTable, NativeResolver, ObjectStore,
    ObjectStoreExt, ParquetRecordBatchReaderBuilder, Path, ProfileError, ProfileIndex, ProfileScan,
    ProfileStats, ProfileStore, RecordBatch, RwLock, SeriesFingerprint, SessionContext, SymbolDb,
    UInt64Type, batch_fingerprints_overlap, block_partition_map, filter_and_remap_batch,
    is_unbounded_metadata_range, local_native_resolver, profile_samples_schema,
};

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
}

impl ColdProfileStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, index: Arc<ProfileIndex>) -> Self {
        Self {
            store,
            index: Arc::new(RwLock::new(index)),
            resolver: local_native_resolver(),
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
        })
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

    pub(crate) fn block_keys(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(Vec<String>, std::collections::BTreeSet<SeriesFingerprint>), ProfileError> {
        let fps = self
            .current_index()
            .select_fingerprints(tenant, profile_type, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok((Vec::new(), fps));
        }
        let blocks = self
            .current_index()
            .candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        Ok((blocks, fps))
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
        let (blocks, fps) = self.block_keys(tenant, profile_type, matchers, start_ms, end_ms)?;
        let mut batches = Vec::new();
        let mut symbols = CompositeSymbols::default();
        for (block_idx, block_key) in blocks.iter().enumerate() {
            // Re-base this block's stored partitions to a dense local `0..n` range
            // before OR-ing the per-block high-bit base. A block that has already
            // been compacted stores partitions that occupy the high bits; OR-ing a
            // fresh base straight onto them folds bits together and can collide
            // across blocks (e.g. `1<<32 | (2<<32)` and `2<<32 | (1<<32)` both ==
            // `3<<32`). Dense re-basing keeps each block's external keys unique.
            let stored_partitions = self.current_index().stacktrace_partitions(block_key);
            let partition_map = block_partition_map(block_idx, &stored_partitions)?;
            let symdb = self.load_symdb(block_key).await?;
            let source = Arc::new(LazySymbolizer::new(symdb, Arc::clone(&self.resolver)));
            for (source_partition, external) in &partition_map {
                // `source_partition` is the partition key within this block's own
                // symbol DB, so resolution stays scoped to the correct block.
                symbols.insert(
                    ExternalPartition(*external),
                    source.clone(),
                    LocalPartition(*source_partition),
                );
            }
            batches.extend(
                self.load_block_batches(
                    block_key,
                    &partition_map,
                    &fps,
                    profile_type,
                    start_ms,
                    end_ms,
                )
                .await?,
            );
        }

        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(profile_samples_schema()));
        }
        let table = MemTable::try_new(profile_samples_schema(), vec![batches])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = SessionContext::new();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(symbols),
        })
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

    pub(crate) async fn load_block_batches_for_fingerprints(
        &self,
        block_key: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Result<Vec<RecordBatch>, ProfileError> {
        let bytes = self
            .store
            .get(&Path::from(block_key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .build()
            .map_err(|err| ProfileError::Store(err.to_string()))?;
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
        let key = format!("{block_key}.symdb");
        let bytes = self
            .store
            .get(&Path::from(key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        SymbolDb::decode(&bytes)
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
        let bytes = self
            .store
            .get(&Path::from(block_key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .build()
            .map_err(|err| ProfileError::Store(err.to_string()))?;
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
