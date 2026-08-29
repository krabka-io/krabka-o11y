#[allow(clippy::wildcard_imports)]
use super::*;

impl QuerierState {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, label_index: LabelIndex, block_index: BlockIndex) -> Self {
        Self {
            root: root.into(),
            label_index,
            block_index,
            cold_store: None,
            dynamic_index: None,
            dynamic_index_cache: DynamicIndexCache::default(),
            cold_block_fetch_concurrency: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            hot_tail: None,
            delete_requests: None,
            rules: SharedLokiRules::default(),
            alert_states: SharedPrometheusAlertStates::default(),
            query_authorizer: Arc::new(AllowAllQueryAuthorizer),
            max_query_range: None,
            max_query_series: None,
            max_query_read: None,
            max_query_length: None,
            metrics: None,
        }
    }

    /// Threads a shared RED-metrics bundle, so each querier handler records
    /// its per-route request count and latency on the same registry the
    /// `:9404` exporter serves. It is a no-op when left unset, as in test
    /// routers.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Records one querier request outcome, that is the per-route count and
    /// latency, on the shared bundle. It is a no-op when metrics are not
    /// wired, as in test routers.
    pub(crate) fn record_query(&self, route: &str, ok: bool, start: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(route, ok, start.elapsed().as_time());
        }
    }

    #[must_use]
    pub fn with_max_query_range(mut self, max_query_range: Time) -> Self {
        self.max_query_range = Some(max_query_range);
        self
    }

    #[must_use]
    pub fn with_max_query_series(mut self, max_query_series: usize) -> Self {
        self.max_query_series = Some(max_query_series);
        self
    }

    #[must_use]
    pub fn with_max_query_read(mut self, max_query_read: ByteSize) -> Self {
        self.max_query_read = Some(max_query_read);
        self
    }

    #[must_use]
    pub fn with_max_query_length(mut self, max_query_length: ByteSize) -> Self {
        self.max_query_length = Some(max_query_length);
        self
    }

    #[must_use]
    pub fn with_query_authorizer(mut self, authorizer: impl LogQueryAuthorizer) -> Self {
        self.query_authorizer = Arc::new(authorizer);
        self
    }

    pub(crate) fn with_query_authorizer_source(
        mut self,
        authorizer: Arc<dyn LogQueryAuthorizer>,
    ) -> Self {
        self.query_authorizer = authorizer;
        self
    }

    #[must_use]
    pub fn with_hot_tail(self, source: impl LogHotTail, compacted_through_ns: i64) -> Self {
        self.with_hot_tail_frontier(source, CompactionFrontier::new(compacted_through_ns))
    }

    #[must_use]
    pub fn with_hot_tail_frontier(
        self,
        source: impl LogHotTail,
        frontier: CompactionFrontier,
    ) -> Self {
        self.with_hot_tail_source(
            Arc::new(source),
            CompactionFrontierSource::Snapshot(frontier),
        )
    }

    #[must_use]
    pub fn with_hot_tail_shared_frontier(
        self,
        source: impl LogHotTail,
        frontier: SharedCompactionFrontier,
    ) -> Self {
        self.with_hot_tail_source(Arc::new(source), CompactionFrontierSource::Shared(frontier))
    }

    pub(crate) fn with_hot_tail_source(
        mut self,
        source: Arc<dyn LogHotTail>,
        frontier: CompactionFrontierSource,
    ) -> Self {
        self.hot_tail = Some(HotTailState { source, frontier });
        self
    }

    pub(crate) fn with_delete_requests(mut self, requests: SharedLogDeleteRequests) -> Self {
        self.delete_requests = Some(requests);
        self
    }

    pub(crate) fn with_rules(mut self, rules: SharedLokiRules) -> Self {
        self.rules = rules;
        self
    }

    pub(crate) fn with_cold_object_store_source(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.cold_store = Some(ColdObjectStoreState { store, prefix });
        self
    }

    pub(crate) fn with_runtime_policy(mut self, config: &ServiceConfig) -> Self {
        self.dynamic_index_cache.cache_ttl = config.querier_dynamic_index_cache_ttl;
        self.dynamic_index_cache.shard_cache_ttl = config.querier_shard_index_cache_ttl;
        self.dynamic_index_cache.shard_fetch_concurrency = config.querier_shard_fetch_concurrency;
        self.cold_block_fetch_concurrency = config.querier_cold_block_fetch_concurrency;
        self
    }

    pub(crate) fn with_dynamic_tenant_object_store_manifest(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.dynamic_index_cache.clear();
        self.dynamic_index = Some(DynamicIndexSource::TenantObjectStoreManifest { store, prefix });
        self
    }

    pub(crate) fn with_dynamic_tenant_object_store_shards(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.dynamic_index_cache.clear();
        self.dynamic_index = Some(DynamicIndexSource::TenantObjectStoreShards { store, prefix });
        self
    }

    pub(crate) async fn with_request_tenant_index(
        &self,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let Some(dynamic_index) = &self.dynamic_index else {
            return Ok(self.clone());
        };

        match dynamic_index {
            DynamicIndexSource::TenantObjectStoreManifest { store, prefix } => {
                let cache_key = DynamicIndexCacheKey::TenantManifest {
                    tenant: tenant.to_string(),
                };
                if let Some((label_index, block_index)) = self.dynamic_index_cache.get(&cache_key) {
                    let mut state = self.clone();
                    state.label_index = label_index;
                    state.block_index = block_index;
                    return Ok(state);
                }
                let (label_index, block_index) =
                    match read_tenant_log_index_manifest_from_object_store(
                        store.as_ref(),
                        prefix,
                        tenant,
                    )
                    .await
                    {
                        Ok(indexes) => indexes,
                        Err(BlockStoreError::ObjectStore(object_store::Error::NotFound {
                            ..
                        })) => {
                            return Ok(self.clone());
                        }
                        Err(error) => return Err(error),
                    };
                self.dynamic_index_cache.insert(
                    cache_key,
                    label_index.clone(),
                    block_index.clone(),
                );
                let mut state = self.clone();
                state.label_index = label_index;
                state.block_index = block_index;
                Ok(state)
            }
            DynamicIndexSource::TenantObjectStoreShards { store, prefix } => {
                let cache_key = DynamicIndexCacheKey::TenantShards {
                    tenant: tenant.to_string(),
                    start_ns: query_range.start_ns,
                    end_ns: query_range.end_ns,
                };
                if let Some((label_index, block_index)) = self.dynamic_index_cache.get(&cache_key) {
                    let mut state = self.clone();
                    state.label_index = label_index;
                    state.block_index = block_index;
                    return Ok(state);
                }
                let (label_index, block_index) = self
                    .cached_tenant_shard_indexes(store.as_ref(), prefix, tenant, query_range)
                    .await?;
                self.dynamic_index_cache.insert(
                    cache_key,
                    label_index.clone(),
                    block_index.clone(),
                );
                let mut state = self.clone();
                state.label_index = label_index;
                state.block_index = block_index;
                Ok(state)
            }
        }
    }

    pub(crate) async fn cached_tenant_shard_ranges(
        &self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Vec<TimeRange>, BlockStoreError> {
        let required_from_ns =
            krabka_blockstore::log_tenant_index_shard_list_offset_start_ns(query_range);
        let cache_key = DynamicShardRangesCacheKey {
            tenant: tenant.to_string(),
        };
        if let Some(ranges) = self
            .dynamic_index_cache
            .get_shard_ranges(&cache_key, required_from_ns)
        {
            return Ok(ranges);
        }

        let mut shard_ranges =
            krabka_blockstore::list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
                store,
                prefix,
                tenant,
                query_range,
            )
            .await?;
        if shard_ranges.is_empty() {
            shard_ranges =
                match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant)
                    .await
                {
                    Ok(shard_ranges) => shard_ranges,
                    Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                        Vec::new()
                    }
                    Err(error) => return Err(error),
                };
        }

        self.dynamic_index_cache.insert_shard_ranges(
            cache_key,
            required_from_ns,
            shard_ranges.clone(),
        );
        Ok(shard_ranges)
    }

    pub(crate) async fn cached_tenant_shard_indexes(
        &self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        let shard_ranges = self
            .cached_tenant_shard_ranges(store, prefix, tenant, query_range)
            .await?;
        let mut indexes = Vec::new();
        let mut misses = Vec::new();

        for shard_range in shard_ranges
            .into_iter()
            .filter(|shard_range| shard_range.overlaps(query_range))
        {
            let cache_key = DynamicShardIndexCacheKey {
                tenant: tenant.to_string(),
                start_ns: shard_range.start_ns,
                end_ns: shard_range.end_ns,
            };
            if let Some(index) = self.dynamic_index_cache.get_shard_index(&cache_key) {
                indexes.push(index);
            } else {
                misses.push(shard_range);
            }
        }

        let fetched = futures_util::stream::iter(misses)
            .map(|shard_range| async move {
                let (label_index, block_index) = read_tenant_log_index_shard_from_object_store(
                    store,
                    prefix,
                    tenant,
                    shard_range,
                )
                .await?;
                Ok::<_, BlockStoreError>((shard_range, label_index, block_index))
            })
            .buffer_unordered(self.dynamic_index_cache.shard_fetch_concurrency.get())
            .try_collect::<Vec<_>>()
            .await?;

        for (shard_range, label_index, block_index) in fetched {
            let cache_key = DynamicShardIndexCacheKey {
                tenant: tenant.to_string(),
                start_ns: shard_range.start_ns,
                end_ns: shard_range.end_ns,
            };
            self.dynamic_index_cache.insert_shard_index(
                cache_key,
                label_index.clone(),
                block_index.clone(),
            );
            indexes.push((label_index, block_index));
        }

        Ok(merge_tenant_shard_indexes(tenant, indexes))
    }

    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub fn from_manifest(root: impl Into<PathBuf>) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) = read_log_index_manifest(&root)?;
        Ok(Self::new(root, label_index, block_index))
    }

    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn from_tenant_object_store(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_manifest_from_object_store(store, prefix, tenant).await?;
        Ok(Self::new(root, label_index, block_index))
    }

    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn from_tenant_object_store_shard(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        shard_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;
        Ok(Self::new(root, label_index, block_index))
    }

    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn from_tenant_object_store_shards(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_shards_from_object_store(store, prefix, tenant, query_range)
                .await?;
        Ok(Self::new(root, label_index, block_index))
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_querier_state(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<QuerierState, ServiceConfigError> {
    build_querier_state_with_object_store_prefix(config, object_store, None).await
}

pub(crate) async fn build_querier_state_with_object_store_prefix(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<QuerierState, ServiceConfigError> {
    let state = match config.querier_index_source {
        QuerierIndexSource::LocalManifest => QuerierState::from_manifest(config.data_root.clone())?,
        QuerierIndexSource::TenantObjectStoreManifest => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            QuerierState::from_tenant_object_store(config.data_root.clone(), store, &prefix, tenant)
                .await?
        }
        QuerierIndexSource::TenantObjectStoreShards => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            let start_ns = config
                .query_start_ns
                .ok_or(ServiceConfigError::MissingQueryStartNs)?;
            let end_ns = config
                .query_end_ns
                .ok_or(ServiceConfigError::MissingQueryEndNs)?;

            QuerierState::from_tenant_object_store_shards(
                config.data_root.clone(),
                store,
                &prefix,
                tenant,
                TimeRange::new(start_ns, end_ns)?,
            )
            .await?
        }
    }
    .with_runtime_policy(config);

    let state = if let Some(max_query_range) = config.max_query_range {
        state.with_max_query_range(max_query_range)
    } else {
        state
    };

    let state = if let Some(max_query_series) = config.max_query_series {
        state.with_max_query_series(max_query_series)
    } else {
        state
    };

    let state = if let Some(max_query_read) = config.max_query_read {
        state.with_max_query_read(max_query_read)
    } else {
        state
    };

    Ok(if let Some(max_query_length) = config.max_query_length {
        state.with_max_query_length(max_query_length)
    } else {
        state
    })
}
