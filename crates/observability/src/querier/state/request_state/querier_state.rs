use axum::http::StatusCode;

use super::*;
use crate::{
    ByteSize, Time, WalLogRecord,
    service_metrics::{QueryRequest, RequestOutcome},
};

struct SnapshotLogHotTail(Vec<Arc<WalLogRecord>>);

impl LogHotTail for SnapshotLogHotTail {
    fn records(&self) -> Vec<WalLogRecord> {
        self.0
            .iter()
            .map(|record| record.as_ref().clone())
            .collect()
    }

    fn records_shared_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<Arc<WalLogRecord>> {
        self.0
            .iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }
}

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
            overrides: Arc::new(OverridesProvider::new(Limits::default())),
            limits: Limits::default(),
            max_count_min_sketch_heap_size: 10_000,
            federated_metric_tenants: None,
            metrics: None,
            query_frontend_cache: Arc::new(
                InMemoryCache::<Value>::new(days(7).to_std()).with_weigher(json_value_bytes),
            ),
            query_frontend_options: ExecutionOptions::default(),
            query_frontend_split_ns: hours(1).nanos_i64(),
            query_frontend_target_bytes: krabka_units::mebibytes(600).bytes_u64(),
        }
    }

    /// Threads a shared RED-metrics bundle, so each querier handler records
    /// its per-route request count and latency on the same registry the
    /// `:9404` exporter serves. It is a no-op when left unset, as in test
    /// routers.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        if let Ok(mut registry) = metrics.registry.try_lock() {
            self.query_frontend_cache.metrics().register(&mut registry);
        } else {
            tracing::error!("query cache metrics registry is busy during setup");
        }
        self.metrics = Some(metrics);
        self
    }

    /// Records one querier request outcome, that is the per-route count and
    /// latency, on the shared bundle. It is a no-op when metrics are not
    /// wired, as in test routers.
    ///
    /// A 2xx `status` counts as `status="ok"`, anything else as
    /// `status="error"`.
    pub(crate) fn record_query(&self, route: &str, status: StatusCode, start: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(QueryRequest {
                route,
                outcome: RequestOutcome::from_success_status(status),
                elapsed: start.elapsed().as_time(),
            });
        }
    }

    /// Gives every tenant the same limits.
    #[must_use]
    pub fn with_limits(self, limits: Limits) -> Self {
        self.with_limits_overrides(OverridesProvider::new(limits))
    }

    /// Resolves each tenant's limits through `overrides`.
    ///
    /// The state starts on the provider's defaults, and each read path resolves
    /// one tenant's limits once it knows whose request it is.
    #[must_use]
    pub fn with_limits_overrides(self, overrides: OverridesProvider) -> Self {
        self.with_limits_overrides_source(Arc::new(overrides))
    }

    pub(crate) fn with_limits_overrides_source(
        mut self,
        overrides: Arc<OverridesProvider>,
    ) -> Self {
        self.limits = overrides.defaults().clone();
        self.overrides = overrides;
        self
    }

    /// Resolves one tenant's limits, once, for the request in hand.
    pub(crate) fn with_tenant_limits(&self, tenant: &TenantId) -> Self {
        let mut state = self.clone();
        if self.federated_metric_tenants.is_none() {
            state.limits = self.overrides.for_tenant(tenant).clone();
        }
        state
    }

    pub(crate) fn with_federated_metric_tenants(&self, tenants: &[TenantId]) -> Self {
        let mut state = self.clone();
        let limits = tenants
            .iter()
            .map(|tenant| self.overrides.for_tenant(tenant))
            .collect::<Vec<_>>();
        if let Some(first) = limits.first() {
            state.limits = (*first).clone();
            state.limits.enable_multi_variant_queries = limits
                .iter()
                .any(|limit| limit.enable_multi_variant_queries);
            state.limits.max_query_series = limits
                .iter()
                .map(|limit| limit.max_query_series)
                .min()
                .unwrap_or_default();
            state.limits.shard_aggregations.retain(|name| {
                limits
                    .iter()
                    .all(|limit| limit.shard_aggregations.contains(name))
            });
            for limit in limits {
                for (value, candidate) in [
                    (&mut state.limits.max_query_length, limit.max_query_length),
                    (
                        &mut state.limits.max_query_lookback,
                        limit.max_query_lookback,
                    ),
                    (&mut state.limits.max_query_range, limit.max_query_range),
                ] {
                    if candidate > Time::ZERO && (*value == Time::ZERO || candidate < *value) {
                        *value = candidate;
                    }
                }
                for (value, candidate) in [
                    (&mut state.limits.max_query_read, limit.max_query_read),
                    (
                        &mut state.limits.max_query_string_bytes,
                        limit.max_query_string_bytes,
                    ),
                ] {
                    if candidate > ByteSize::ZERO
                        && (*value == ByteSize::ZERO || candidate < *value)
                    {
                        *value = candidate;
                    }
                }
                let candidate = limit.max_entries_limit_per_query;
                if candidate > 0
                    && (state.limits.max_entries_limit_per_query == 0
                        || candidate < state.limits.max_entries_limit_per_query)
                {
                    state.limits.max_entries_limit_per_query = candidate;
                }
            }
        }
        state.federated_metric_tenants = Some(Arc::from(tenants));
        state
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
        self.max_count_min_sketch_heap_size = config.max_count_min_sketch_heap_size;
        self.dynamic_index_cache.cache_ttl = config.querier_dynamic_index_cache_ttl;
        self.dynamic_index_cache.shard_cache_ttl = config.querier_shard_index_cache_ttl;
        self.dynamic_index_cache.shard_fetch_concurrency = config.querier_shard_fetch_concurrency;
        self.cold_block_fetch_concurrency = config.querier_cold_block_fetch_concurrency;
        let cache_metrics = self.query_frontend_cache.metrics();
        self.query_frontend_cache = Arc::new(
            InMemoryCache::new(config.querier_query_frontend_cache_ttl.to_std())
                .with_weigher(json_value_bytes)
                .with_metrics(cache_metrics),
        );
        self.query_frontend_options = ExecutionOptions {
            max_parallelism: config.querier_query_frontend_max_parallelism,
            max_retries: config.querier_query_frontend_max_retries,
            max_cache_freshness: config.querier_query_frontend_max_cache_freshness.to_std(),
        };
        self.query_frontend_split_ns = config.querier_query_frontend_split_interval.nanos_i64();
        self.query_frontend_target_bytes = config
            .querier_query_frontend_target_bytes_per_shard
            .bytes_u64();
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
        self.with_request_tenant_index_and_hot_range(tenant, query_range, Some(query_range))
            .await
    }

    pub(crate) async fn with_request_tenant_index_and_hot_range(
        &self,
        tenant: &str,
        query_range: TimeRange,
        hot_range: Option<TimeRange>,
    ) -> Result<Self, BlockStoreError> {
        if self.dynamic_index.is_none() {
            return Ok(self.clone());
        }
        loop {
            let mut request = self.clone();
            let captured = self.hot_tail.as_ref().map(|hot_tail| {
                let (version, frontier) = match &hot_tail.frontier {
                    CompactionFrontierSource::Shared(shared) => {
                        let (version, frontier) = shared.snapshot_with_version();
                        request.dynamic_index_cache =
                            self.dynamic_index_cache.for_frontier(shared, version);
                        (Some(version), frontier)
                    }
                    CompactionFrontierSource::Snapshot(frontier) => (None, frontier.clone()),
                };
                (hot_tail, version, frontier)
            });
            let loaded = request.with_loaded_tenant_index(tenant, query_range).await;
            let records = captured.as_ref().map(|(hot_tail, _, _)| {
                let range = hot_range.unwrap_or(TimeRange {
                    start_ns: i64::MIN,
                    end_ns: i64::MAX,
                });
                hot_tail
                    .source
                    .records_shared_in_range(range.start_ns, range.end_ns)
            });
            if let Some((hot_tail, Some(version), _)) = &captured
                && let CompactionFrontierSource::Shared(shared) = &hot_tail.frontier
                && shared.snapshot_with_version().0 != *version
            {
                continue;
            }
            let mut request = loaded?;
            if let Some((_, _, frontier)) = captured {
                request = request.with_hot_tail_frontier(
                    SnapshotLogHotTail(records.expect("hot tail snapshot exists")),
                    frontier,
                );
            }
            request.dynamic_index = None;
            return Ok(request);
        }
    }

    async fn with_loaded_tenant_index(
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
    ) -> Result<Vec<TimeRange>, BlockStoreError> {
        let cache_key = DynamicShardRangesCacheKey {
            tenant: tenant.to_string(),
        };
        if let Some(ranges) = self
            .dynamic_index_cache
            .get_shard_ranges(&cache_key, i64::MIN)
        {
            return Ok(ranges);
        }

        // A later query can extend either bound, so cache all tenant ranges.
        let mut shard_ranges =
            krabka_blockstore::list_tenant_log_index_shard_ranges_from_object_store(
                store, prefix, tenant,
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

        self.dynamic_index_cache
            .insert_shard_ranges(cache_key, i64::MIN, shard_ranges.clone());
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
            .cached_tenant_shard_ranges(store, prefix, tenant)
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

fn json_value_bytes(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}
