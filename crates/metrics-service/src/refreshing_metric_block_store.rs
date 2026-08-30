use super::{
    Arc, BTreeMap, BlockStore, CachedMetricBlockStore, CompactionIndexManifest,
    DEFAULT_COLD_CACHE_TTL, DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK, ExemplarRecord, Instant,
    LabelMatcher, LabelNameCardinality, LabelValueCardinality, Labels, MergedMetricStore,
    MetadataRecord, MetricBlockStore, MetricStore, MetricsServiceError, ObjectStore, ScanResult,
    Time, TsdbBlock, Url, WalHead, load_compaction_manifests_for_range_with_cache,
    normalize_refresh_range, unix_time_ms,
};

pub struct RefreshingMetricBlockStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) base: Url,
    pub(crate) manifest_prefix: String,
    pub(crate) hot_store: WalHead,
    pub(crate) manifest_cache: Arc<tokio::sync::RwLock<BTreeMap<String, CompactionIndexManifest>>>,
    pub(crate) cold_cache: Arc<tokio::sync::RwLock<Option<CachedMetricBlockStore>>>,
    pub(crate) cold_refresh: tokio::sync::Mutex<()>,
    pub(crate) cold_cache_ttl: Time,
    pub(crate) unbounded_compatibility_lookback: Time,
}

impl RefreshingMetricBlockStore {
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base: Url,
        manifest_prefix: impl Into<String>,
        hot_store: WalHead,
    ) -> Self {
        Self {
            store,
            base,
            manifest_prefix: manifest_prefix.into(),
            hot_store,
            manifest_cache: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())),
            cold_cache: Arc::new(tokio::sync::RwLock::new(None)),
            cold_refresh: tokio::sync::Mutex::new(()),
            cold_cache_ttl: DEFAULT_COLD_CACHE_TTL,
            unbounded_compatibility_lookback: DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK,
        }
    }

    #[must_use]
    pub fn with_cold_cache_ttl(mut self, ttl: Time) -> Self {
        self.cold_cache_ttl = ttl;
        self
    }

    #[must_use]
    pub fn with_unbounded_compatibility_lookback(mut self, lookback: Time) -> Self {
        self.unbounded_compatibility_lookback = lookback;
        self
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.current_store",
        skip_all,
        fields(start_ms, end_ms, cold_refreshed = tracing::field::Empty),
        err
    )]
    pub(crate) async fn current_store(
        &self,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<MergedMetricStore<MetricBlockStore, WalHead>, MetricsServiceError> {
        let (start_ms, end_ms) = normalize_refresh_range(
            start_ms,
            end_ms,
            self.unbounded_compatibility_lookback,
            unix_time_ms(),
        );

        {
            let guard = self.cold_cache.read().await;
            if let Some(entry) = guard.as_ref()
                && entry.covers(start_ms, end_ms, self.cold_cache_ttl)
            {
                return Ok(MergedMetricStore::new(
                    entry.cold.clone(),
                    self.hot_store.clone(),
                ));
            }
        }

        let _refresh_guard = self.cold_refresh.lock().await;
        {
            let guard = self.cold_cache.read().await;
            if let Some(entry) = guard.as_ref()
                && entry.covers(start_ms, end_ms, self.cold_cache_ttl)
            {
                return Ok(MergedMetricStore::new(
                    entry.cold.clone(),
                    self.hot_store.clone(),
                ));
            }
        }

        let manifests = load_compaction_manifests_for_range_with_cache(
            self.store.clone(),
            &self.manifest_prefix,
            start_ms,
            end_ms,
            &self.manifest_cache,
        )
        .await?;
        let cold = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(self.store.clone(), self.base.clone()),
            Some(BlockStore::new(self.store.clone(), self.base.clone())),
            &manifests,
        );
        let merged = MergedMetricStore::new(cold.clone(), self.hot_store.clone());
        *self.cold_cache.write().await = Some(CachedMetricBlockStore {
            cached_at: Instant::now(),
            start_ms,
            end_ms,
            cold,
        });
        tracing::Span::current().record("cold_refreshed", true);
        Ok(merged)
    }
}

#[async_trait::async_trait]
impl MetricStore for RefreshingMetricBlockStore {
    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.scan",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, krabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .scan(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.label_names",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, krabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .label_names(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.label_values",
        skip_all,
        fields(tenant = %tenant, label = %name, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, krabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.series",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, krabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .series(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.exemplars",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms, end_ms),
        err
    )]
    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, krabka_promql::PromqlError> {
        self.current_store(start_ms, end_ms)
            .await?
            .exemplars(tenant, matchers, start_ms, end_ms)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.metadata",
        skip_all,
        fields(tenant = %tenant, metric = metric.unwrap_or("")),
        err
    )]
    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .metadata(tenant, metric)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_label_names",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_label_names(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_label_names(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_label_values",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_label_values(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_label_values(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.cardinality_active_series",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn cardinality_active_series(
        &self,
        tenant: &str,
    ) -> Result<Vec<Labels>, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .cardinality_active_series(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.tsdb_stats",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn tsdb_stats(
        &self,
        tenant: &str,
    ) -> Result<krabka_promql::TsdbStats, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .tsdb_stats(tenant)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "metrics.store.tsdb_blocks",
        skip_all,
        fields(tenant = %tenant),
        err
    )]
    async fn tsdb_blocks(
        &self,
        tenant: &str,
    ) -> Result<Vec<TsdbBlock>, krabka_promql::PromqlError> {
        self.current_store(i64::MIN, i64::MAX)
            .await?
            .tsdb_blocks(tenant)
            .await
    }
}
