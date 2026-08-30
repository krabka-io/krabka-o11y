use super::{
    Arc, BTreeMap, DEFAULT_HEATMAP_TIME_BUCKETS_MAX, DEFAULT_HEATMAP_VALUE_BUCKETS, DefaultStore,
    EndMs, EngineOpts, FlameEngine, FlameGraph, FrontendConfig, HeatmapSpanExemplarsBySeries,
    InMemoryProfileStore, LabelMatcher, LabeledHeatmap, Limits, MatchOp, OverridesProvider,
    PROFILE_ID_LABEL, ProfileError, ProfileStats, ProfileStore, QueryExecution, QueryRange,
    QueryTarget, Series, SeriesAgg, ServiceMetrics, SpanExemplarsBySeries, StartMs, Time,
    bin_heatmap, heatmap_individual_exemplars_from_scan, heatmap_span_exemplars_from_scan,
    individual_exemplars_from_scan, parse_label_selector, span_exemplars_from_scan,
    span_heatmap_points_from_scan, split_inclusive_range,
};

pub struct QuerierState<S: ProfileStore = DefaultStore> {
    pub(crate) store: Arc<S>,
    pub(crate) engine: FlameEngine<S>,
    pub(crate) execution: QueryExecution,
    pub(crate) overrides: OverridesProvider,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) heatmap_value_buckets: usize,
    pub(crate) heatmap_time_buckets_max: usize,
}

impl QuerierState<DefaultStore> {
    #[must_use]
    pub fn empty() -> Self {
        Self::new(Arc::new(InMemoryProfileStore::new()))
    }
}

impl<S: ProfileStore> QuerierState<S> {
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self::new_with_limits(store, Limits::default())
    }

    #[must_use]
    pub fn new_with_limits(store: Arc<S>, limits: Limits) -> Self {
        Self::new_with_overrides(store, OverridesProvider::new(limits))
    }

    #[must_use]
    pub fn new_with_overrides(store: Arc<S>, overrides: OverridesProvider) -> Self {
        Self::from_parts(store, QueryExecution::Direct, overrides)
    }

    #[must_use]
    pub fn new_frontend(store: Arc<S>, config: FrontendConfig) -> Self {
        Self::new_frontend_with_limits(store, config, Limits::default())
    }

    #[must_use]
    pub fn new_frontend_with_limits(store: Arc<S>, config: FrontendConfig, limits: Limits) -> Self {
        Self::new_frontend_with_overrides(store, config, OverridesProvider::new(limits))
    }

    #[must_use]
    pub fn new_frontend_with_overrides(
        store: Arc<S>,
        config: FrontendConfig,
        overrides: OverridesProvider,
    ) -> Self {
        Self::from_parts(store, QueryExecution::Sharded(config), overrides)
    }

    pub(crate) fn from_parts(
        store: Arc<S>,
        execution: QueryExecution,
        overrides: OverridesProvider,
    ) -> Self {
        let engine = FlameEngine::new(Arc::clone(&store), EngineOpts::default());
        Self {
            store,
            engine,
            execution,
            overrides,
            // A self-contained default registry; the binary `main` attaches the
            // process-shared bundle (the one wired to `/metrics`) via
            // [`Self::with_metrics`] so query handlers feed the exported series.
            metrics: ServiceMetrics::new(),
            heatmap_value_buckets: DEFAULT_HEATMAP_VALUE_BUCKETS,
            heatmap_time_buckets_max: DEFAULT_HEATMAP_TIME_BUCKETS_MAX,
        }
    }

    #[must_use]
    pub fn with_heatmap_policy(mut self, value_buckets: usize, time_buckets_max: usize) -> Self {
        self.heatmap_value_buckets = value_buckets;
        self.heatmap_time_buckets_max = time_buckets_max;
        self
    }

    /// Attaches the process-shared metrics bundle so query handlers record into
    /// the exported series. The registry of this bundle backs the `/metrics`
    /// exporter. The binary `main` calls this method once after it constructs
    /// the state.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn validate_query_range(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(), ProfileError> {
        self.overrides
            .for_tenant(tenant)
            .validate_query_range_ms(StartMs(start_ms), EndMs(end_ms))
            .map_err(|err| ProfileError::Plan(err.message()))
    }

    /// Returns global profile stats for a tenant across all ingested data.
    ///
    /// Pyroscope's `GetProfileStats` is unbounded, because the request carries
    /// no time range. This method therefore queries the full time span and not
    /// a caller-supplied window. A `[0, 0]`-scoped query always reports "no
    /// data". It then traps Grafana's Profiles Drilldown on its onboarding
    /// screen even when the tenant has data.
    pub(crate) async fn global_profile_stats(
        &self,
        tenant: &str,
    ) -> Result<ProfileStats, ProfileError> {
        self.store.stats(tenant, 0, i64::MAX).await
    }

    pub(crate) fn effective_max_nodes(&self, tenant: &str, requested: i64) -> i64 {
        self.overrides
            .for_tenant(tenant)
            .effective_max_nodes(requested)
    }

    pub(crate) async fn select_merge_stacktraces(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_stack_trace_selector(
            (tenant, profile_type, label_selector),
            (start_ms, end_ms),
            max_nodes,
            &[],
        )
        .await
    }

    pub(crate) async fn select_merge_stacktraces_grouped(
        &self,
        target: QueryTarget<'_>,
        range: QueryRange,
        max_nodes: i64,
        group_by: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        if group_by.is_empty() {
            return self
                .select_merge_stacktraces(
                    tenant,
                    profile_type,
                    label_selector,
                    start_ms,
                    end_ms,
                    max_nodes,
                )
                .await;
        }
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        self.engine
            .select_merge_stacktraces_grouped(
                tenant,
                profile_type,
                label_selector,
                (start_ms, end_ms),
                max_nodes,
                group_by,
            )
            .await
    }

    pub(crate) async fn select_merge_stacktraces_with_stack_trace_selector(
        &self,
        target: QueryTarget<'_>,
        range: QueryRange,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_with_stack_trace_selector(
                        tenant,
                        profile_type,
                        label_selector,
                        (start_ms, end_ms),
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_stacktraces_with_stack_trace_selector_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn select_merge_stacktraces_tree_with_stack_trace_selector(
        &self,
        target: QueryTarget<'_>,
        range: QueryRange,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_tree_with_stack_trace_selector(
                        tenant,
                        profile_type,
                        label_selector,
                        (start_ms, end_ms),
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn select_series(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        range: QueryRange,
        stack_trace_call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_series_with_stack_trace_selector(
                        (tenant, profile_type, label_selector),
                        group_by,
                        step,
                        agg,
                        (start_ms, end_ms),
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_series_with_stack_trace_selector_sharded(
                        (tenant, profile_type, label_selector),
                        group_by,
                        step,
                        agg,
                        &shards,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn select_series_span_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        step: Time,
        range: QueryRange,
        call_sites: &[String],
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = BTreeMap::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = span_exemplars_from_scan(&scan, step, &labels, call_sites).await?;
            if !exemplars.is_empty() {
                out.insert(labels, exemplars);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_series_individual_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        step: Time,
        range: QueryRange,
        call_sites: &[String],
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let mut profile_group_by = group_by.to_vec();
        if !profile_group_by.iter().any(|name| name == PROFILE_ID_LABEL) {
            profile_group_by.push(PROFILE_ID_LABEL.to_string());
        }
        let groups = self
            .store
            .series(tenant, &base_matchers, &profile_group_by, start_ms, end_ms)
            .await?;
        let mut out: SpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = labels
                .iter()
                .find(|(name, _)| name == PROFILE_ID_LABEL)
                .map(|(_, value)| value.clone())
            else {
                continue;
            };
            let series_labels: Vec<_> = labels
                .iter()
                .filter(|(name, _)| name != PROFILE_ID_LABEL)
                .cloned()
                .collect();
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = individual_exemplars_from_scan(
                &scan,
                step,
                &series_labels,
                &profile_id,
                call_sites,
            )
            .await?;
            let points = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                points.entry(timestamp).or_default().append(&mut exemplars);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_heatmap_span_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        range: QueryRange,
        time_buckets: usize,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = BTreeMap::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars =
                heatmap_span_exemplars_from_scan(&scan, start_ms, end_ms, time_buckets, &labels)
                    .await?;
            if !exemplars.is_empty() {
                out.insert(labels, exemplars);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_heatmap_individual_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        range: QueryRange,
        time_buckets: usize,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let mut profile_group_by = group_by.to_vec();
        if !profile_group_by.iter().any(|name| name == PROFILE_ID_LABEL) {
            profile_group_by.push(PROFILE_ID_LABEL.to_string());
        }
        let groups = self
            .store
            .series(tenant, &base_matchers, &profile_group_by, start_ms, end_ms)
            .await?;
        let mut out: HeatmapSpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = labels
                .iter()
                .find(|(name, _)| name == PROFILE_ID_LABEL)
                .map(|(_, value)| value.clone())
            else {
                continue;
            };
            let series_labels: Vec<_> = labels
                .iter()
                .filter(|(name, _)| name != PROFILE_ID_LABEL)
                .cloned()
                .collect();
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = heatmap_individual_exemplars_from_scan(
                &scan,
                start_ms,
                end_ms,
                time_buckets,
                &series_labels,
                &profile_id,
            )
            .await?;
            let slots = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                slots.entry(timestamp).or_default().append(&mut exemplars);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_span_heatmaps(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        range: QueryRange,
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Vec<LabeledHeatmap>, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = span_heatmap_points_from_scan(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push(LabeledHeatmap {
                labels,
                heatmap: bin_heatmap(&points, start_ms, end_ms, time_buckets, value_buckets),
            });
        }
        Ok(out)
    }

    pub(crate) async fn select_merge_span_profile(
        &self,
        target: QueryTarget<'_>,
        span_ids: &[u64],
        range: QueryRange,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_span_profile(
                        (tenant, profile_type, label_selector),
                        span_ids,
                        (start_ms, end_ms),
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_span_profile_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        &shards,
                        max_nodes,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn select_merge_span_profile_tree(
        &self,
        target: QueryTarget<'_>,
        span_ids: &[u64],
        range: QueryRange,
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_span_profile_tree(
                        (tenant, profile_type, label_selector),
                        span_ids,
                        (start_ms, end_ms),
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_span_profile_tree_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        &shards,
                        max_nodes,
                    )
                    .await
            }
        }
    }
}
