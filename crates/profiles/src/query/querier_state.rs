use krabka_pprof::{MillisRange, ProfileSelection, SpanProfileShards};
use krabka_units::convert::TimeExt as _;

use super::{
    Arc, BTreeMap, DEFAULT_HEATMAP_TIME_BUCKETS_MAX, DEFAULT_HEATMAP_VALUE_BUCKETS, DefaultStore,
    EndMs, EngineOpts, FlameEngine, FlameGraph, FrontendConfig, HeatmapSlotsMillis,
    HeatmapSpanExemplarsBySeries, InMemoryProfileStore, IndividualProfile, LabelMatcher, Limits,
    MatchOp, OverridesProvider, PROFILE_ID_LABEL, ProfileError, ProfileStats, ProfileStore,
    QueryExecution, QueryRange, QueryTarget, SampleSelector, Series, SeriesAgg,
    SeriesExemplarQuery, ServiceMetrics, SpanExemplarsBySeries, SpanHeatmapRequest, StartMs,
    TenantId, TenantPolicy, Time, heatmap_individual_exemplars_from_scan,
    heatmap_span_exemplars_from_scan, individual_exemplars_from_scan, parse_label_selector,
    span_exemplars_from_scan, span_heatmap_points_from_scan, split_inclusive_range,
};

pub struct QuerierState<S: ProfileStore = DefaultStore> {
    pub(crate) query_architecture: super::PyroscopeQueryArchitecture,
    pub(crate) async_queries_enabled: bool,
    pub(crate) query_analysis_series_enabled: bool,
    pub(crate) async_runtime: super::async_stacktrace_query::Runtime,
    pub(crate) store: Arc<S>,
    pub(crate) async_query_slots:
        tokio::sync::Mutex<BTreeMap<String, std::collections::BTreeSet<String>>>,
    pub(crate) engine: FlameEngine<S>,
    pub(crate) execution: QueryExecution,
    pub(crate) overrides: OverridesProvider,
    /// What a query that names no tenant resolves to. Every querier handler
    /// resolves the `X-Scope-OrgID` header under this one value.
    pub(crate) tenant_policy: TenantPolicy,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) admin_store: Arc<dyn object_store::ObjectStore>,
    pub(crate) heatmap_value_buckets: usize,
    pub(crate) heatmap_time_buckets_max: usize,
}

/// A validated series exemplar query: its parsed selector, and where its
/// scan starts once the one-step lookback before the range is included.
struct SeriesExemplarScan {
    base_matchers: Vec<LabelMatcher>,
    scan_start: i64,
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
        let admission_overrides = overrides.clone();
        let engine = FlameEngine::new(Arc::clone(&store), EngineOpts::default())
            .with_admission_limits(move |tenant| {
                tenant.parse().ok().map_or_else(Default::default, |tenant| {
                    admission_overrides.for_tenant(&tenant).query_admission
                })
            });
        Self {
            query_architecture: super::PyroscopeQueryArchitecture::default(),
            async_queries_enabled: false,
            query_analysis_series_enabled: false,
            async_runtime: super::async_stacktrace_query::Runtime::default(),
            store,
            async_query_slots: tokio::sync::Mutex::new(BTreeMap::new()),
            engine,
            execution,
            overrides,
            // Pyroscope keeps multi-tenancy off by default and serves a query
            // without a tenant from the `anonymous` tenant.
            tenant_policy: TenantPolicy::anonymous(),
            // A self-contained default registry; the binary `main` attaches the
            // process-shared bundle (the one wired to `/metrics`) via
            // [`Self::with_metrics`] so query handlers feed the exported series.
            metrics: ServiceMetrics::new(),
            admin_store: Arc::new(object_store::memory::InMemory::new()),
            heatmap_value_buckets: DEFAULT_HEATMAP_VALUE_BUCKETS,
            heatmap_time_buckets_max: DEFAULT_HEATMAP_TIME_BUCKETS_MAX,
        }
    }

    /// Binds query behavior to Pyroscope's v1 ingester or v2 segment-writer API.
    #[must_use]
    pub fn with_query_architecture(
        mut self,
        architecture: super::PyroscopeQueryArchitecture,
    ) -> Self {
        self.query_architecture = architecture;
        self
    }

    /// Enables the experimental v2 asynchronous query frontend.
    #[must_use]
    pub fn with_async_queries_enabled(mut self, enabled: bool) -> Self {
        self.async_queries_enabled = enabled;
        self
    }

    /// Enables selector-matching series counts in v1 query analysis.
    /// Physical component costs are reported regardless of this setting.
    #[must_use]
    pub fn with_query_analysis_series_enabled(mut self, enabled: bool) -> Self {
        self.query_analysis_series_enabled = enabled;
        self
    }

    /// Overrides maintenance intervals for durable asynchronous query records.
    ///
    /// # Errors
    /// Rejects zero intervals and leases shorter than a heartbeat.
    pub fn with_async_query_policy(
        mut self,
        policy: super::AsyncQueryPolicy,
    ) -> Result<Self, super::ConnectError> {
        if policy.heartbeat_interval.is_zero()
            || policy.adoption_interval.is_zero()
            || policy.cleanup_interval.is_zero()
            || policy.retention.is_zero()
            || policy.lease_timeout < policy.heartbeat_interval
        {
            return Err(super::ConnectError::new(
                super::Code::InvalidArgument,
                "invalid async maintenance intervals",
            ));
        }
        self.async_runtime.policy = policy;
        Ok(self)
    }

    /// Cancels maintenance and workers, awaiting lease relinquishment.
    pub async fn shutdown_async_queries(&self) {
        super::async_stacktrace_query::shutdown(self).await;
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
        if let Ok(mut registry) = metrics.registry.try_lock() {
            self.engine.cache_metrics().register(&mut registry);
        } else {
            tracing::error!("query cache metrics registry is busy during setup");
        }
        self.metrics = metrics;
        self
    }

    /// Stores tenant settings, ad hoc profiles, recording rules and debug info.
    #[must_use]
    pub fn with_admin_store(mut self, store: Arc<dyn object_store::ObjectStore>) -> Self {
        self.admin_store = store;
        self
    }

    pub(crate) fn validate_query_range(
        &self,
        tenant: &TenantId,
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
        tenant: &TenantId,
    ) -> Result<ProfileStats, ProfileError> {
        self.store.stats(tenant.as_str(), 0, i64::MAX).await
    }

    pub(crate) fn effective_max_nodes(&self, tenant: &TenantId, requested: i64) -> i64 {
        self.overrides
            .for_tenant(tenant)
            .effective_max_nodes(requested)
    }

    pub(crate) async fn select_merge_stacktraces(
        &self,
        tenant: &TenantId,
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
                tenant.as_str(),
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
        self.select_merge_stacktraces_with_selectors(
            target,
            range,
            max_nodes,
            stack_trace_call_sites,
            SampleSelector::None,
        )
        .await
    }

    pub(crate) async fn select_merge_stacktraces_with_selectors(
        &self,
        target: QueryTarget<'_>,
        range: QueryRange,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_with_selectors(
                        (tenant.as_str(), profile_type, label_selector),
                        (start_ms, end_ms),
                        max_nodes,
                        stack_trace_call_sites,
                        sample_selector,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_stacktraces_with_selectors_sharded(
                        (tenant.as_str(), profile_type, label_selector),
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                        sample_selector,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn select_merge_stacktraces_tree_with_selectors(
        &self,
        target: QueryTarget<'_>,
        range: QueryRange,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let (start_ms, end_ms) = range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_tree_with_selectors(
                        (tenant.as_str(), profile_type, label_selector),
                        (start_ms, end_ms),
                        max_nodes,
                        stack_trace_call_sites,
                        sample_selector,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_stacktraces_tree_with_selectors_sharded(
                        (tenant.as_str(), profile_type, label_selector),
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                        sample_selector,
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
                        (tenant.as_str(), profile_type, label_selector),
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
                        (tenant.as_str(), profile_type, label_selector),
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

    /// Validates a series exemplar query and parses its selector.
    fn series_exemplar_scan(
        &self,
        query: SeriesExemplarQuery<'_>,
    ) -> Result<SeriesExemplarScan, ProfileError> {
        let (tenant, _, label_selector) = query.target;
        let (start_ms, end_ms) = query.range;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        Ok(SeriesExemplarScan {
            base_matchers: parse_label_selector(label_selector)?,
            // The first point covers the one-step lookback before `start_ms`.
            scan_start: start_ms.saturating_sub(query.step.millis_i64()),
        })
    }

    pub(crate) async fn select_series_span_exemplars(
        &self,
        query: SeriesExemplarQuery<'_>,
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        let SeriesExemplarScan {
            base_matchers,
            scan_start,
        } = self.series_exemplar_scan(query)?;
        let SeriesExemplarQuery {
            target: (tenant, profile_type, _),
            group_by,
            step,
            range,
            call_sites,
        } = query;
        let end_ms = range.1;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(
                    tenant.as_str(),
                    &base_matchers,
                    group_by,
                    scan_start,
                    end_ms,
                )
                .await?
        };
        let mut out = BTreeMap::new();
        for labels in groups {
            let matchers = series_matchers(&base_matchers, &labels);
            let scan = self
                .store
                .select(tenant.as_str(), profile_type, &matchers, scan_start, end_ms)
                .await?;
            let exemplars =
                span_exemplars_from_scan(&scan, krabka_units::millis(1), &labels, call_sites)
                    .await?;
            let mut buckets = BTreeMap::<i64, Vec<_>>::new();
            for (timestamp, mut exemplars) in exemplars {
                if let Some(endpoint) = krabka_pprof::series_bucket_ms(timestamp, step, range) {
                    buckets.entry(endpoint).or_default().append(&mut exemplars);
                }
            }
            if !buckets.is_empty() {
                out.insert(labels, buckets);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_series_individual_exemplars(
        &self,
        query: SeriesExemplarQuery<'_>,
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        let SeriesExemplarScan {
            base_matchers,
            scan_start,
        } = self.series_exemplar_scan(query)?;
        let SeriesExemplarQuery {
            target: (tenant, profile_type, _),
            group_by,
            step,
            range,
            call_sites,
        } = query;
        let end_ms = range.1;
        let groups = self
            .store
            .series(tenant.as_str(), &base_matchers, &[], scan_start, end_ms)
            .await?;
        let mut out: SpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = profile_id_of(&labels) else {
                continue;
            };
            let series_labels = labels_grouped_by(&labels, group_by);
            let exemplar_labels = labels
                .iter()
                .filter(|(name, _)| name != PROFILE_ID_LABEL)
                .cloned()
                .collect::<Vec<_>>();
            let matchers = series_matchers(&base_matchers, &labels);
            let scan = self
                .store
                .select(tenant.as_str(), profile_type, &matchers, scan_start, end_ms)
                .await?;
            let exemplars = individual_exemplars_from_scan(
                &scan,
                krabka_units::millis(1),
                &exemplar_labels,
                &profile_id,
                call_sites,
            )
            .await?;
            let points = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                if let Some(endpoint) = krabka_pprof::series_bucket_ms(timestamp, step, range) {
                    points.entry(endpoint).or_default().append(&mut exemplars);
                }
            }
        }
        Ok(out)
    }

    /// Validates a heatmap exemplar query and lists every series its
    /// selector matches, with the selector's matchers.
    async fn heatmap_exemplar_series(
        &self,
        query: HeatmapExemplarQuery<'_>,
    ) -> Result<(Vec<LabelMatcher>, Vec<Vec<(String, String)>>), ProfileError> {
        let HeatmapExemplarQuery {
            tenant,
            label_selector,
            start_ms,
            end_ms,
        } = query;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = self
            .store
            .series(tenant.as_str(), &base_matchers, &[], start_ms, end_ms)
            .await?;
        Ok((base_matchers, groups))
    }

    pub(crate) async fn select_heatmap_span_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        slots: HeatmapSlotsMillis,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let HeatmapSlotsMillis {
            start: start_ms,
            end: end_ms,
            ..
        } = slots;
        let (base_matchers, groups) = self
            .heatmap_exemplar_series(HeatmapExemplarQuery {
                tenant,
                label_selector,
                start_ms,
                end_ms,
            })
            .await?;
        let mut out = BTreeMap::new();
        for labels in groups {
            let matchers = series_matchers(&base_matchers, &labels);
            let scan = self
                .store
                .select(tenant.as_str(), profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplar_labels = heatmap_exemplar_labels(&labels, group_by);
            let series_labels = labels_grouped_by(&labels, group_by);
            let exemplars =
                heatmap_span_exemplars_from_scan(&scan, slots, &exemplar_labels).await?;
            if !exemplars.is_empty() {
                let slots = out.entry(series_labels).or_insert_with(BTreeMap::new);
                for (timestamp, mut exemplars) in exemplars {
                    slots
                        .entry(timestamp)
                        .or_insert_with(Vec::new)
                        .append(&mut exemplars);
                }
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_heatmap_individual_exemplars(
        &self,
        target: QueryTarget<'_>,
        group_by: &[String],
        slots: HeatmapSlotsMillis,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        let (tenant, profile_type, label_selector) = target;
        let HeatmapSlotsMillis {
            start: start_ms,
            end: end_ms,
            ..
        } = slots;
        let (base_matchers, groups) = self
            .heatmap_exemplar_series(HeatmapExemplarQuery {
                tenant,
                label_selector,
                start_ms,
                end_ms,
            })
            .await?;
        let mut out: HeatmapSpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = profile_id_of(&labels) else {
                continue;
            };
            let series_labels = labels_grouped_by(&labels, group_by);
            let exemplar_labels = heatmap_exemplar_labels(&labels, group_by);
            let matchers = series_matchers(&base_matchers, &labels);
            let scan = self
                .store
                .select(tenant.as_str(), profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = heatmap_individual_exemplars_from_scan(
                &scan,
                slots,
                IndividualProfile {
                    profile_id: &profile_id,
                    labels: &exemplar_labels,
                },
            )
            .await?;
            let slots = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                slots.entry(timestamp).or_default().append(&mut exemplars);
            }
        }
        Ok(out)
    }

    pub(crate) async fn select_span_heatmap_points(
        &self,
        request: SpanHeatmapRequest<'_>,
    ) -> Result<Vec<krabka_pprof::LabeledHeatmapPoints>, ProfileError> {
        let SpanHeatmapRequest {
            tenant,
            profile_type,
            label_selector,
            group_by,
            range: MillisRange { start_ms, end_ms },
        } = request;
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant.as_str(), &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = Vec::new();
        for labels in groups {
            let matchers = series_matchers(&base_matchers, &labels);
            let scan = self
                .store
                .select(tenant.as_str(), profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = span_heatmap_points_from_scan(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push((labels, points));
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
                        (tenant.as_str(), profile_type, label_selector),
                        span_ids,
                        (start_ms, end_ms),
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_span_profile_sharded(SpanProfileShards {
                        selection: ProfileSelection {
                            tenant: tenant.as_str(),
                            profile_type,
                            label_selector,
                        },
                        span_selector: span_ids,
                        ranges: &shards,
                        max_nodes,
                    })
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
                        (tenant.as_str(), profile_type, label_selector),
                        span_ids,
                        (start_ms, end_ms),
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width)?;
                self.engine
                    .select_merge_span_profile_tree_sharded(SpanProfileShards {
                        selection: ProfileSelection {
                            tenant: tenant.as_str(),
                            profile_type,
                            label_selector,
                        },
                        span_selector: span_ids,
                        ranges: &shards,
                        max_nodes,
                    })
                    .await
            }
        }
    }
}

/// The selector's matchers narrowed to the one series that `labels` names.
/// The series a heatmap exemplar query reads: those of `tenant` that
/// `label_selector` matches in `[start_ms, end_ms]`.
#[derive(Clone, Copy)]
struct HeatmapExemplarQuery<'a> {
    tenant: &'a TenantId,
    label_selector: &'a str,
    start_ms: i64,
    end_ms: i64,
}

fn series_matchers(base: &[LabelMatcher], labels: &[(String, String)]) -> Vec<LabelMatcher> {
    let mut matchers = base.to_vec();
    matchers.extend(
        labels
            .iter()
            .map(|(name, value)| LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())),
    );
    matchers
}

fn profile_id_of(labels: &[(String, String)]) -> Option<String> {
    labels
        .iter()
        .find(|(name, _)| name == PROFILE_ID_LABEL)
        .map(|(_, value)| value.clone())
}

fn labels_grouped_by(labels: &[(String, String)], group_by: &[String]) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(name, _)| group_by.contains(name))
        .cloned()
        .collect()
}

/// The labels a heatmap exemplar carries: those its series is not grouped
/// by, less the profile id.
fn heatmap_exemplar_labels(
    labels: &[(String, String)],
    group_by: &[String],
) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(name, _)| name != PROFILE_ID_LABEL && !group_by.contains(name))
        .cloned()
        .collect()
}
