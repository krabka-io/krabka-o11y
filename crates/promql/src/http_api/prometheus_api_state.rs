use super::{
    ActiveQueryGuard, AlertStateKey, Arc, BTreeMap, ByteSize, EngineOpts, MetricStore,
    OverridesProvider, PromqlEngine, QueryFrontendCache, QueryFrontendOptions, QueryFrontendState,
    RangeQueryCache, RulerAlertStateRecord, RulerAlertStateStore, RulerGroupState,
    RulerGroupStateRecord, RulerRuleStore, RwLock, Semaphore, ServiceMetrics, SystemTime, Time,
    mebibytes,
};

/// Shared state for the Prometheus HTTP query API.
pub struct PrometheusApiState<S: MetricStore> {
    pub(crate) engine: PromqlEngine<S>,
    pub(crate) engine_opts: EngineOpts,
    pub(crate) store: Arc<S>,
    pub(crate) ruler_rules: RwLock<RulerRuleStore>,
    pub(crate) ruler_alerts: RwLock<RulerAlertStateStore>,
    pub(crate) ruler_group_state: RwLock<RulerGroupState>,
    pub(crate) ruler_evaluation_time_ms: RwLock<i64>,
    pub(crate) query_frontend: Option<QueryFrontendState>,
    pub(crate) query_limits: Option<OverridesProvider>,
    pub(crate) query_gate: Option<Arc<Semaphore>>,
    pub(crate) max_concurrent_queries: usize,
    pub(crate) remote_read_max_body: ByteSize,
    pub(crate) metrics: Option<ServiceMetrics>,
    pub(crate) start_time: SystemTime,
}

impl<S: MetricStore> PrometheusApiState<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self {
            engine: PromqlEngine::new(Arc::clone(&store), opts),
            engine_opts: opts,
            store,
            ruler_rules: RwLock::new(BTreeMap::new()),
            ruler_alerts: RwLock::new(BTreeMap::new()),
            ruler_group_state: RwLock::new(RulerGroupState::default()),
            ruler_evaluation_time_ms: RwLock::new(0),
            query_frontend: None,
            query_limits: None,
            query_gate: None,
            max_concurrent_queries: 0,
            remote_read_max_body: mebibytes(64),
            metrics: None,
            start_time: SystemTime::now(),
        }
    }

    #[must_use]
    pub fn with_query_limits(mut self, limits: OverridesProvider) -> Self {
        self.query_limits = Some(limits);
        self
    }

    #[must_use]
    pub fn with_max_concurrent_queries(mut self, max_concurrent_queries: usize) -> Self {
        let max_concurrent_queries = max_concurrent_queries.max(1);
        self.query_gate = Some(Arc::new(Semaphore::new(max_concurrent_queries)));
        self.max_concurrent_queries = max_concurrent_queries;
        self
    }

    /// Sets the compressed and decompressed body cap for remote reads.
    #[must_use]
    pub fn with_remote_read_max_body(mut self, max_body: ByteSize) -> Self {
        self.remote_read_max_body = max_body;
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Records one query request outcome on `route`.
    ///
    /// This method does nothing when no metrics bundle is configured.
    pub(crate) fn record_query(&self, route: &str, ok: bool, latency: Time) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(route, ok, latency);
        }
    }

    /// Records one `PromQL` engine evaluation and its latency.
    ///
    /// `query_type` is `"instant"` or `"range"`. When `ok` is false, this method
    /// also increments the error count. The method does nothing when no metrics
    /// bundle is configured.
    pub(crate) fn record_eval(&self, query_type: &str, ok: bool, latency: Time) {
        if let Some(metrics) = &self.metrics {
            metrics.record_eval(query_type, ok, latency);
        }
    }

    /// Increments the in-flight-query gauge for the lifetime of the returned guard.
    ///
    /// The guard decrements the gauge on drop. The guard does nothing when no
    /// metrics bundle is configured.
    pub(crate) fn active_query_guard(&self) -> ActiveQueryGuard {
        if let Some(metrics) = &self.metrics {
            metrics.query_started();
            ActiveQueryGuard {
                metrics: Some(metrics.clone()),
            }
        } else {
            ActiveQueryGuard { metrics: None }
        }
    }

    #[must_use]
    pub fn with_query_frontend(mut self, opts: QueryFrontendOptions) -> Self {
        self.query_frontend = Some(QueryFrontendState {
            opts,
            cache: Arc::new(QueryFrontendCache::default()),
        });
        self
    }

    #[must_use]
    pub fn with_query_frontend_cache(
        mut self,
        opts: QueryFrontendOptions,
        cache: Arc<dyn RangeQueryCache>,
    ) -> Self {
        self.query_frontend = Some(QueryFrontendState { opts, cache });
        self
    }

    /// Returns the `PromQL` engine that backs this HTTP API state.
    #[must_use]
    pub fn engine(&self) -> &PromqlEngine<S> {
        &self.engine
    }

    #[must_use]
    pub fn engine_for_tenant(&self, tenant: &str) -> PromqlEngine<S> {
        let mut opts = self.engine_opts;
        if let Some(limits) = &self.query_limits {
            let max_samples = limits.for_tenant(tenant).max_samples_per_query;
            if max_samples != 0 {
                opts.max_samples = usize::try_from(max_samples).unwrap_or(usize::MAX);
            }
        }
        PromqlEngine::new(Arc::clone(&self.store), opts)
    }

    /// Returns a snapshot of the ruler rules for one tenant.
    #[must_use]
    pub fn ruler_rule_set(
        &self,
        tenant: &str,
    ) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
        self.ruler_rules
            .read()
            .ok()
            .and_then(|rules| rules.get(tenant).cloned())
            .unwrap_or_default()
    }

    /// Applies replayed ruler group state for HTTP rule rendering.
    pub fn apply_ruler_group_state(&self, record: RulerGroupStateRecord) {
        if let Ok(mut group_state) = self.ruler_group_state.write() {
            group_state.apply_record(record);
        }
    }

    /// Applies replayed ruler alert state for HTTP alert rendering.
    pub fn apply_ruler_alert_state(&self, record: RulerAlertStateRecord) {
        if let Ok(mut alert_states) = self.ruler_alerts.write() {
            let key = AlertStateKey {
                tenant: record.tenant,
                rule_id: record.rule_id,
                labels: record.labels,
            };
            match record.active_since_ms {
                Some(active_since_ms) => {
                    alert_states.insert(key, active_since_ms);
                }
                None => {
                    alert_states.remove(&key);
                }
            }
        }
    }

    /// Sets the timestamp for the ruler evaluations that the HTTP API renders.
    ///
    /// A production ruler loop advances this timestamp from its injected clock.
    /// Tests set it to exercise `for:` alert state transitions deterministically.
    pub fn set_ruler_evaluation_time_ms(&self, time_ms: i64) {
        if let Ok(mut eval_time) = self.ruler_evaluation_time_ms.write() {
            *eval_time = time_ms;
        }
    }

    pub(crate) fn ruler_evaluation_time_ms(&self) -> i64 {
        self.ruler_evaluation_time_ms
            .read()
            .map_or(0, |eval_time| *eval_time)
    }

    pub(crate) fn ruler_group_last_eval_ms(
        &self,
        tenant: &str,
        namespace: &str,
        group: &str,
    ) -> Option<i64> {
        self.ruler_group_state
            .read()
            .ok()
            .and_then(|group_state| group_state.last_eval_ms(tenant, namespace, group))
    }
}
