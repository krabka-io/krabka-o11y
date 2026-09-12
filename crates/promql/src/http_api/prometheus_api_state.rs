use super::{
    ActiveQueryGuard, AlertStateKey, Arc, AuditHandle, BTreeMap, ByteSize, EngineOpts, Limits,
    MetricStore, OverridesProvider, PromqlEngine, QueryFrontendCache, QueryFrontendOptions,
    QueryFrontendState, RangeQueryCache, ReadinessGate, RulerAlertStateRecord,
    RulerAlertStateStore, RulerGroupState, RulerGroupStateRecord, RulerRuleStore, RwLock,
    Semaphore, ServiceMetrics, SystemTime, TenantId, Time, WalHead, mebibytes, minutes,
};
use crate::{RulerEvaluationReport, RulerGroupEvaluationStatus, RulerRuleEvaluationStatus};

/// Shared state for the Prometheus HTTP query API.
pub struct PrometheusApiState<S: MetricStore> {
    pub(crate) engine: PromqlEngine<S>,
    pub(crate) engine_opts: EngineOpts,
    pub(crate) store: Arc<S>,
    pub(crate) ruler_rules: RwLock<RulerRuleStore>,
    pub(crate) ruler_alerts: RwLock<RulerAlertStateStore>,
    pub(crate) ruler_group_state: RwLock<RulerGroupState>,
    pub(crate) ruler_evaluation_time_ms: RwLock<i64>,
    pub(crate) ruler_rule_status:
        RwLock<BTreeMap<(String, String, String, usize), RulerRuleEvaluationStatus>>,
    pub(crate) ruler_group_status:
        RwLock<BTreeMap<(String, String, String), RulerGroupEvaluationStatus>>,
    pub(crate) query_frontend: Option<QueryFrontendState>,
    /// The per-tenant query limits. A state built without
    /// [`PrometheusApiState::with_query_limits`] applies `Limits::default()` to
    /// every tenant, as Mimir applies its defaults without a runtime config.
    pub(crate) query_limits: OverridesProvider,
    pub(crate) query_gate: Option<Arc<Semaphore>>,
    pub(crate) max_concurrent_queries: usize,
    pub(crate) query_timeout: Time,
    pub(crate) remote_read_max_body: ByteSize,
    pub(crate) metrics: Option<ServiceMetrics>,
    pub(crate) audit: AuditHandle,
    pub(crate) start_time: SystemTime,
    pub(crate) status_log_level: String,
    pub(crate) storage_retention: Option<Time>,
    pub(crate) wal_head: Option<WalHead>,
    pub(crate) wal_head_readiness: Option<ReadinessGate>,
    pub(crate) erasure_store: Option<Arc<dyn object_store::ObjectStore>>,
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
            ruler_rule_status: RwLock::new(BTreeMap::new()),
            ruler_group_status: RwLock::new(BTreeMap::new()),
            query_frontend: None,
            query_limits: OverridesProvider::new(Limits::default()),
            query_gate: None,
            max_concurrent_queries: 0,
            query_timeout: minutes(2),
            remote_read_max_body: mebibytes(64),
            metrics: None,
            audit: AuditHandle::disabled(),
            start_time: SystemTime::now(),
            status_log_level: "unknown (not configured)".to_string(),
            storage_retention: None,
            wal_head: None,
            wal_head_readiness: None,
            erasure_store: None,
        }
    }

    /// Records every ruler config mutation through `audit`.
    ///
    /// Without this call the state holds a disabled handle, and the ruler
    /// config API records nothing, as a service with no `--audit-topic` does.
    #[must_use]
    pub fn with_audit(mut self, audit: AuditHandle) -> Self {
        self.audit = audit;
        self
    }

    /// Resolves each tenant's query limits through `limits`, in place of
    /// `Limits::default()`.
    #[must_use]
    pub fn with_query_limits(mut self, limits: OverridesProvider) -> Self {
        self.query_limits = limits;
        self
    }

    #[must_use]
    pub fn with_max_concurrent_queries(mut self, max_concurrent_queries: usize) -> Self {
        let max_concurrent_queries = max_concurrent_queries.max(1);
        self.query_gate = Some(Arc::new(Semaphore::new(max_concurrent_queries)));
        self.max_concurrent_queries = max_concurrent_queries;
        self
    }

    /// Sets the process-wide default and upper bound for HTTP query deadlines.
    #[must_use]
    pub fn with_query_timeout(mut self, query_timeout: Time) -> Self {
        self.query_timeout = query_timeout;
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

    /// Supplies process values rendered by the Prometheus status endpoints.
    #[must_use]
    pub fn with_runtime_status(
        mut self,
        log_level: impl Into<String>,
        storage_retention: Option<Time>,
    ) -> Self {
        self.status_log_level = log_level.into();
        self.storage_retention = storage_retention;
        self
    }

    /// Exposes the live WAL-head retention and materialized offsets.
    #[must_use]
    pub fn with_wal_head_status(mut self, head: WalHead, readiness: ReadinessGate) -> Self {
        self.storage_retention = Some(head.retention());
        self.wal_head = Some(head);
        self.wal_head_readiness = Some(readiness);
        self
    }

    /// Enables the Prometheus TSDB erasure endpoints on this API state.
    #[must_use]
    pub fn with_erasure_store(mut self, store: Arc<dyn object_store::ObjectStore>) -> Self {
        self.erasure_store = Some(store);
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
    pub fn engine_for_tenant(&self, tenant: &TenantId) -> PromqlEngine<S> {
        let mut opts = self.engine_opts;
        let tenant_limits = self.query_limits.for_tenant(tenant.as_str());
        // The engine options are the process caps, as Mimir's
        // `-querier.max-samples` is. A tenant's cap can lower a process cap and
        // cannot raise it. A tenant cap of zero leaves the process cap alone.
        // The engine has no value that turns its sample cap off, because a zero
        // cap would refuse the first row.
        let max_samples = tenant_limits.max_samples_per_query;
        if max_samples != 0 {
            opts.max_samples = opts
                .max_samples
                .min(usize::try_from(max_samples).unwrap_or(usize::MAX));
        }
        // Zero is "no series cap" at both levels, so the lower non-zero cap wins.
        let tenant_series =
            usize::try_from(tenant_limits.max_fetched_series_per_query).unwrap_or(usize::MAX);
        opts.max_fetched_series = match (opts.max_fetched_series, tenant_series) {
            (0, cap) | (cap, 0) => cap,
            (process, tenant) => process.min(tenant),
        };
        PromqlEngine::new(Arc::clone(&self.store), opts)
    }

    /// Returns a snapshot of the ruler rules for one tenant.
    #[must_use]
    pub fn ruler_rule_set(
        &self,
        tenant: &TenantId,
    ) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
        self.ruler_rules
            .read()
            .ok()
            .and_then(|rules| rules.get(tenant).cloned())
            .unwrap_or_default()
    }

    /// Returns the tenants that currently have ruler configuration.
    #[must_use]
    pub fn ruler_tenants(&self) -> Vec<TenantId> {
        self.ruler_rules
            .read()
            .map_or_else(|_| Vec::new(), |rules| rules.keys().cloned().collect())
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

    /// Publishes one completed ruler pass to the HTTP status view and metrics.
    pub fn apply_ruler_evaluation_report(&self, report: &RulerEvaluationReport) {
        if let Ok(mut statuses) = self.ruler_rule_status.write() {
            for status in &report.rules {
                if let Some(metrics) = &self.metrics {
                    metrics.record_ruler_rule(status.last_error.is_empty());
                }
                statuses.insert(
                    (
                        status.tenant.clone(),
                        status.namespace.clone(),
                        status.group.clone(),
                        status.rule_index,
                    ),
                    status.clone(),
                );
            }
        }
        if let Ok(mut statuses) = self.ruler_group_status.write() {
            for status in &report.groups {
                if let Some(metrics) = &self.metrics {
                    metrics.record_ruler_group(status.evaluation_time_seconds);
                }
                statuses.insert(
                    (
                        status.tenant.clone(),
                        status.namespace.clone(),
                        status.group.clone(),
                    ),
                    status.clone(),
                );
            }
        }
    }

    pub(crate) fn ruler_rule_status(
        &self,
        tenant: &TenantId,
        namespace: &str,
        group: &str,
        rule_index: usize,
    ) -> Option<RulerRuleEvaluationStatus> {
        self.ruler_rule_status.read().ok().and_then(|statuses| {
            statuses
                .get(&(
                    tenant.to_string(),
                    namespace.into(),
                    group.into(),
                    rule_index,
                ))
                .cloned()
        })
    }

    pub(crate) fn ruler_group_status(
        &self,
        tenant: &TenantId,
        namespace: &str,
        group: &str,
    ) -> Option<RulerGroupEvaluationStatus> {
        self.ruler_group_status.read().ok().and_then(|statuses| {
            statuses
                .get(&(tenant.to_string(), namespace.into(), group.into()))
                .cloned()
        })
    }
}
