use super::{
    Arc, BlockIndex, ColdObjectStoreState, DynamicIndexCache, DynamicIndexSource, ExecutionOptions,
    HotTailState, InMemoryCache, LabelIndex, Limits, LogQueryAuthorizer, NonZeroUsize,
    OverridesProvider, PathBuf, ServiceMetrics, SharedLogDeleteRequests, SharedLokiRules,
    SharedPrometheusAlertStates, Value,
};

#[derive(Clone)]
pub struct QuerierState {
    pub(crate) root: PathBuf,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
    pub(crate) cold_store: Option<ColdObjectStoreState>,
    pub(crate) dynamic_index: Option<DynamicIndexSource>,
    pub(crate) dynamic_index_cache: DynamicIndexCache,
    pub(crate) cold_block_fetch_concurrency: NonZeroUsize,
    pub(crate) hot_tail: Option<HotTailState>,
    pub(crate) delete_requests: Option<SharedLogDeleteRequests>,
    pub(crate) rules: SharedLokiRules,
    pub(crate) alert_states: SharedPrometheusAlertStates,
    pub(crate) query_authorizer: Arc<dyn LogQueryAuthorizer>,
    /// The one provider this service resolves every tenant's limits through.
    ///
    /// It is shared with the distributor, so a read gate and an ingest gate
    /// answer the same tenant with the same numbers.
    pub(crate) overrides: Arc<OverridesProvider>,
    /// The limits of the tenant the request in hand names.
    ///
    /// [`QuerierState::with_tenant_limits`] resolves them once, at the top of
    /// each per-tenant read path, and every check below reads them from here.
    /// Before that resolution they are the provider's defaults.
    pub(crate) limits: Limits,
    /// Shared RED-metrics bundle. It is `None` for test routers that do not
    /// wire metrics. The binary threads a shared bundle in with
    /// [`QuerierState::with_metrics`].
    pub(crate) metrics: Option<ServiceMetrics>,
    pub(crate) query_frontend_cache: Arc<InMemoryCache<Value>>,
    pub(crate) query_frontend_options: ExecutionOptions,
    pub(crate) query_frontend_split_ns: i64,
    pub(crate) query_frontend_target_bytes: u64,
}
