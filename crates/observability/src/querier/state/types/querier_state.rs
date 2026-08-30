use super::{
    Arc, BlockIndex, ByteSize, ColdObjectStoreState, DynamicIndexCache, DynamicIndexSource,
    HotTailState, LabelIndex, LogQueryAuthorizer, NonZeroUsize, PathBuf, ServiceMetrics,
    SharedLogDeleteRequests, SharedLokiRules, SharedPrometheusAlertStates, Time,
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
    pub(crate) max_query_range: Option<Time>,
    /// A count of series, not a data volume, so it stays a plain integer.
    pub(crate) max_query_series: Option<usize>,
    pub(crate) max_query_read: Option<ByteSize>,
    pub(crate) max_query_length: Option<ByteSize>,
    /// Shared RED-metrics bundle. It is `None` for test routers that do not
    /// wire metrics. The binary threads a shared bundle in with
    /// [`QuerierState::with_metrics`].
    pub(crate) metrics: Option<ServiceMetrics>,
}
