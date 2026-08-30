use super::{BackendError, MetricsJobRequest, MetricsPartial, SearchJobRequest, SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial, TraceByIdJobRequest, TracePartial, async_trait};

/// A queryable querier backend, a pool that fronts N queriers.
///
/// Every method is one fanned-out job's worth of work.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    /// Number of queriers in the pool, which is the by-id fan-out width.
    fn querier_count(&self) -> usize;

    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError>;
    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError>;
    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError>;
    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError>;
    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError>;
}
