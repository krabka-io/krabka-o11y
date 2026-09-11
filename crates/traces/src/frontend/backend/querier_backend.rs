use super::{
    BackendError, MetricsJobRequest, MetricsPartial, SearchJobRequest, SearchPartial,
    TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
    TraceByIdJobRequest, TracePartial, async_trait,
};

/// A transport that runs one assigned job against one querier.
///
/// Every method is one fanned-out job's worth of work, and every request
/// carries the `host:port` it is for. The backend chooses nothing: which
/// queriers exist and which of them may take work is
/// [`MembershipView`](crate::frontend::membership::MembershipView)'s answer,
/// and where a shard goes is [`assign_jobs`](crate::frontend::assign_jobs)'.
/// A backend that picked its own target would be picking from a list nobody
/// was keeping current, which is what it used to do.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
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
