use super::{AnnotatedQueryResult, FrontendRangeQuery, PromqlError, TenantId, async_trait};

/// Executes one planned range subquery and reports its annotations.
///
/// An implementor returns the warnings and infos that the subquery raised with
/// its result. The frontend merges the annotations of every subquery into the
/// annotations of the whole range query, so a split or sharded query reports
/// what an unsplit query reports.
#[async_trait]
pub trait RangeQueryExecutor: Send + Sync {
    async fn execute_range_query(
        &self,
        tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError>;

    /// Reports whether a structured execution error can succeed on a retry.
    fn is_transient_error(&self, _error: &PromqlError) -> bool {
        false
    }
}
