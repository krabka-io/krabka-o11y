use super::{FrontendRangeQuery, PromqlError, QueryResult, TenantId, async_trait};

/// Executes one planned range subquery.
#[async_trait]
pub trait RangeQueryExecutor: Send + Sync {
    async fn execute_range_query(
        &self,
        tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError>;
}
