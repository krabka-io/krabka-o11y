use super::{FrontendRangeQuery, PromqlError, QueryResult, async_trait};

/// Executes one planned range subquery.
#[async_trait]
pub trait RangeQueryExecutor: Send + Sync {
    async fn execute_range_query(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError>;
}
