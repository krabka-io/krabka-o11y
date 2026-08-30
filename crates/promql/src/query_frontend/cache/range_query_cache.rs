use super::{FrontendRangeQuery, PromqlError, QueryResult, async_trait};

#[async_trait]
pub trait RangeQueryCache: Send + Sync {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError>;

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError>;
}
