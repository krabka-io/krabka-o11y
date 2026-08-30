use super::{async_trait, FrontendRangeQuery, QueryResult, PromqlError};

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
