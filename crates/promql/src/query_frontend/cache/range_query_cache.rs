use super::{AnnotatedQueryResult, FrontendRangeQuery, PromqlError, async_trait};

/// Caches the result of one planned range subquery with its annotations.
///
/// An implementor stores the annotations beside the result and returns both on a
/// hit. A cache that dropped the annotations would make the first request warn
/// and every later request stay silent, and that difference is not reproducible.
#[async_trait]
pub trait RangeQueryCache: Send + Sync {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<AnnotatedQueryResult>, PromqlError>;

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: AnnotatedQueryResult,
    ) -> Result<(), PromqlError>;
}
