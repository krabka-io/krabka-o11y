use super::{async_trait, RangeQueryExecutor, PromqlEngine, MetricStore, FrontendRangeQuery, QueryResult, PromqlError, query_with_shard_selector};

#[async_trait]
impl<S: MetricStore> RangeQueryExecutor for PromqlEngine<S> {
    async fn execute_range_query(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        let query_text = match query.shard {
            Some(shard) => query_with_shard_selector(&query.query, shard)?,
            None => query.query.clone(),
        };

        self.query_range(
            tenant,
            &query_text,
            query.start_ms,
            query.end_ms,
            query.step,
        )
        .await
    }
}
