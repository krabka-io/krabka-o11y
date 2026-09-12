use super::{
    AnnotatedQueryResult, FrontendRangeQuery, MetricStore, PromqlEngine, PromqlError,
    RangeQueryExecutor, TenantId, async_trait, query_with_shard_selector,
};

#[async_trait]
impl<S: MetricStore> RangeQueryExecutor for PromqlEngine<S> {
    async fn execute_range_query(
        &self,
        tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        let query_text = match query.shard {
            Some(shard) => query_with_shard_selector(&query.query, shard)?,
            None => query.query.clone(),
        };

        let (result, annotations) = self
            .query_range_with_annotations(
                tenant,
                &query_text,
                query.start_ms,
                query.end_ms,
                query.step,
            )
            .await?;
        Ok(AnnotatedQueryResult {
            result,
            annotations,
        })
    }
}
