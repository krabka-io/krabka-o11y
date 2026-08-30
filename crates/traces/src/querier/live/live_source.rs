use super::*;

#[async_trait::async_trait]
pub trait LiveSource: Send + Sync {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>>;

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>>;

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>>;

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}
