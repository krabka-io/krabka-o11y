use super::{
    Result, ScanOptions, ScanResult, SpanMatcher, TagCatalog, TraceSpans,
    filter_trace_spans_by_time,
};

#[async_trait::async_trait]
pub trait SpanStore: TagCatalog {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult>;

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult> {
        let _ = options;
        self.scan(tenant, matchers, start_ns, end_ns).await
    }

    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>> {
        Ok(self
            .trace_by_id(tenant, trace_id)
            .await?
            .map(|trace| filter_trace_spans_by_time(trace, start_ns, end_ns)))
    }
}
