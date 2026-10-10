use super::{RecordBatch, Result, TagCatalog, TraceSpans};

#[async_trait::async_trait]
pub trait LiveSource: TagCatalog {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>>;

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    /// The exclusive upper bound of what the block builder has flushed for
    /// `tenant`: one nanosecond past the newest `max_ts` across its blocks.
    ///
    /// This is a *planning hint*, not a tier boundary. It says where the
    /// flushed data ends, not where the unflushed data begins: a span can reach
    /// the hot tier with a `start_ns` well below this bound (a lagging client
    /// clock, a batching exporter, a long span that outlives its siblings), and
    /// no block holds it yet. Splitting a query window here and reading the hot
    /// tier only above it drops exactly those spans, and drops them silently.
    /// [`crate::querier::store::KrabkaSpanStore`] therefore scans both tiers
    /// over the whole window and deduplicates the overlap.
    fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}
