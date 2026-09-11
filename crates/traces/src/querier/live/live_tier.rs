use super::{Arc, LiveSource, RecordBatch, Result, ScopedTag, TagScope, TraceSpans, TypedValue};

pub struct LiveTier {
    pub(crate) source: Arc<dyn LiveSource>,
}

impl LiveTier {
    #[must_use]
    pub fn new(source: Arc<dyn LiveSource>) -> Self {
        Self { source }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        self.source.span_batches(tenant, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the live source query fails.
    pub async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.source.trace_spans(tenant, trace_id).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.source.tag_names(tenant, scope, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.source.tag_values(tenant, tag, start_ns, end_ns).await
    }

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
    #[must_use]
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        self.source.block_builder_frontier_ns(tenant)
    }
}
