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

    #[must_use]
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        self.source.block_builder_frontier_ns(tenant)
    }
}
