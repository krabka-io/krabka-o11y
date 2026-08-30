use super::{Arc, RwLock, LiveStore, SharedTraceIndex, LiveSource};

pub(crate) struct IndexedLiveSource {
    pub(crate) store: Arc<RwLock<LiveStore>>,
    pub(crate) trace_index: SharedTraceIndex,
}

impl IndexedLiveSource {
    pub(crate) fn new(store: Arc<RwLock<LiveStore>>, trace_index: SharedTraceIndex) -> Self {
        Self { store, trace_index }
    }
}

#[async_trait::async_trait]
impl LiveSource for IndexedLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<arrow::record_batch::RecordBatch>> {
        let guard = self.store.read().await;
        guard.span_batches(tenant, start_ns, end_ns).await
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> krabka_traces::querier::live::Result<Option<krabka_traceql::TraceSpans>> {
        let guard = self.store.read().await;
        guard.trace_spans(tenant, trace_id).await
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<krabka_traceql::ScopedTag>> {
        let guard = self.store.read().await;
        guard.tag_names(tenant, scope, start_ns, end_ns).await
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<krabka_traceql::TypedValue>> {
        let guard = self.store.read().await;
        guard.tag_values(tenant, tag, start_ns, end_ns).await
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        let trace_index = self.trace_index.load();
        trace_index
            .trace_blocks(tenant)
            .iter()
            .map(|block| block.max_ts.saturating_add(1))
            .max()
            .unwrap_or_default()
    }
}
