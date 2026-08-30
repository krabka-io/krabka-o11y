use super::{SpanMetricsRegistry, EdgeStore};

#[derive(Debug)]
pub(crate) struct TenantState {
    pub(crate) span_metrics: SpanMetricsRegistry,
    pub(crate) edges: EdgeStore,
}
