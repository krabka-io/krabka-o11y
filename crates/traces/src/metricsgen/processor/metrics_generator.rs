use super::*;

/// Composes span-metrics and service-graph processors per tenant.
pub struct MetricsGenerator {
    pub(crate) cfg: MetricsGenConfig,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) per_tenant: HashMap<String, TenantState>,
}

impl MetricsGenerator {
    #[must_use]
    pub fn new(cfg: MetricsGenConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            cfg,
            clock,
            per_tenant: HashMap::new(),
        }
    }

    pub fn process(&mut self, span: &SpanRecord) {
        let cfg = &self.cfg;
        let state = self
            .per_tenant
            .entry(span.tenant.clone())
            .or_insert_with(|| TenantState {
                span_metrics: SpanMetricsRegistry::new(cfg),
                edges: EdgeStore::new(cfg),
            });

        state.span_metrics.record_span(span);
        state.edges.record_span(span, self.clock.now_ns());
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn restore_edge_checkpoint(
        &mut self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), CheckpointCodecError> {
        let cfg = &self.cfg;
        let state = self
            .per_tenant
            .entry(tenant.to_string())
            .or_insert_with(|| TenantState {
                span_metrics: SpanMetricsRegistry::new(cfg),
                edges: EdgeStore::new(cfg),
            });
        state.edges.restore_checkpoint_entry(tenant, key, value)
    }

    #[must_use]
    pub fn collect(&mut self, timestamp_ms: i64) -> Vec<SeriesPayload> {
        let now_ns = self.clock.now_ns();
        let mut payloads = Vec::new();

        for (tenant, state) in &mut self.per_tenant {
            state.edges.expire(now_ns);
            let mut series = state.span_metrics.drain(timestamp_ms);
            series.extend(state.edges.drain(timestamp_ms));
            if !series.is_empty() {
                payloads.push(SeriesPayload {
                    tenant: tenant.clone(),
                    series,
                });
            }
        }

        payloads
    }

    #[must_use]
    pub fn edge_checkpoints(&self) -> Vec<TenantEdgeCheckpoints> {
        let mut checkpoints: Vec<_> = self
            .per_tenant
            .iter()
            .map(|(tenant, state)| (tenant.clone(), state.edges.checkpoint_entries(tenant)))
            .collect();
        checkpoints.sort_by(|a, b| a.0.cmp(&b.0));
        checkpoints
    }
}
