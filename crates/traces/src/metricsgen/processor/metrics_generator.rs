use super::{
    Arc, CheckpointCodecError, Clock, EdgeStore, Entry, HashMap, MetricsGenConfig, RecordOutcome,
    SeriesPayload, SpanMetricsRegistry, SpanRecord, TenantEdgeCheckpoints, TenantState,
};

/// Composes span-metrics and service-graph processors per tenant.
pub struct MetricsGenerator {
    pub(crate) cfg: MetricsGenConfig,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) per_tenant: HashMap<String, TenantState>,
    pub(crate) discarded_tenant_spans: u64,
    pub(crate) tenant_cap_reported: bool,
}

impl MetricsGenerator {
    #[must_use]
    pub fn new(cfg: MetricsGenConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            cfg,
            clock,
            per_tenant: HashMap::new(),
            discarded_tenant_spans: 0,
            tenant_cap_reported: false,
        }
    }

    /// Fold one span into the state of its tenant.
    ///
    /// This returns [`RecordOutcome::Dropped`] when the generator holds state
    /// for `max_tenants` tenants already and the span names another one, and
    /// [`RecordOutcome::Recorded`] otherwise. The outcome is of the tenant, not
    /// of the two processors below it: each of them counts its own refusals
    /// into a series that [`Self::collect`] drains, so a caller that ignores
    /// this value still sees every span-metrics and service-graph drop.
    pub fn process(&mut self, span: &SpanRecord) -> RecordOutcome {
        let cfg = &self.cfg;
        // Read the length before the entry borrow, because `entry` holds the
        // map for as long as the match below runs.
        let at_capacity = cfg.max_tenants != 0 && self.per_tenant.len() >= cfg.max_tenants;
        let state = match self.per_tenant.entry(span.tenant.clone()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                if at_capacity {
                    self.discarded_tenant_spans = self.discarded_tenant_spans.saturating_add(1);
                    // Once, not once per span: the map stays full, so every
                    // later refusal would repeat this line on the ingest path.
                    if !self.tenant_cap_reported {
                        self.tenant_cap_reported = true;
                        tracing::warn!(
                            max_tenants = cfg.max_tenants,
                            "metrics generator holds its maximum tenants; spans of new tenants are discarded"
                        );
                    }
                    return RecordOutcome::Dropped;
                }
                let tenant_cfg = cfg.for_tenant(&span.tenant);
                vacant.insert(TenantState {
                    span_metrics: SpanMetricsRegistry::new(&tenant_cfg),
                    edges: EdgeStore::new(&tenant_cfg),
                })
            }
        };

        state.span_metrics.record_span(span);
        state.edges.record_span(span, self.clock.now_ns());
        RecordOutcome::Recorded
    }

    /// How many spans this generator refused because its tenant map was full.
    #[must_use]
    pub fn discarded_tenant_spans(&self) -> u64 {
        self.discarded_tenant_spans
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
        // A replay obeys the cap too. A restart that lowers `max_tenants` would
        // otherwise restore more tenants than it goes on to admit, and the map
        // would stay above the new cap until the process stopped.
        let at_capacity = cfg.max_tenants != 0 && self.per_tenant.len() >= cfg.max_tenants;
        let state = match self.per_tenant.entry(tenant.to_string()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                if at_capacity {
                    return Ok(());
                }
                let tenant_cfg = cfg.for_tenant(tenant);
                vacant.insert(TenantState {
                    span_metrics: SpanMetricsRegistry::new(&tenant_cfg),
                    edges: EdgeStore::new(&tenant_cfg),
                })
            }
        };
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
