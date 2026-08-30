use super::{Arc, WalSink, TenantLimits, Limits, OverridesProvider, IngestEnforcer, ByteSize, ServiceMetrics, mebibytes, Span, TracesError, validate, validate_shared, limit_error_to_traces_error};

/// Shared distributor state.
pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    pub limits: TenantLimits,
    pub shared_limits: Limits,
    pub overrides: Option<OverridesProvider>,
    pub ingest_enforcer: IngestEnforcer,
    /// Ceiling on a decompressed request body.
    pub max_decompressed: ByteSize,
    pub metrics: ServiceMetrics,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self::with_metrics(sink, ServiceMetrics::new())
    }

    #[must_use]
    pub fn with_metrics(sink: Arc<dyn WalSink>, metrics: ServiceMetrics) -> Self {
        Self {
            sink,
            limits: TenantLimits::default(),
            shared_limits: TenantLimits::default().to_shared_limits(),
            overrides: None,
            ingest_enforcer: IngestEnforcer::new(),
            max_decompressed: mebibytes(10),
            metrics,
        }
    }
}

impl DistributorState {
    pub(crate) fn enforce_ingest(&self, tenant: &str, spans: &[Span]) -> Result<(), TracesError> {
        validate(spans, &self.limits)?;
        let limits = self.ingest_limits_for_tenant(tenant);
        validate_shared(spans, limits)?;
        self.ingest_enforcer
            .check_span_rate(
                limits,
                tenant,
                u64::try_from(spans.len()).unwrap_or(u64::MAX),
            )
            .map_err(|err| limit_error_to_traces_error(&err))
    }

    pub(crate) fn ingest_limits_for_tenant(&self, tenant: &str) -> &Limits {
        self.overrides
            .as_ref()
            .map_or(&self.shared_limits, |overrides| {
                overrides.for_tenant(tenant)
            })
    }
}
