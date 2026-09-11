use super::{
    Arc, ByteSize, IngestEnforcer, Limits, OverridesProvider, Principal, ServiceMetrics, Span,
    TenantId, TenantPolicy, TracesError, WalSink, authorize_tenant, limit_error_to_traces_error,
    mebibytes, validate_shared,
};

/// Shared distributor state.
pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    /// The one place a tenant's ingest limits come from.
    ///
    /// Its defaults are what an unlisted tenant gets, so a process with no
    /// overrides file still has every limit here and needs no second set of
    /// globals beside it.
    pub overrides: OverridesProvider,
    pub ingest_enforcer: IngestEnforcer,
    /// Ceiling on a decompressed request body.
    pub max_decompressed: ByteSize,
    pub metrics: ServiceMetrics,
    /// The policy that every ingest door resolves a tenant with.
    ///
    /// The HTTP doors read the header and the gRPC doors read the metadata.
    /// The Jaeger UDP receiver has neither, so each datagram gets what the
    /// policy gives a request without a tenant. Default:
    /// [`TenantPolicy::anonymous`].
    pub tenant_policy: TenantPolicy,
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
            overrides: OverridesProvider::new(Limits::default()),
            ingest_enforcer: IngestEnforcer::new(),
            max_decompressed: mebibytes(10),
            metrics,
            tenant_policy: TenantPolicy::anonymous(),
        }
    }
}

impl DistributorState {
    /// Resolves the tenant of one push from its raw `X-Scope-OrgID` value, and checks that `principal` may use it.
    ///
    /// `value` is `None` for a door that carries no header or metadata. Every
    /// door calls this before it decodes the body, so a denied push costs no
    /// decode and writes nothing.
    pub(crate) fn resolve_tenant(
        &self,
        principal: &Principal,
        value: Option<&[u8]>,
    ) -> Result<TenantId, TracesError> {
        // Tempo with multi-tenancy off ignores this header. Krabka does not:
        // it isolates the WAL, the blocks and the queries by tenant. So a
        // malformed value is a 400 or an `InvalidArgument` here, and is never
        // stored under the fallback tenant.
        let tenant = TenantId::resolve(value, &self.tenant_policy)?;
        authorize_tenant(principal, &tenant)?;
        Ok(tenant)
    }

    pub(crate) fn enforce_ingest(
        &self,
        tenant: &TenantId,
        spans: &[Span],
    ) -> Result<(), TracesError> {
        // Resolved once, and given to every gate below, so that no gate can
        // read a different limit from the one the tenant was granted.
        let limits = self.overrides.for_tenant(tenant.as_str());
        validate_shared(spans, limits)?;
        self.ingest_enforcer
            .check_span_rate(
                limits,
                tenant.as_str(),
                u64::try_from(spans.len()).unwrap_or(u64::MAX),
            )
            .map_err(|err| limit_error_to_traces_error(&err))
    }
}
