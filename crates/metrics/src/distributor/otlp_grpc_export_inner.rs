use super::{
    DecodedSeries, DistributorState, ExportMetricsServiceRequest, MetricsData, Principal,
    PushError, TenantId, TonicRequest, TranslationStrategy, append_decoded_series,
    authorize_tenant, decode_otlp_stateful_with_promoted_resource_attributes, tenant_from_metadata,
};

/// Decodes and appends an OTLP gRPC export. Returns the decoded series count on
/// success, which is the ingest `items` measure.
///
/// The router mounts the gRPC service with `route_service`, so the
/// authentication layer has put the request's [`Principal`] into its
/// extensions. A request without one fails closed.
pub(crate) async fn otlp_grpc_export_inner(
    state: &DistributorState,
    request: TonicRequest<ExportMetricsServiceRequest>,
) -> Result<u64, PushError> {
    let principal = request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or(PushError::MissingPrincipal)?;
    let tenant = tenant_from_metadata(request.metadata())?;
    authorize_tenant(&principal, &tenant)?;
    let data = MetricsData {
        resource_metrics: request.into_inner().resource_metrics,
    };
    let mut series = decode_tenant_otlp(state, &tenant, &data)?;
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok(items)
}

/// Decodes against the tenant's own delta accumulator, and bounds that
/// accumulator against the tenant's own limits before the lock is released.
fn decode_tenant_otlp(
    state: &DistributorState,
    tenant: &TenantId,
    data: &MetricsData,
) -> Result<Vec<DecodedSeries>, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    let accumulator = state.otlp_delta_accumulators.for_tenant(tenant);
    let mut guard = accumulator.lock().expect("otlp delta accumulator poisoned");
    guard.seen_at(now);
    let decoded = decode_otlp_stateful_with_promoted_resource_attributes(
        data,
        TranslationStrategy::default(),
        &mut guard,
        &state.otlp_promote_resource_attributes,
    );
    // Bound before the error is propagated, so a rejected body cannot leave the
    // streams it created behind.
    guard.bound(limits);
    Ok(decoded?)
}
