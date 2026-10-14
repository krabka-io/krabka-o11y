use super::{
    DistributorState, ExportMetricsServiceRequest, MetricsData, Principal, PushError, TonicRequest,
    TranslationStrategy, append_decoded_series, authorize_tenant, decode_otlp_inner,
    decode_tenant_otlp, tenant_from_metadata,
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
    let mut series =
        decode_tenant_otlp(state, &tenant, TranslationStrategy::default(), |options| {
            decode_otlp_inner(&data, options)
        })?;
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok(items)
}
