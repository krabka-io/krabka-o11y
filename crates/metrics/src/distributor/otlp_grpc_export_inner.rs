use super::{DistributorState, TonicRequest, ExportMetricsServiceRequest, PushError, tenant_from_metadata, MetricsData, decode_otlp_stateful, TranslationStrategy, append_decoded_series};

/// Decodes and appends an OTLP gRPC export. Returns the decoded series count on
/// success, which is the ingest `items` measure.
pub(crate) async fn otlp_grpc_export_inner(
    state: &DistributorState,
    request: TonicRequest<ExportMetricsServiceRequest>,
) -> Result<u64, PushError> {
    let tenant = tenant_from_metadata(request.metadata())?.to_string();
    let data = MetricsData {
        resource_metrics: request.into_inner().resource_metrics,
    };
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful(&data, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(&tenant, items);
    }
    Ok(items)
}
