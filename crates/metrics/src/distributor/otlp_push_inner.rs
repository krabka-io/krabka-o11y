use super::{
    DecodedSeries, DistributorState, HeaderMap, PushError, PushSuccess, TenantId,
    TranslationStrategy, append_decoded_series, decode_otlp_http_body,
    decode_otlp_stateful_bytes_with_promoted_resource_attributes,
    require_otlp_protobuf_content_type,
};

pub(crate) async fn otlp_push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    require_otlp_protobuf_content_type(headers)?;
    let body = decode_otlp_http_body(headers, body, state.max_decompressed)?;
    let mut series = decode_tenant_otlp(state, tenant, &body)?;
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("krabka.ingest.series", items);
    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok((PushSuccess::Ok, items))
}

/// Decodes the body against the tenant's own delta accumulator, and bounds that
/// accumulator against the tenant's own limits before the lock is released.
fn decode_tenant_otlp(
    state: &DistributorState,
    tenant: &TenantId,
    body: &[u8],
) -> Result<Vec<DecodedSeries>, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    let accumulator = state.otlp_delta_accumulators.for_tenant(tenant);
    let mut guard = accumulator.lock().expect("otlp delta accumulator poisoned");
    guard.seen_at(now);
    let decoded = decode_otlp_stateful_bytes_with_promoted_resource_attributes(
        body,
        TranslationStrategy::default(),
        &mut guard,
        &state.otlp_promote_resource_attributes,
    );
    // Bound before the error is propagated, so a rejected body cannot leave the
    // streams it created behind.
    guard.bound(limits);
    Ok(decoded?)
}
