use super::*;

pub(crate) async fn otlp_push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    require_otlp_protobuf_content_type(headers)?;
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful_bytes(body, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("krabka.ingest.series", items);
    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::Ok, items))
}
