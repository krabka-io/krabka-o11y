use super::*;

pub(crate) async fn ingest_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    // ONE server span per ingest request. The `/ingest` door carries exactly one
    // profile per request, so `krabka.ingest.samples` is fixed at 1.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        krabka.tenant = %ingest_span_tenant(&headers),
        krabka.ingest.samples = 1_u64,
        krabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let query = parse_ingest_query(query.as_deref().unwrap_or(""))?;
        let content_type = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let raw = decode_ingest_body_with_limits(
            &query,
            content_type,
            body,
            state.max_decompressed,
            state.legacy_decode_limits,
        )
        .await?;
        process_raw(&state, &tenant, vec![raw]).await
    }
    .instrument(ingest_span)
    .await;

    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, 1);
    }
    // The `/ingest` door carries exactly one profile per request.
    state.metrics.record_ingest(
        result.is_ok(),
        IngestBytes(bytes),
        IngestItems(1),
        start.elapsed().as_time(),
    );
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => profiles_error_response(err),
    }
}
