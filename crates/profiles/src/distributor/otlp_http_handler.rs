use super::*;

pub(crate) async fn otlp_http_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    let mut items: u64 = 0;
    // ONE server span per ingest request (not per sample). `krabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        krabka.tenant = %ingest_span_tenant(&headers),
        krabka.ingest.samples = tracing::field::Empty,
        krabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let req = pb::otlp_profiles::ExportProfilesServiceRequest::decode(body)
            .map_err(|err| ProfilesError::Decode(format!("OTLP profiles decode: {err}")))?;
        let raws = decode_otlp(&req)?;
        items = raws.len() as u64;
        process_raw(&state, &tenant, raws).await?;
        Ok::<_, ProfilesError>(
            pb::otlp_profiles::ExportProfilesServiceResponse {
                partial_success: None,
            }
            .encode_to_vec(),
        )
    }
    .instrument(ingest_span.clone())
    .await;

    ingest_span.record("krabka.ingest.samples", items);
    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, items);
    }
    state.metrics.record_ingest(
        result.is_ok(),
        IngestBytes(bytes),
        IngestItems(items),
        start.elapsed().as_time(),
    );
    match result {
        Ok(body) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/x-protobuf")],
            Bytes::from(body),
        )
            .into_response(),
        Err(err) => profiles_error_response(err),
    }
}
