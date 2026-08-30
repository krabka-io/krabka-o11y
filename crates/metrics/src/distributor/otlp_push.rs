use super::*;

pub(crate) async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    // ONE ingest span per OTLP HTTP push request; series recorded post-decode.
    let span = ingest_span(&headers, body_size);
    let result = otlp_push_inner(&state, &headers, &body)
        .instrument(span)
        .await;
    record_ingest_outcome(&state, &result, body_size, started.elapsed().as_time());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}
