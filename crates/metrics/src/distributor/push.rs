use super::*;

pub(crate) async fn push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let tenant = authorized_tenant_from_headers(&headers, &principal);
    // ONE ingest span per request (not per series/sample). `krabka.ingest.series`
    // starts empty and is recorded from inside `push_inner` once the body is
    // decoded; the WAL producer injects this span's trace context into the record
    // headers so the compactor's span joins the same distributed trace.
    let span = ingest_span(tenant.as_ref().ok(), body_size);
    let result = async { push_inner(&state, &tenant?, &headers, &body).await }
        .instrument(span)
        .await;
    record_ingest_outcome(&state, &result, body_size, started.elapsed().as_time());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}
