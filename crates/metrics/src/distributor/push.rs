use super::*;

pub(crate) async fn push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let ingest = IngestRequestStart::begin(&headers, &principal, &body);
    // ONE ingest span per request (not per series/sample). `krabka.ingest.series`
    // starts empty and is recorded from inside `push_inner` once the body is
    // decoded; the WAL producer injects this span's trace context into the record
    // headers so the compactor's span joins the same distributed trace.
    let result = async { push_inner(&state, ingest.authorized_tenant()?, &headers, &body).await }
        .instrument(ingest.span())
        .await;
    ingest.respond(&state, result)
}
