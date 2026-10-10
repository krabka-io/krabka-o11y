use super::*;

pub(crate) async fn clocks_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let ingest = IngestRequestStart::begin(&headers, &principal, &body);
    // ONE ingest span per clock batch, as on the `remote_write` push path.
    let result =
        async { clocks_push_inner(&state, ingest.authorized_tenant()?, &headers, &body).await }
            .instrument(ingest.span())
            .await;
    ingest.respond(&state, result)
}
