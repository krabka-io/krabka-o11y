use super::*;

pub(crate) async fn influx_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: BodyBytes,
) -> Response {
    let ingest = IngestRequestStart::begin(&headers, &principal, &body);
    let result = async {
        influx_push_inner(
            &state,
            ingest.authorized_tenant()?,
            &headers,
            raw_query.as_deref(),
            &body,
        )
        .await
    }
    .instrument(ingest.span())
    .await;
    ingest.respond(&state, result)
}
