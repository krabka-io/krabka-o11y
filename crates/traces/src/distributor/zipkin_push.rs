use super::{
    Arc, Bytes, DistributorState, Extension, HeaderMap, Principal, Response, State, decode_zipkin,
    push_spans,
};

pub(crate) async fn zipkin_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_spans(
        &state,
        &principal,
        &headers,
        &body,
        &["application/json"],
        decode_zipkin,
    )
    .await
}
