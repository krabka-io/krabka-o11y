use super::{
    Arc, Bytes, DistributorState, Extension, HeaderMap, Principal, Response, SpanPush, State,
    decode_zipkin, push_spans,
};

pub(crate) async fn zipkin_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_spans(SpanPush {
        state: &state,
        principal: &principal,
        headers: &headers,
        body: &body,
        content_types: &["application/json"],
        decode: decode_zipkin,
    })
    .await
}
