use super::{
    Arc, Bytes, DistributorState, Extension, HeaderMap, Principal, Response, State,
    decode_jaeger_binary_thrift, decode_jaeger_thrift, is_jaeger_binary_thrift, push_spans,
};

pub(crate) async fn jaeger_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let binary = is_jaeger_binary_thrift(&headers);
    push_spans(
        &state,
        &principal,
        &headers,
        &body,
        &[
            "application/x-thrift",
            "application/octet-stream",
            "application/vnd.apache.thrift.binary",
        ],
        |body| {
            if binary {
                decode_jaeger_binary_thrift(body)
            } else {
                decode_jaeger_thrift(body)
            }
        },
    )
    .await
}
