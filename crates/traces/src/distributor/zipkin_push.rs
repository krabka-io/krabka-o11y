use super::{PushRequest, Response, SpanPush, decode_zipkin, push_spans};

pub(crate) async fn zipkin_push(request: PushRequest) -> Response {
    let PushRequest {
        state,
        principal,
        headers,
        body,
    } = request;
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
