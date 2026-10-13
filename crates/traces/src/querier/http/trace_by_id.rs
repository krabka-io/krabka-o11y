use super::{
    Path, QuerierRequest, Response, SpanStore, TraceByIdRequest, TraceEncoding, trace_by_id_inner,
    trace_by_id_response_protobuf, wants_protobuf,
};

pub(crate) async fn trace_by_id<S>(
    request: QuerierRequest<S>,
    Path(trace_id): Path<String>,
) -> Response
where
    S: SpanStore + 'static,
{
    let encoding = if wants_protobuf(&request.headers) {
        TraceEncoding::Protobuf(trace_by_id_response_protobuf)
    } else {
        TraceEncoding::Json
    };
    trace_by_id_with_encoding(request, Path(trace_id), encoding).await
}

pub(crate) async fn trace_by_id_with_encoding<S>(
    request: QuerierRequest<S>,
    Path(trace_id): Path<String>,
    encoding: TraceEncoding,
) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest {
        state,
        principal,
        headers,
        uri,
    } = request;
    let start = std::time::Instant::now();
    let resp = trace_by_id_inner(
        &state,
        TraceByIdRequest {
            principal: &principal,
            headers,
            trace_id,
            uri,
            encoding,
        },
    )
    .await;
    state.record_query("trace_by_id", resp.status(), start);
    resp
}
