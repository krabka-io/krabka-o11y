use super::{
    Path, QuerierRequest, Response, SpanStore, TraceByIdRequest, TraceEncoding, trace_by_id_inner,
    trace_protobuf, wants_json,
};

/// Tempo v1 trace-by-id, at `/api/traces/{id}`.
///
/// Grafana's Tempo *backend* datasource fetches the trace-view here with
/// `Accept: application/protobuf` and proto-decodes the body as OTLP. This
/// handler therefore defaults to OTLP `TracesData` protobuf, which is Tempo's
/// v1 default. It falls back to the wrapped JSON for humans.
pub(crate) async fn trace_by_id_v1<S>(
    request: QuerierRequest<S>,
    Path(trace_id): Path<String>,
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
    let encoding = if wants_json(&headers) {
        TraceEncoding::Json
    } else {
        TraceEncoding::Protobuf(trace_protobuf)
    };
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
