use super::*;

/// Tempo v1 trace-by-id, at `/api/traces/{id}`.
///
/// Grafana's Tempo *backend* datasource fetches the trace-view here with
/// `Accept: application/protobuf` and proto-decodes the body as OTLP. This
/// handler therefore defaults to OTLP `TracesData` protobuf, which is Tempo's
/// v1 default. It falls back to the wrapped JSON for humans.
pub(crate) async fn trace_by_id_v1<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = trace_by_id_v1_inner(&state, headers, trace_id, uri).await;
    state.record_query("trace_by_id", resp.status().is_success(), start);
    resp
}
