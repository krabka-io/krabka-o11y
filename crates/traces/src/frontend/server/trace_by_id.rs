use super::{
    Arc, BlockCatalog, HeaderMap, IntoResponse, Json, Path, QuerierBackend, QueryFrontend,
    Response, State, StatusCode, TraceStatus, Uri, backend_error_response, json,
    optional_time_bounds, parse_hex16, tenant,
};

pub(crate) async fn trace_by_id<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    }
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let tid = parse_hex16(&trace_id);
    let (trace, _metrics, status, warnings) =
        match qf.trace_by_id(&tenant, tid, start_ns, end_ns).await {
            Ok(out) => out,
            Err(err) => return backend_error_response(&err),
        };

    let Some(trace) = trace else {
        // 404 asserts the trace does not exist. With a querier out of the
        // fan-out, nobody asked the one that may have held it, so the honest
        // 404 says what was not looked at rather than claiming absence.
        if warnings.is_empty() {
            return (StatusCode::NOT_FOUND, "trace not found").into_response();
        }
        return (
            StatusCode::NOT_FOUND,
            format!(
                "trace not found in the queriers that answered; {}",
                warnings.join("; ")
            ),
        )
            .into_response();
    };
    // v2 envelope: { trace, status, message }. Per the querier's contract the
    // by-id endpoint does NOT carry a metrics block. Tempo's own `message`
    // carries why a trace came back `PARTIAL`, so an excluded querier is
    // reported there rather than in a field Grafana would not read.
    let message = match status {
        TraceStatus::Partial => warnings.join("; "),
        TraceStatus::Complete => String::new(),
    };
    Json(json!({
        "trace": trace.trace,
        "status": status.as_str(),
        "message": message,
    }))
    .into_response()
}
