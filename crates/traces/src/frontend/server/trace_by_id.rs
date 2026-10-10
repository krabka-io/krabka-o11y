use super::{
    ApiVersion, BlockCatalog, FrontendRequest, IntoResponse, Json, Path, QuerierBackend, Response,
    RouteVariant, StatusCode, TraceStatus, backend_error_response, json, optional_time_bounds,
    parse_hex16, tenant_and_bounds, trace_v1_response,
};

/// The v2 trace-by-id envelope at `/api/v2/traces/{id}`, or for
/// [`ApiVersion::V1`] the bare trace that `/api/traces/{id}` answers.
pub(crate) async fn trace_by_id<B, C, V>(
    request: FrontendRequest<B, C>,
    Path(trace_id): Path<String>,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    }
    let (tenant, start_ns, end_ns) =
        match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
            Ok(request) => request,
            Err(rejection) => return *rejection,
        };
    let FrontendRequest { qf, headers, .. } = request;
    let tid = parse_hex16(&trace_id);
    let (trace, _metrics, status, warnings) =
        match qf.trace_by_id(&tenant, tid, start_ns, end_ns).await {
            Ok(out) => out,
            Err(err) => return backend_error_response(&err),
        };
    if matches!(V::VARIANT, ApiVersion::V1) {
        return trace_v1_response(&headers, trace);
    }

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
