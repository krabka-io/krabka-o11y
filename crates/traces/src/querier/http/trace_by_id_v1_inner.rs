use super::{SpanStore, AppState, HeaderMap, Uri, Response, decode_trace_id, IntoResponse, StatusCode, tenant, optional_time_bounds, wants_json, Json, trace_json, trace_protobuf, header};

pub(crate) async fn trace_by_id_v1_inner<S>(
    state: &AppState<S>,
    headers: HeaderMap,
    trace_id: String,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let Ok(trace_id) = decode_trace_id(&trace_id) else {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    };
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .trace_by_id_within(&tenant, &trace_id, start_ns, end_ns)
        .await
    {
        Ok(Some(trace)) => {
            if wants_json(&headers) {
                Json(trace_json(&trace, state.cfg.max_trace_spans)).into_response()
            } else {
                match trace_protobuf(&trace, state.cfg.max_trace_spans) {
                    Ok(bytes) => {
                        ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response()
                    }
                    Err(err) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }
                }
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, "trace not found").into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}
