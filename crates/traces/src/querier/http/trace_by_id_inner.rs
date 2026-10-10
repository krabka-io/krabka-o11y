use super::{
    AppState, HeaderMap, IntoResponse, Json, Principal, Response, SpanStore, StatusCode,
    TenantRequest, TraceSpans, Uri, decode_trace_id, header, optional_time_bounds,
    tenant_and_bounds, trace_json,
};

/// How a trace-by-id endpoint encodes a found trace as protobuf.
pub(crate) type TraceProtobuf = fn(&TraceSpans, usize) -> Result<Vec<u8>, prost::EncodeError>;

/// How a trace-by-id endpoint answers with a found trace. The v1 and v2
/// endpoints differ in which encoding they default to and in the protobuf
/// message they send.
pub(crate) enum TraceEncoding {
    Json,
    Protobuf(TraceProtobuf),
}

/// A trace-by-id request.
pub(crate) struct TraceByIdRequest<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) trace_id: String,
    pub(crate) uri: Uri,
    pub(crate) encoding: TraceEncoding,
}

/// Answer a trace-by-id request in its encoding.
pub(crate) async fn trace_by_id_inner<S>(
    state: &AppState<S>,
    request: TraceByIdRequest<'_>,
) -> Response
where
    S: SpanStore + 'static,
{
    let TraceByIdRequest {
        principal,
        headers,
        trace_id,
        uri,
        encoding,
    } = request;
    let Ok(trace_id) = decode_trace_id(&trace_id) else {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    };
    let (tenant, start_ns, end_ns) = match tenant_and_bounds(
        TenantRequest {
            headers: &headers,
            principal,
            policy: &state.cfg.tenant_policy,
            uri: &uri,
        },
        optional_time_bounds,
    ) {
        Ok(tenant_window) => tenant_window,
        Err(rejection) => return *rejection,
    };

    match state
        .engine
        .trace_by_id_within(tenant.as_str(), &trace_id, start_ns, end_ns)
        .await
    {
        Ok(Some(trace)) => match encoding {
            TraceEncoding::Protobuf(encode) => match encode(&trace, state.cfg.max_trace_spans) {
                Ok(bytes) => {
                    ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response()
                }
                Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
            },
            TraceEncoding::Json => {
                Json(trace_json(&trace, state.cfg.max_trace_spans)).into_response()
            }
        },
        Ok(None) => (StatusCode::NOT_FOUND, "trace not found").into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}
