use super::*;


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
        return status_text_response(StatusCode::BAD_REQUEST, "trace id must be 32 hex chars");
    };
    let bounds = match tenant_and_bounds(
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
        .trace_by_id_within(
            bounds.tenant.as_str(),
            &trace_id,
            bounds.start_ns,
            bounds.end_ns,
        )
        .await
    {
        Ok(Some(trace)) => match encoding {
            TraceEncoding::Protobuf(encode) => match encode(&trace, state.cfg.max_trace_spans) {
                Ok(bytes) => status_bytes_response(StatusCode::OK, "application/protobuf", bytes),
                Err(err) => {
                    status_text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
                }
            },
            TraceEncoding::Json => {
                Json(trace_json(&trace, state.cfg.max_trace_spans)).into_response()
            }
        },
        Ok(None) => status_text_response(StatusCode::NOT_FOUND, "trace not found"),
        Err(err) => status_text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

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
