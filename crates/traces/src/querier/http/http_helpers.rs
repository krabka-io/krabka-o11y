use super::{trace_by_id::trace_by_id_with_encoding, *};

pub(crate) async fn overrides<S>(
    request: QuerierRequest<S>,
    method: Method,
    body: Bytes,
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
    let tenant = match request_tenant(&headers, &principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    overrides_api_response(
        &state.cfg.overrides,
        tenant.as_str(),
        &method,
        &headers,
        uri.query(),
        &body,
    )
}

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
    let encoding = if wants_json(&request.headers) {
        TraceEncoding::Json
    } else {
        TraceEncoding::Protobuf(trace_protobuf)
    };
    trace_by_id_with_encoding(request, Path(trace_id), encoding).await
}
