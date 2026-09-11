use super::{
    ByteSizeExt, Bytes, CONTENT_ENCODING, CONTENT_TYPE, DistributorState, HeaderMap, Instant,
    Instrument, IntoResponse, RequestSecurity, Response, State, StatusCode, TenantErrorSurface,
    append_distributor_wal_records, measured_size, normalize_otlp_http_logs,
    otlp_http_error_response, record_ingest_response, resolve_single_tenant, tenant_error_response,
    tenant_header_value, validate_ingest_body_limit,
};

pub(crate) async fn push_otlp_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let body_size = measured_size(body.len());
    // As the Loki push handler does: resolved before the body is read.
    let tenant = match resolve_single_tenant(tenant_header_value(&headers)) {
        Ok(tenant) => tenant,
        Err(error) => {
            let response = tenant_error_response(&error, TenantErrorSurface::OtlpPush);
            return record_ingest_response(&state, response, body_size, 0, start);
        }
    };
    if let Err(denied) = security.authorize_tenant(&tenant) {
        return record_ingest_response(&state, denied.into_response(), body_size, 0, start);
    }
    // ONE server span per OTLP push request, mirroring the Loki push handler:
    // the OTLP emit path feeds the same log WAL, so instrumenting it keeps the
    // produce→compaction trace intact for OTLP-emitted logs too.
    let span = tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__krabka_observability_logs_wal",
        krabka.tenant = %tenant,
        krabka.ingest.lines = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    );
    async move {
        // One resolution for the whole push, as the Loki push handler does.
        let limits = state.limits_for(&tenant).clone();
        if let Err(error) = validate_ingest_body_limit(&limits, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_otlp_http_logs(&tenant, &headers, &body, &limits) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("krabka.ingest.lines", items);
                state.metrics.record_ingest_lines(tenant.as_str(), items);
                let resp = match append_distributor_wal_records(&state, &security, &tenant, records)
                    .await
                {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => {
                        // Surface why an accepted OTLP log batch failed to persist
                        // (WAL append errors are otherwise opaque to the client).
                        tracing::debug!(error = %error, "OTLP logs: WAL append failed");
                        error.into_response()
                    }
                };
                return record_ingest_response(&state, resp, body_size, items, start);
            }
            Err(error) => {
                // Surface why an OTLP log push was rejected at decode/normalize
                // (content-type, encoding, and size pinpoint client misconfig).
                tracing::debug!(
                    error = %error,
                    content_type = ?headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()),
                    content_encoding = ?headers.get(CONTENT_ENCODING).and_then(|v| v.to_str().ok()),
                    bytes = body_size.bytes_u64(),
                    "OTLP logs: decode/normalize rejected the request"
                );
                otlp_http_error_response(error)
            }
        };
        record_ingest_response(&state, resp, body_size, 0, start)
    }
    .instrument(span)
    .await
}
