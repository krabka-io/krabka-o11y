use super::{
    ByteSizeExt, Bytes, DistributorState, HeaderMap, Instant, Instrument, IntoResponse,
    RequestSecurity, Response, State, StatusCode, TenantErrorSurface,
    append_distributor_wal_records, measured_size, normalize_loki_http_push,
    record_ingest_response, resolve_single_tenant, tenant_error_response, tenant_header_value,
    validate_ingest_body_limit,
};

pub(crate) async fn push_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let body_size = measured_size(body.len());
    // The tenant is resolved before anything reads the body, so a malformed
    // tenant never picks limits and never reaches the WAL.
    let tenant = match resolve_single_tenant(tenant_header_value(&headers)) {
        Ok(tenant) => tenant,
        Err(error) => {
            let response = tenant_error_response(&error, TenantErrorSurface::Push);
            return record_ingest_response(&state, response, body_size, 0, start);
        }
    };
    if let Err(denied) = security.authorize_tenant(&tenant) {
        return record_ingest_response(&state, denied.into_response(), body_size, 0, start);
    }
    // ONE server span per push request (not per log line): wraps the whole
    // ingest body so the produce-side WAL append (which injects `traceparent`)
    // and downstream compaction stitch onto this trace. `krabka.ingest.lines`
    // is unknown until normalization, so it is recorded on the span below.
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
        // One resolution for the whole push: the body cap, the line cap, the
        // label caps and the two timestamp windows all come from this set.
        let limits = state.limits_for(&tenant).clone();
        if let Err(error) = validate_ingest_body_limit(&limits, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_loki_http_push(&tenant, &headers, &body, &limits) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("krabka.ingest.lines", items);
                state.metrics.record_ingest_lines(tenant.as_str(), items);
                let resp = match append_distributor_wal_records(&state, &security, &tenant, records)
                    .await
                {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => error.into_response(),
                };
                return record_ingest_response(&state, resp, body_size, items, start);
            }
            Err(error) => error.into_response(),
        };
        record_ingest_response(&state, resp, body_size, 0, start)
    }
    .instrument(span)
    .await
}
