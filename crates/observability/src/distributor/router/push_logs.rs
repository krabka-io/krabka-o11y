use super::{
    Bytes, DistributorState, HeaderMap, IngestPushMeasurement, Instant, Instrument, IntoResponse,
    NormalizedPush, RequestSecurity, Response, State, TenantErrorSurface, append_and_record_push,
    logs_ingest_span, measured_size, normalize_loki_http_push, record_ingest_response,
    resolve_single_tenant, tenant_error_response, tenant_header_value, validate_ingest_body_limit,
};

pub(crate) async fn push_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_logs_with_normalizer(
        state,
        security,
        headers,
        body,
        TenantErrorSurface::Push,
        normalize_loki_http_push,
        |error, _, _| error.into_response(),
        |_| {},
    )
    .await
}

// Keep the normalizer and response callbacks injectable for focused handler tests.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn push_logs_with_normalizer<E>(
    state: DistributorState,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
    error_surface: TenantErrorSurface,
    normalize: impl FnOnce(
        &super::TenantId,
        &HeaderMap,
        &[u8],
        &super::Limits,
    ) -> Result<Vec<crate::compactor::WalLogRecord>, E>,
    decode_error: impl FnOnce(E, &HeaderMap, super::ByteSize) -> Response,
    append_error: impl FnOnce(&crate::DistributorError),
) -> Response {
    let start = Instant::now();
    let body_size = measured_size(body.len());
    let measurement = IngestPushMeasurement::before_decode(body_size, start);
    // The tenant is resolved before anything reads the body, so a malformed
    // tenant never picks limits and never reaches the WAL.
    let tenant = match resolve_single_tenant(tenant_header_value(&headers)) {
        Ok(tenant) => tenant,
        Err(error) => {
            let response = tenant_error_response(&error, error_surface);
            return record_ingest_response(&state, response, measurement);
        }
    };
    if let Err(denied) = security.authorize_tenant(&tenant) {
        return record_ingest_response(&state, denied.into_response(), measurement);
    }
    // ONE server span per push request (not per log line): wraps the whole
    // ingest body so the produce-side WAL append (which injects `traceparent`)
    // and downstream compaction stitch onto this trace. `krabka.ingest.lines`
    // is unknown until normalization, so it is recorded on the span below.
    let span = logs_ingest_span(&tenant, body_size);
    async move {
        // One resolution for the whole push: the body cap, the line cap, the
        // label caps and the two timestamp windows all come from this set.
        let limits = state.limits_for(&tenant).clone();
        if let Err(error) = validate_ingest_body_limit(&limits, body_size) {
            return record_ingest_response(&state, error.into_response(), measurement);
        }
        let resp = match normalize(&tenant, &headers, body.as_ref(), &limits) {
            Ok(records) => {
                return append_and_record_push(
                    NormalizedPush {
                        state: &state,
                        security: &security,
                        tenant: &tenant,
                        measurement,
                    },
                    records,
                    append_error,
                )
                .await;
            }
            Err(error) => decode_error(error, &headers, body_size),
        };
        record_ingest_response(&state, resp, measurement)
    }
    .instrument(span)
    .await
}
