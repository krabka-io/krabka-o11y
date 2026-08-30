use super::{
    ByteSizeExt, Bytes, CONTENT_ENCODING, CONTENT_TYPE, DistributorState, HeaderMap, Instant,
    Instrument, IntoResponse, Response, State, StatusCode, append_distributor_wal_records,
    ingest_tenant, measured_size, normalize_otlp_http_logs, otlp_http_error_response,
    record_ingest_response, validate_ingest_body_limit,
};

pub(crate) async fn push_otlp_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_size = measured_size(body.len());
    let tenant = ingest_tenant(&headers);
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
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_otlp_http_logs(
            &headers,
            &body,
            state.reject_old_samples_max_age,
            state.creation_grace_period,
        ) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("krabka.ingest.lines", items);
                state.metrics.record_ingest_lines(&tenant, items);
                let resp = match append_distributor_wal_records(&state, records).await {
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
