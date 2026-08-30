use super::*;

pub(crate) async fn push_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_size = measured_size(body.len());
    let tenant = ingest_tenant(&headers);
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
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_loki_http_push(
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
