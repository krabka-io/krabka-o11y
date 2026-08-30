use super::*;

pub(crate) async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    // One ingest span per request (NOT per span-record). The accepted-span count
    // is only known after decode, so it is declared `Empty` and recorded below;
    // this span becomes the local parent whose context is injected onto each WAL
    // record in `KafkaSink::append`, continuing the trace into the block-builder.
    let span = tracing::info_span!(
        "traces_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = TRACES_WAL_TOPIC,
        krabka.tenant = %tenant(&headers),
        krabka.ingest.spans = tracing::field::Empty,
        // A span field is a raw-number seam: emit the byte count itself.
        krabka.ingest.bytes = body_size.bytes_u64(),
    );
    async move {
        if let Err(err) = require_content_type(
            &headers,
            &["application/x-protobuf", "application/protobuf"],
        ) {
            return record_ingest_response(&state, error_response(&err), body_size, 0, start);
        }
        match decode_body(&headers, &body, state.max_decompressed)
            .and_then(|body| {
                TracesData::decode(body.as_slice())
                    .map_err(|err| TracesError::Decode(err.to_string()))
            })
            .and_then(|data| decode_otlp(&data))
        {
            Ok(spans) => {
                let items = spans.len() as u64;
                tracing::Span::current().record("krabka.ingest.spans", items);
                let resp =
                    append_decoded_response(&state, &headers, spans, otlp_success_response()).await;
                record_ingest_response(&state, resp, body_size, items, start)
            }
            Err(err) => record_ingest_response(&state, error_response(&err), body_size, 0, start),
        }
    }
    .instrument(span)
    .await
}
