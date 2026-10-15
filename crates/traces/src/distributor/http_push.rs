use super::*;
use krabka_observability::service_metrics::IngestPushMeasurement;


pub(crate) async fn otlp_push(request: PushRequest) -> Response {
    let PushRequest {
        state,
        principal,
        headers,
        body,
    } = request;
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let measurement = IngestPushMeasurement::before_decode(body_size, start);
    // One ingest span per request (NOT per span-record). The tenant and the
    // accepted-span count are only known after resolve and decode, so both are
    // declared `Empty` and recorded below; this span becomes the local parent
    // whose context is injected onto each WAL record in `KafkaSink::append`,
    // continuing the trace into the block-builder.
    let span = tracing::info_span!(
        "traces_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = TRACES_WAL_TOPIC,
        krabka.tenant = tracing::field::Empty,
        krabka.ingest.spans = tracing::field::Empty,
        // A span field is a raw-number seam: emit the byte count itself.
        krabka.ingest.bytes = body_size.bytes_u64(),
    );
    async move {
        let tenant = match state.resolve_tenant(
            &principal,
            headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
        ) {
            Ok(tenant) => tenant,
            Err(err) => {
                return record_ingest_response(&state, error_response(&err), measurement);
            }
        };
        tracing::Span::current().record("krabka.tenant", tenant.as_str());
        if let Err(err) = require_content_type(
            &headers,
            &["application/x-protobuf", "application/protobuf"],
        ) {
            return record_ingest_response(&state, error_response(&err), measurement);
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
                    append_decoded_response(&state, &tenant, spans, otlp_success_response()).await;
                record_ingest_response(&state, resp, measurement.with_items(items))
            }
            Err(err) => record_ingest_response(&state, error_response(&err), measurement),
        }
    }
    .instrument(span)
    .await
}

pub(crate) async fn jaeger_push(request: PushRequest) -> Response {
    let PushRequest {
        state,
        principal,
        headers,
        body,
    } = request;
    let binary = is_jaeger_binary_thrift(&headers);
    push_spans(SpanPush {
        state: &state,
        principal: &principal,
        headers: &headers,
        body: &body,
        content_types: &[
            "application/x-thrift",
            "application/octet-stream",
            "application/vnd.apache.thrift.binary",
        ],
        decode: |body: &[u8]| {
            if binary {
                decode_jaeger_binary_thrift(body)
            } else {
                decode_jaeger_thrift(body)
            }
        },
    })
    .await
}


pub(crate) fn is_jaeger_binary_thrift(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|declared| declared.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/vnd.apache.thrift.binary")
        })
}

pub(crate) async fn zipkin_push(request: PushRequest) -> Response {
    let PushRequest {
        state,
        principal,
        headers,
        body,
    } = request;
    push_spans(SpanPush {
        state: &state,
        principal: &principal,
        headers: &headers,
        body: &body,
        content_types: &["application/json"],
        decode: decode_zipkin,
    })
    .await
}
