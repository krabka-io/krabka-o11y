use super::*;

pub(crate) async fn jaeger_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let tenant = match state.resolve_tenant(
        &principal,
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
    ) {
        Ok(tenant) => tenant,
        Err(err) => {
            return record_ingest_response(&state, error_response(&err), body_size, 0, start);
        }
    };
    if let Err(err) = require_content_type(
        &headers,
        &[
            "application/x-thrift",
            "application/octet-stream",
            "application/vnd.apache.thrift.binary",
        ],
    ) {
        return record_ingest_response(&state, error_response(&err), body_size, 0, start);
    }
    match decode_body(&headers, &body, state.max_decompressed).and_then(|body| {
        if is_jaeger_binary_thrift(&headers) {
            decode_jaeger_binary_thrift(&body)
        } else {
            decode_jaeger_thrift(&body)
        }
    }) {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(&state, &tenant, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(&state, resp, body_size, items, start)
        }
        Err(err) => record_ingest_response(&state, error_response(&err), body_size, 0, start),
    }
}
