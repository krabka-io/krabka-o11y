use super::*;

pub(crate) async fn zipkin_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    if let Err(err) = require_content_type(&headers, &["application/json"]) {
        return record_ingest_response(&state, error_response(&err), body_size, 0, start);
    }
    match decode_body(&headers, &body, state.max_decompressed).and_then(|body| decode_zipkin(&body))
    {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(&state, &headers, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(&state, resp, body_size, items, start)
        }
        Err(err) => record_ingest_response(&state, error_response(&err), body_size, 0, start),
    }
}
