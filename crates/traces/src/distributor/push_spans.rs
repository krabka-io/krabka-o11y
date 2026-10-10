use krabka_units::convert::ByteSizeExt as _;

use super::{
    ByteSize, Bytes, DistributorState, HeaderMap, HeaderValue, Principal, Response, Span,
    StatusCode, TENANT_HEADER, TracesError, append_decoded, decode_body, error_response,
    record_ingest_response, require_content_type,
};

/// Serve one span push: resolve the tenant, check the content type against
/// `content_types`, decode the body with `decode`, and append the spans.
///
/// The ingest outcome is recorded whichever step answers.
pub(crate) async fn push_spans(
    state: &DistributorState,
    principal: &Principal,
    headers: &HeaderMap,
    body: &Bytes,
    content_types: &[&str],
    decode: impl FnOnce(&[u8]) -> Result<Vec<Span>, TracesError>,
) -> Response {
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let tenant = match state.resolve_tenant(
        principal,
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
    ) {
        Ok(tenant) => tenant,
        Err(err) => {
            return record_ingest_response(state, error_response(&err), body_size, 0, start);
        }
    };
    if let Err(err) = require_content_type(headers, content_types) {
        return record_ingest_response(state, error_response(&err), body_size, 0, start);
    }
    match decode_body(headers, body, state.max_decompressed).and_then(|body| decode(&body)) {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(state, &tenant, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(state, resp, body_size, items, start)
        }
        Err(err) => record_ingest_response(state, error_response(&err), body_size, 0, start),
    }
}
