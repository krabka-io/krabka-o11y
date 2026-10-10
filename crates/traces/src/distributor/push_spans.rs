use krabka_observability::service_metrics::IngestPushMeasurement;
use krabka_units::convert::ByteSizeExt as _;

use super::{
    ByteSize, Bytes, DistributorState, HeaderMap, HeaderValue, Principal, Response, Span,
    StatusCode, TENANT_HEADER, TracesError, append_decoded, decode_body, error_response,
    record_ingest_response, require_content_type,
};

/// One span push.
pub(crate) struct SpanPush<'a, D> {
    pub(crate) state: &'a DistributorState,
    pub(crate) principal: &'a Principal,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) body: &'a Bytes,
    /// The content types the endpoint accepts.
    pub(crate) content_types: &'a [&'a str],
    /// Decodes the decompressed body.
    pub(crate) decode: D,
}

/// Serve one span push: resolve the tenant, check the content type against
/// `content_types`, decode the body with `decode`, and append the spans.
///
/// The ingest outcome is recorded whichever step answers.
pub(crate) async fn push_spans<D>(push: SpanPush<'_, D>) -> Response
where
    D: FnOnce(&[u8]) -> Result<Vec<Span>, TracesError>,
{
    let SpanPush {
        state,
        principal,
        headers,
        body,
        content_types,
        decode,
    } = push;
    let start = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let measurement = IngestPushMeasurement::before_decode(body_size, start);
    let tenant = match state.resolve_tenant(
        principal,
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
    ) {
        Ok(tenant) => tenant,
        Err(err) => {
            return record_ingest_response(state, error_response(&err), measurement);
        }
    };
    if let Err(err) = require_content_type(headers, content_types) {
        return record_ingest_response(state, error_response(&err), measurement);
    }
    match decode_body(headers, body, state.max_decompressed).and_then(|body| decode(&body)) {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(state, &tenant, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(state, resp, measurement.with_items(items))
        }
        Err(err) => record_ingest_response(state, error_response(&err), measurement),
    }
}
