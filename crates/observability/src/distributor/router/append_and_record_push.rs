use super::{
    DistributorState, Instant, IntoResponse, RequestSecurity, Response, StatusCode, TenantId,
    append_distributor_wal_records, record_ingest_response,
};
use crate::{ByteSize, DistributorError, WalLogRecord};

/// One authorized push request whose body the distributor has normalized.
pub(crate) struct NormalizedPush<'a> {
    pub(crate) state: &'a DistributorState,
    pub(crate) security: &'a RequestSecurity,
    pub(crate) tenant: &'a TenantId,
    pub(crate) body_size: ByteSize,
    /// When the distributor started handling the request.
    pub(crate) start: Instant,
}

/// Appends a normalized push's `records` to the WAL and records the response,
/// with the push's line count, on the ingest metrics and the request span.
///
/// `on_append_error` sees a WAL append failure before it becomes the response.
pub(crate) async fn append_and_record_push(
    push: NormalizedPush<'_>,
    records: Vec<WalLogRecord>,
    on_append_error: impl FnOnce(&DistributorError),
) -> Response {
    let NormalizedPush {
        state,
        security,
        tenant,
        body_size,
        start,
    } = push;
    let items = records.len() as u64;
    tracing::Span::current().record("krabka.ingest.lines", items);
    state.metrics.record_ingest_lines(tenant.as_str(), items);
    let resp = match append_distributor_wal_records(state, security, tenant, records).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            on_append_error(&error);
            error.into_response()
        }
    };
    record_ingest_response(state, resp, body_size, items, start)
}
