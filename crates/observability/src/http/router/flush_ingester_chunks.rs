use super::{
    AuditOutcome, IntoResponse, OPERATION_INGESTER_FLUSH, RequestSecurity, Response, StatusCode,
};

/// `POST /flush`: force buffered writes out to storage.
///
/// Krabka's distributor holds no chunk buffer. `POST /loki/api/v1/push` awaits
/// the WAL append and answers 204 only once the WAL has taken the records, so
/// at the moment this handler runs everything acknowledged is already durable
/// and there is nothing left to force. The empty 204 is `Loki`'s answer to a
/// flush with nothing to do, and here it is the answer every time.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403. The audit trail records each flush.
pub(crate) async fn flush_ingester_chunks(security: RequestSecurity) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    security.admin_operation(
        OPERATION_INGESTER_FLUSH,
        vec![security.ingester()],
        AuditOutcome::Success,
    );
    StatusCode::NO_CONTENT.into_response()
}
