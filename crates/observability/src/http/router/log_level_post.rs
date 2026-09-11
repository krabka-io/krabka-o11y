use super::{
    AuditOutcome, Bytes, HttpQueryError, IntoResponse, LogLevelControl, LogLevelError,
    OPERATION_LOG_LEVEL_SET, RESOURCE_LOG_LEVEL, RawQuery, RequestSecurity, Response, StatusCode,
    json, json_response, log_level_failed_response, requested_log_level, resource,
};

/// `POST /log_level`, moving this process's filter.
///
/// The reply follows what happened. A success means the filter moved and the
/// next line at that level is emitted; a process whose logging was installed
/// without a reload handle answers 501 and names the variable that does set
/// its level, because an operator who is told `success` and then sees no new
/// output goes looking for the bug somewhere else entirely.
///
/// An authenticated principal needs `admin: true`. Any other authenticated
/// principal gets 403 before the request is read. The audit trail records each
/// attempt to set a valid level, with its outcome.
pub(crate) async fn log_level_post(
    security: RequestSecurity,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    match requested_log_level(raw_query.as_deref(), &body) {
        Ok(level) => {
            let result = LogLevelControl::process().set_level(&level);
            security.admin_operation(
                OPERATION_LOG_LEVEL_SET,
                vec![resource(RESOURCE_LOG_LEVEL, level.as_str())],
                if result.is_ok() {
                    AuditOutcome::Success
                } else {
                    AuditOutcome::Failure
                },
            );
            match result {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &json!({
                        "status": "success",
                        "message": format!("Log level set to {level}"),
                    }),
                ),
                Err(error @ LogLevelError::Fixed) => json_response(
                    StatusCode::NOT_IMPLEMENTED,
                    &json!({
                        "status": "failed",
                        "message": error.to_string(),
                    }),
                ),
                Err(error) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({
                        "status": "failed",
                        "message": error.to_string(),
                    }),
                ),
            }
        }
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            value,
        }) => log_level_failed_response(&format!("unrecognized log level \"{value}\"")),
        Err(HttpQueryError::MissingQueryParameter("log_level")) => {
            log_level_failed_response("unrecognized log level \"\"")
        }
        Err(error) => error.into_response(),
    }
}
