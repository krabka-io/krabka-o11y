use super::{
    Bytes, HttpQueryError, IntoResponse, LogLevelControl, LogLevelError, RawQuery, Response,
    StatusCode, json, json_response, log_level_failed_response, requested_log_level,
};

/// `POST /log_level`, moving this process's filter.
///
/// The reply follows what happened. A success means the filter moved and the
/// next line at that level is emitted; a process whose logging was installed
/// without a reload handle answers 501 and names the variable that does set
/// its level, because an operator who is told `success` and then sees no new
/// output goes looking for the bug somewhere else entirely.
pub(crate) async fn log_level_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    match requested_log_level(raw_query.as_deref(), &body) {
        Ok(level) => match LogLevelControl::process().set_level(&level) {
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
        },
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
