use super::{LogLevelControl, Response, StatusCode, json, json_response};

/// `GET /log_level`, reporting the level this process is really filtering at.
///
/// It used to answer `Current log level is info` whatever the process was
/// doing, so the one page that could have told an operator their `RUST_LOG`
/// had not taken agreed with them that it had.
pub(crate) async fn log_level() -> Response {
    json_response(
        StatusCode::OK,
        &json!({
            "message": format!("Current log level is {}", LogLevelControl::process().level()),
        }),
    )
}
