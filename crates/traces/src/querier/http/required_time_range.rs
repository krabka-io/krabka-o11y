use super::{Response, StatusCode, TimeBounds, Uri, required_seconds_param, status_text_response};

/// The required `start` and `end` of a request, in nanoseconds, or the 400
/// that rejects a missing, malformed, or reversed range.
pub(crate) fn required_time_range(uri: &Uri) -> Result<TimeBounds, Box<Response>> {
    let start_ns = required_seconds_param(uri, "start")
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    let end_ns = required_seconds_param(uri, "end")
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    if end_ns < start_ns {
        return Err(Box::new(status_text_response(
            StatusCode::BAD_REQUEST,
            "end must be >= start",
        )));
    }
    Ok(TimeBounds { start_ns, end_ns })
}
