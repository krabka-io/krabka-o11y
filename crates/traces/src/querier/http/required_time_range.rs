use super::{IntoResponse, Response, StatusCode, Uri, required_seconds_param};

/// The required `start` and `end` of a request, in nanoseconds, or the 400
/// that rejects a missing, malformed, or reversed range.
pub(crate) fn required_time_range(uri: &Uri) -> Result<(i64, i64), Box<Response>> {
    let start_ns = required_seconds_param(uri, "start")
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    let end_ns = required_seconds_param(uri, "end")
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    if end_ns < start_ns {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "end must be >= start").into_response(),
        ));
    }
    Ok((start_ns, end_ns))
}
