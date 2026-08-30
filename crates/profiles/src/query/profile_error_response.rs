use super::*;

/// Maps a [`ProfileError`] to a legacy flamebearer HTTP response.
///
/// The mapping matches the code mapping of [`connect_error`]. The client-shaped
/// errors are `Decode`, `Plan`, and `Unsupported`, and they include limit and
/// range violations that become `Plan`. These keep their user-facing message at
/// 400. The internal failures are `Exec`, `Store`, and `Symbolize`. These
/// return a generic 500 and log the detail with tracing, so raw `DataFusion` text
/// and other internal text never reaches the client.
pub(crate) fn profile_error_response(err: ProfileError) -> Response {
    let status = match &err {
        ProfileError::Decode(_) | ProfileError::Plan(_) | ProfileError::Unsupported(_) => {
            StatusCode::BAD_REQUEST
        }
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            tracing::error!(%err, "profiles querier internal error");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    let message = if status == StatusCode::BAD_REQUEST {
        err.to_string()
    } else {
        "internal error".to_string()
    };
    drop(err);
    (status, message).into_response()
}
