use super::*;

pub(crate) fn connect_error(err: ProfileError) -> ConnectError {
    let code = match &err {
        ProfileError::Decode(_) | ProfileError::Plan(_) | ProfileError::Unsupported(_) => {
            Code::InvalidArgument
        }
        ProfileError::Overloaded { .. } => Code::ResourceExhausted,
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            Code::Internal
        }
    };
    let message = err.to_string();
    drop(err);
    ConnectError::new(code, message)
}

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
        ProfileError::Overloaded { .. } => StatusCode::TOO_MANY_REQUESTS,
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            tracing::error!(%err, "profiles querier internal error");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    let message = if matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::TOO_MANY_REQUESTS
    ) {
        err.to_string()
    } else {
        "internal error".to_string()
    };
    drop(err);
    Response::builder()
        .status(status)
        .body(axum::body::Body::from(message))
        .expect("valid profile error response")
}

/// Maps a tenant that does not resolve to the HTTP response of a legacy route.
///
/// This is the plain HTTP form of [`super::tenant_connect_error`], and the
/// same reason applies: Pyroscope with multi-tenancy off does not read
/// `X-Scope-OrgID`, but Krabka isolates by tenant and so rejects a malformed
/// name. The status is 400. The body is the `dskit` message as
/// `text/plain; charset=utf-8`, which is the content type that the pinned
/// Mimir and Loki images send with a tenant error.
pub(crate) fn tenant_error_response(error: &TenantResolveError) -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )
        .body(axum::body::Body::from(error.to_string()))
        .expect("valid tenant error response")
}
