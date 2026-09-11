use super::{Error, IntoResponse, Response, StatusCode};

/// A request reached a handler that needs a principal, but it carries none.
///
/// The authentication layer gives every request a principal, except on a
/// route that skips authentication. So a handler that needs a principal on
/// such a route answers 500, and a route put on that list by mistake fails
/// closed. A service that reads a credentials file also answers 500 for a
/// request that came through no listener, so a router served without the
/// authentication layer fails closed too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the request has no principal from the authentication layer")]
pub(crate) struct MissingPrincipal;

impl IntoResponse for MissingPrincipal {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("{self}\n"),
        )
            .into_response()
    }
}
