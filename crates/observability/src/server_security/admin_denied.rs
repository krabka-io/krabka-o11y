use std::sync::Arc;

use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

/// An authenticated principal without `admin: true` asked for an admin operation.
///
/// The response is a 403 with a plain-text body that names the principal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("principal {principal:?} is not allowed to call admin operations")]
pub struct AdminDenied {
    /// The principal's name.
    pub principal: Arc<str>,
}

impl IntoResponse for AdminDenied {
    fn into_response(self) -> Response {
        (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            format!("{self}\n"),
        )
            .into_response()
    }
}
