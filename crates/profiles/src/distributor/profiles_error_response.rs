use super::{IntoResponse, ProfilesError, Response, StatusCode, client_facing_message};

pub(crate) fn profiles_error_response(err: ProfilesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        ProfilesError::TenantDenied(denied) => denied.into_response(),
        ProfilesError::Limit(limit) => {
            let mut response = axum::Json(serde_json::json!({
                "code": limit.connect_code(),
                "message": limit.message(),
            }))
            .into_response();
            *response.status_mut() = status;
            response
        }
        other => {
            let message = client_facing_message(&other);
            Response::builder()
                .status(status)
                .body(axum::body::Body::from(message))
                .expect("valid profiles error response")
        }
    }
}
