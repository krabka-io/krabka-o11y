use super::{IntoResponse, ProfilesError, Response, StatusCode, client_facing_message};

pub(crate) fn profiles_error_response(err: ProfilesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        ProfilesError::Limit(limit) => (
            status,
            axum::Json(serde_json::json!({
                "code": limit.connect_code(),
                "message": limit.message(),
            })),
        )
            .into_response(),
        other => {
            let message = client_facing_message(&other);
            (status, message).into_response()
        }
    }
}
