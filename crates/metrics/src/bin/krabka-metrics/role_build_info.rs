use super::*;

pub(crate) fn role_build_info(role: &'static str) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": {
                "role": role,
                "version": env!("CARGO_PKG_VERSION"),
                "revision": "unknown",
                "branch": "unknown",
                "buildUser": "krabka",
                "buildDate": "unknown",
                "goVersion": "n/a"
            }
        })),
    )
}
