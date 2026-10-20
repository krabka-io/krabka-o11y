use axum::{
    http::{StatusCode, header},
    response::Response,
};

/// The 401 that every authentication failure gets.
///
/// Every failure gets the same headers and the same body, so the answer does
/// not say whether the token, the username, or the certificate was wrong.
pub fn unauthorized_response() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            header::WWW_AUTHENTICATE,
            r#"Basic realm="krabka", Bearer realm="krabka""#,
        )
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body("unauthorized\n".into())
        .expect("unauthorized response headers are valid")
}
