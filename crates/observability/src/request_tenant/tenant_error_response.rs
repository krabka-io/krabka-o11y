use super::{
    IntoResponse, Response, StatusCode, TenantErrorSurface, TenantRequestError, TenantResolveError,
    encode_otlp_status_message,
};

/// The response Loki 3.5.1 gives a tenant error on `surface`.
///
/// Every row is a status, a content type and a body captured from the pinned
/// Loki image. Several of them are a 500 for what is a client error. That is
/// what upstream sends, so Krabka sends it too.
pub(crate) fn tenant_error_response(
    error: &TenantRequestError,
    surface: TenantErrorSurface,
) -> Response {
    if matches!(
        error,
        TenantRequestError::Resolve(TenantResolveError::Missing)
    ) {
        // `dskit`'s auth middleware, through Go's `http.Error`.
        return plain_text_error(StatusCode::UNAUTHORIZED, &format!("{error}\n"));
    }
    let multiple = matches!(error, TenantRequestError::MultipleOrgIds);
    match surface {
        TenantErrorSurface::Push => {
            plain_text_error(StatusCode::BAD_REQUEST, &format!("{error}\n"))
        }
        TenantErrorSurface::OtlpPush => (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/octet-stream")],
            encode_otlp_status_message(&error.to_string()),
        )
            .into_response(),
        TenantErrorSurface::Read | TenantErrorSurface::QuerierRead if multiple => {
            plain_text_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
        }
        TenantErrorSurface::Read | TenantErrorSurface::Tail => {
            plain_text_error(StatusCode::BAD_REQUEST, &error.to_string())
        }
        TenantErrorSurface::QuerierRead => plain_text_error(
            StatusCode::BAD_REQUEST,
            &format!("rpc error: code = Code(400) desc = {error}"),
        ),
        TenantErrorSurface::Patterns if multiple => plain_text_error(StatusCode::NOT_FOUND, ""),
        TenantErrorSurface::InstantLogQuery | TenantErrorSurface::Patterns => {
            plain_text_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
        }
        TenantErrorSurface::Ruler => ruler_error("no org id"),
        TenantErrorSurface::PrometheusRuler => ruler_error("no valid org id found"),
    }
}

// The ruler writes its JSON error body itself, not through `http.Error`. So the
// body is JSON, the content type is plain text with no line break, and there
// is no `X-Content-Type-Options` header. The pinned Loki image answers every
// ruler route that way for a malformed or multiple tenant, including the
// Prometheus-compatible `/prometheus/api/v1/rules` and `/alerts`.
fn ruler_error(message: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [("content-type", "text/plain; charset=utf-8")],
        format!(
            r#"{{"status":"error","data":null,"errorType":"server_error","error":"{message}"}}"#
        ),
    )
        .into_response()
}

// Go's `http.Error` sets `X-Content-Type-Options: nosniff` next to the
// plain-text content type, so a browser does not read the body as HTML. Loki
// answers its tenant errors through it, and `krabka-metrics` sends the same
// header for the same reason.
fn plain_text_error(status: StatusCode, body: &str) -> Response {
    (
        status,
        [
            ("content-type", "text/plain; charset=utf-8"),
            ("x-content-type-options", "nosniff"),
        ],
        body.to_owned(),
    )
        .into_response()
}
