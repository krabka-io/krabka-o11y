use super::{LOKI_CONFIG_TARGET, Response, StatusCode, query_param_value};

fn yaml_response(status: StatusCode, body: String, content_type: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(axum::body::Body::from(body))
        .expect("a static config response is valid")
}

pub(crate) fn status_config(raw_query: Option<&str>) -> Response {
    match query_param_value(raw_query, "mode").as_deref() {
        Some("diff") => {
            return yaml_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unsupported type <nil>\n".to_string(),
                "text/plain; charset=utf-8",
            );
        }
        Some("defaults") => {
            return yaml_response(
                StatusCode::OK,
                format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
                "application/yaml; charset=utf-8",
            );
        }
        _ => {}
    }

    // `auth_enabled: true` is what this process does: every read and every
    // push without `X-Scope-OrgID` is refused with `no org id`. Loki's
    // effective config names the setting, so a reader of this page can tell
    // a multi-tenant deployment from a single-tenant one.
    yaml_response(
        StatusCode::OK,
        format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
        "application/yaml; charset=utf-8",
    )
}
