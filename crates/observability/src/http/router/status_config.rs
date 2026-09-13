use super::{IntoResponse, LOKI_CONFIG_TARGET, Response, StatusCode, query_param_value};

pub(crate) fn status_config(raw_query: Option<&str>) -> Response {
    match query_param_value(raw_query, "mode").as_deref() {
        Some("diff") => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain; charset=utf-8")],
                "unsupported type <nil>\n",
            )
                .into_response();
        }
        Some("defaults") => {
            return (
                StatusCode::OK,
                [("content-type", "application/yaml; charset=utf-8")],
                format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
            )
                .into_response();
        }
        _ => {}
    }

    // `auth_enabled: true` is what this process does: every read and every
    // push without `X-Scope-OrgID` is refused with `no org id`. Loki's
    // effective config names the setting, so a reader of this page can tell
    // a multi-tenant deployment from a single-tenant one.
    (
        StatusCode::OK,
        [("content-type", "application/yaml; charset=utf-8")],
        format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
    )
        .into_response()
}
