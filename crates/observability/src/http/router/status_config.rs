use super::*;

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

    (
        StatusCode::OK,
        [("content-type", "application/yaml; charset=utf-8")],
        format!("target: {LOKI_CONFIG_TARGET}\n"),
    )
        .into_response()
}
