use super::{Response, StatusCode, text_response};

pub(crate) fn missing_loki_rule_namespace_response(tenant: &str, namespace: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "error parsing /loki/rules/{tenant}/{namespace}: /loki/rules/{tenant}/{namespace}: open /loki/rules/{tenant}/{namespace}: no such file or directory\n"
        ),
    )
}
