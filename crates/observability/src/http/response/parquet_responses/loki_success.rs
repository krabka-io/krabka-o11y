use super::{Response, StatusCode, json_response, loki_success_value};

pub(crate) fn loki_success(data: impl serde::Serialize) -> Response {
    json_response(StatusCode::OK, &loki_success_value(data))
}
