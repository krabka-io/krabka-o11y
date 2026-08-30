use super::*;

pub(crate) fn loki_success(data: impl serde::Serialize) -> Response {
    json_response(StatusCode::OK, &loki_success_value(data))
}
