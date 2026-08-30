use super::*;

pub(crate) fn limit_error_response(err: &LimitError) -> Response {
    tempo_limit_error_response(err)
}
