use super::*;

pub(crate) fn loki_parse_error(status: StatusCode, query: &str, source: &ParseError) -> Response {
    text_response(status, &loki_parse_error_text(query, source))
}
