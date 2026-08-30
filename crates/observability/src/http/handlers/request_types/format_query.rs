use super::*;

pub(crate) async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match execute_format_query(raw_query.as_deref()) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}
