use super::*;

pub(crate) async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}
