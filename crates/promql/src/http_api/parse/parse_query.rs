use super::{RawQuery, Response, parse_query_params, parse_query_inner, IntoResponse};

pub(crate) async fn parse_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}
