use super::{Bytes, IntoResponse, Response, parse_query_inner, parse_query_params};

pub(crate) async fn parse_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}
