use super::*;

pub(crate) async fn format_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}
