use super::{ApiError, IntoResponse, ParseQueryParams, Response, success_data_response};

pub(crate) fn parse_query_inner(params: &ParseQueryParams) -> Response {
    match crate::serialize_promql_query(&params.query) {
        Ok(value) => success_data_response(value),
        Err(error) => ApiError::from(error).into_response(),
    }
}
