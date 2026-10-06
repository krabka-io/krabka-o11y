use super::{ApiError, IntoResponse, ParseQueryParams, Response, success_data_response};

pub(crate) fn format_query_inner(params: &ParseQueryParams) -> Response {
    match crate::format_promql_query(&params.query) {
        Ok(formatted) => success_data_response(formatted),
        Err(error) => ApiError::from(error).into_response(),
    }
}
