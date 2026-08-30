use super::{ParseQueryParams, Response, parse_promql, success_data_response, IntoResponse, ApiError};

pub(crate) fn format_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => success_data_response(expr.to_string()),
        Err(error) => ApiError::from(error).into_response(),
    }
}
