use super::{ParseQueryParams, Response, parse_promql, success_data_response, IntoResponse, ApiError};

pub(crate) fn parse_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => match serde_json::to_value(expr) {
            Ok(value) => success_data_response(value),
            Err(error) => ApiError::internal(format!("PromQL AST serialization failed: {error}"))
                .into_response(),
        },
        Err(error) => ApiError::from(error).into_response(),
    }
}
