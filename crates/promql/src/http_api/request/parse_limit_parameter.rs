use super::ApiError;

pub(crate) fn parse_limit_parameter(value: &str) -> Result<usize, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data("invalid limit parameter"))
}
