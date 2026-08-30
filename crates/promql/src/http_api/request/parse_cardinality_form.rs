use super::{CardinalityParams, ApiError, parse_cardinality_params};

pub(crate) fn parse_cardinality_form(body: &[u8]) -> Result<CardinalityParams, ApiError> {
    parse_cardinality_params(std::str::from_utf8(body).ok())
}
