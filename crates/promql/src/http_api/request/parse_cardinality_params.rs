use super::{ApiError, CardinalityParams, parse_limit_parameter};
use crate::http_api::parse_query_pairs;

pub(crate) fn parse_cardinality_params(
    raw_query: Option<&str>,
) -> Result<CardinalityParams, ApiError> {
    let mut params = CardinalityParams::default();
    parse_query_pairs(raw_query, |name, value| {
        match name.as_str() {
            "selector" => params.selector = Some(value),
            "label_names[]" => params.label_names.push(value),
            "count_method" => match value.as_str() {
                "inmemory" | "active" => {}
                _ => return Err(ApiError::bad_data("invalid count_method parameter")),
            },
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
        Ok(())
    })?;
    Ok(params)
}
