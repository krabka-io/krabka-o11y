use super::{ApiError, CardinalityParams, form_urlencoded, parse_limit_parameter};

pub(crate) fn parse_cardinality_params(
    raw_query: Option<&str>,
) -> Result<CardinalityParams, ApiError> {
    let mut params = CardinalityParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "selector" => params.selector = Some(value.into_owned()),
            "label_names[]" => params.label_names.push(value.into_owned()),
            "count_method" => match value.as_ref() {
                "inmemory" | "active" => {}
                _ => return Err(ApiError::bad_data("invalid count_method parameter")),
            },
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}
