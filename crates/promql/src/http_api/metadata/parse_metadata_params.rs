use super::{ApiError, MetadataParams, form_urlencoded, parse_limit_parameter};

pub(crate) fn parse_metadata_params(raw_query: Option<&str>) -> Result<MetadataParams, ApiError> {
    let mut params = MetadataParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for pair in form_urlencoded::parse(raw_query.as_bytes()) {
        let name = pair.0;
        let value = pair.1;
        match name.as_ref() {
            "metric" => params.metric = Some(value.into_owned()),
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            "limit_per_metric" => params.limit_per_metric = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}
