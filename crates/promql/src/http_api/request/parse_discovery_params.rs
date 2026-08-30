use super::{ApiError, DiscoveryParams, form_urlencoded};

pub(crate) fn parse_discovery_params(raw_query: Option<&str>) -> Result<DiscoveryParams, ApiError> {
    let mut params = DiscoveryParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "match[]" => params.matches.push(value.into_owned()),
            "start" => params.start = Some(value.into_owned()),
            "end" => params.end = Some(value.into_owned()),
            "limit" => {
                params.limit = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid limit parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}
