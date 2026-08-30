use super::{ApiError, TsdbStatusParams, form_urlencoded, parse_limit_parameter};

pub(crate) fn parse_tsdb_status_params(
    raw_query: Option<&str>,
) -> Result<TsdbStatusParams, ApiError> {
    let mut params = TsdbStatusParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        if name == "limit" {
            params.limit = Some(parse_limit_parameter(&value)?);
        }
    }
    Ok(params)
}
