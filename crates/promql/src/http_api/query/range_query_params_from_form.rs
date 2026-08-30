use super::{
    ApiError, RangeQueryParams, form_urlencoded, parse_limit_parameter, required_form_param,
};

pub(crate) fn range_query_params_from_form(body: &[u8]) -> Result<RangeQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            "step" => step = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(RangeQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
        step: required_form_param(step, "step")?,
        limit,
    })
}
