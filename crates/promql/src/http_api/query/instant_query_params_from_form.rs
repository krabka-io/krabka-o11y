use super::{ApiError, InstantQueryParams, form_urlencoded, parse_limit_parameter, required_form_param};

pub(crate) fn instant_query_params_from_form(body: &[u8]) -> Result<InstantQueryParams, ApiError> {
    let mut query = None;
    let mut time = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "time" => time = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(InstantQueryParams {
        query: required_form_param(query, "query")?,
        time,
        limit,
    })
}
