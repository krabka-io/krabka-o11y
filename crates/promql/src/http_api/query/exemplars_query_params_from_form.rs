use super::{ApiError, ExemplarsQueryParams, form_urlencoded, required_form_param};

pub(crate) fn exemplars_query_params_from_form(
    body: &[u8],
) -> Result<ExemplarsQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(ExemplarsQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
    })
}
