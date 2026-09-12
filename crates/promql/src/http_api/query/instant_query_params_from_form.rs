use super::{
    ApiError, InstantQueryParams, form_urlencoded, parse_limit_parameter, required_form_param,
};

pub(crate) fn instant_query_params_from_form(body: &[u8]) -> Result<InstantQueryParams, ApiError> {
    let mut query = None;
    let mut time = None;
    let mut limit = None;
    let mut timeout = None;
    let mut stats = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "time" => time = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            "timeout" => timeout = Some(value.into_owned()),
            "stats" => stats = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(InstantQueryParams {
        query: required_form_param(query, "query")?,
        time,
        limit,
        timeout,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timeout_and_stats() {
        let params = instant_query_params_from_form(b"query=up&timeout=250ms&stats=all").unwrap();
        assert2::assert!(params.timeout.as_deref() == Some("250ms"));
        assert2::assert!(params.stats.as_deref() == Some("all"));
    }
}
