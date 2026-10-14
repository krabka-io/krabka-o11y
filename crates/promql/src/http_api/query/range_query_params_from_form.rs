use super::{ApiError, RangeQueryParams, parse_limit_parameter, query_range_form_fields};

pub(crate) fn range_query_params_from_form(body: &[u8]) -> Result<RangeQueryParams, ApiError> {
    let mut step = None;
    let mut limit = None;
    let mut timeout = None;
    let mut stats = None;
    let fields = query_range_form_fields(body, |name, value| {
        match name {
            "step" => step = Some(value.to_owned()),
            "limit" => limit = Some(parse_limit_parameter(value)?),
            "timeout" => timeout = Some(value.to_owned()),
            "stats" => stats = Some(value.to_owned()),
            _ => {}
        }
        Ok(())
    })?;
    Ok(RangeQueryParams {
        query: fields.query,
        start: fields.start,
        end: fields.end,
        step: super::required_form_param(step, "step")?,
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
        let params =
            range_query_params_from_form(b"query=up&start=0&end=1&step=1&timeout=250ms&stats=all")
                .unwrap();
        assert2::assert!(params.timeout.as_deref() == Some("250ms"));
        assert2::assert!(params.stats.as_deref() == Some("all"));
    }
}
