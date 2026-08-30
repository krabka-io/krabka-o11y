use super::*;

pub(crate) fn parse_detected_fields_params(
    raw_query: Option<&str>,
) -> Result<DetectedFieldsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
    let mut step = None;
    let mut limit = None;
    let mut line_limit = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "start",
            "end",
            "since",
            "step",
            "limit",
            "field_limit",
            "line_limit",
        ],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "start" if start.is_none() => {
                start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if end.is_none() => {
                end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if since.is_none() => {
                since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "field_limit" if limit.is_none() => {
                limit = Some(parse_usize_query_param("field_limit", &value)?);
            }
            "line_limit" if line_limit.is_none() => {
                line_limit = Some(parse_usize_query_param("line_limit", &value)?);
            }
            _ => {}
        }
    }

    if let Some(step) = step
        && step <= 0
    {
        return Err(HttpQueryError::InvalidStep);
    }
    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start_or_since(start, since, Some(end))?
        .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));

    Ok(DetectedFieldsParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start,
        end,
        limit: limit.unwrap_or(1000),
        line_limit: line_limit.unwrap_or(100),
    })
}
