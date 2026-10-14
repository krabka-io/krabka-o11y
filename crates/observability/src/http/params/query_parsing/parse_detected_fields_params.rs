use super::{
    DetectedFieldsParams, HttpQueryError, LOKI_DEFAULT_QUERY_RANGE, TimeExt, current_unix_time_ns,
    decode_query_pair, parse_loki_duration_query_param, parse_loki_timestamp_query_param,
    parse_usize_query_param, set_first_query_param, split_query_param_pairs, start_or_since,
};

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
        let pair = decode_query_pair(pair)?;

        match pair.key.as_str() {
            "query" => set_first_query_param(&mut query, || Ok(pair.value))?,
            "start" => set_first_query_param(&mut start, || {
                parse_loki_timestamp_query_param("start", &pair.value)
            })?,
            "end" => {
                set_first_query_param(&mut end, || {
                    parse_loki_timestamp_query_param("end", &pair.value)
                })?;
            }
            "since" => set_first_query_param(&mut since, || {
                parse_loki_duration_query_param("since", &pair.value)
            })?,
            "step" => set_first_query_param(&mut step, || {
                parse_loki_duration_query_param("step", &pair.value)
            })?,
            "limit" => {
                set_first_query_param(&mut limit, || {
                    parse_usize_query_param("limit", &pair.value)
                })?;
            }
            "field_limit" => set_first_query_param(&mut limit, || {
                parse_usize_query_param("field_limit", &pair.value)
            })?,
            "line_limit" => set_first_query_param(&mut line_limit, || {
                parse_usize_query_param("line_limit", &pair.value)
            })?,
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
