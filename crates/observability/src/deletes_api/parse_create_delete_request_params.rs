use super::*;

pub(crate) fn parse_create_delete_request_params(
    raw_query: Option<&str>,
) -> Result<CreateDeleteRequestParams, HttpQueryError> {
    let mut query = None;
    let mut start_time = None;
    let mut end_time = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(raw_query, &["query", "start", "end", "max_interval"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;
        match key.as_str() {
            "query" => query = Some(value),
            "start" => start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?),
            "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
            "max_interval" => {
                parse_loki_duration_query_param("max_interval", &value)?;
            }
            _ => {}
        }
    }

    let start_time = start_time.ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end_time = end_time.unwrap_or_else(|| current_unix_time_ns() / 1_000_000_000);
    if end_time < start_time {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "end",
            value: "end must be greater than or equal to start".to_string(),
        });
    }

    Ok(CreateDeleteRequestParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start_time,
        end_time,
    })
}
