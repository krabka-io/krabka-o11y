use super::{
    CreateDeleteRequestParams, HttpQueryError, current_unix_time_ns,
    parse_loki_delete_timestamp_query_param, parse_loki_duration_query_param,
};
use crate::http::params::query_parsing::decoded_query_param_pairs;

pub(crate) fn parse_create_delete_request_params(
    raw_query: Option<&str>,
) -> Result<CreateDeleteRequestParams, HttpQueryError> {
    let mut query = None;
    let mut start_time = None;
    let mut end_time = None;
    for pair in decoded_query_param_pairs(raw_query, &["query", "start", "end", "max_interval"])? {
        match pair.key.as_str() {
            "query" => query = Some(pair.value),
            "start" => {
                start_time = Some(parse_loki_delete_timestamp_query_param(
                    "start",
                    &pair.value,
                )?);
            }
            "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &pair.value)?),
            "max_interval" => {
                parse_loki_duration_query_param("max_interval", &pair.value)?;
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
