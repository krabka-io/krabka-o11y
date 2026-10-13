use super::{
    HttpQueryError, PatternsParams, decoded_query_param_pairs, parse_loki_duration_query_param,
    parse_loki_timestamp_query_param,
};

pub(crate) fn parse_patterns_params(
    raw_query: Option<&str>,
) -> Result<PatternsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    for pair in decoded_query_param_pairs(raw_query, &["query", "start", "end", "step"])? {
        match pair.key.as_str() {
            "query" => query = Some(pair.value),
            "start" => start = Some(parse_loki_timestamp_query_param("start", &pair.value)?),
            "end" => end = Some(parse_loki_timestamp_query_param("end", &pair.value)?),
            "step" => step = Some(parse_loki_duration_query_param("step", &pair.value)?),
            _ => {}
        }
    }

    Ok(PatternsParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start: start.ok_or(HttpQueryError::MissingQueryParameter("start"))?,
        end: end.ok_or(HttpQueryError::MissingQueryParameter("end"))?,
        step: step.unwrap_or(1_000_000_000),
    })
}
