use super::{
    HttpQueryError, PatternsParams, decode_form_component, parse_loki_duration_query_param,
    parse_loki_timestamp_query_param, split_query_param_pairs,
};

pub(crate) fn parse_patterns_params(
    raw_query: Option<&str>,
) -> Result<PatternsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(raw_query, &["query", "start", "end", "step"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" => query = Some(value),
            "start" => start = Some(parse_loki_timestamp_query_param("start", &value)?),
            "end" => end = Some(parse_loki_timestamp_query_param("end", &value)?),
            "step" => step = Some(parse_loki_duration_query_param("step", &value)?),
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
