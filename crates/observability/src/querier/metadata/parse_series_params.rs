use super::*;

pub(crate) fn parse_series_params(raw_query: Option<&str>) -> Result<SeriesParams, HttpQueryError> {
    let mut params = SeriesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };

    for pair in split_query_param_pairs(
        raw_query,
        &["match[]", "match%5B%5D", "query", "start", "end", "since"],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "match[]" | "query" => params.matchers.push(value),
            "start" if params.start.is_none() => {
                params.start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if params.end.is_none() => {
                params.end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if params.since.is_none() => {
                params.since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            _ => {}
        }
    }

    Ok(params)
}
