use super::{
    HttpQueryError, SeriesParams, decode_query_pair, parse_loki_duration_query_param,
    parse_loki_timestamp_query_param, split_query_param_pairs,
};

pub(crate) fn parse_series_params(raw_query: Option<&str>) -> Result<SeriesParams, HttpQueryError> {
    let mut params = SeriesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };

    for pair in split_query_param_pairs(
        raw_query,
        &["match[]", "match%5B%5D", "query", "start", "end", "since"],
    ) {
        let pair = decode_query_pair(pair)?;

        match pair.key.as_str() {
            "match[]" | "query" => params.matchers.push(pair.value),
            "start" if params.start.is_none() => {
                params.start = Some(parse_loki_timestamp_query_param("start", &pair.value)?);
            }
            "end" if params.end.is_none() => {
                params.end = Some(parse_loki_timestamp_query_param("end", &pair.value)?);
            }
            "since" if params.since.is_none() => {
                params.since = Some(parse_loki_duration_query_param("since", &pair.value)?);
            }
            _ => {}
        }
    }

    Ok(params)
}
