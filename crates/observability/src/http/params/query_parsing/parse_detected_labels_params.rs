use super::{
    DecodedQueryPair, DetectedLabelsParams, HttpQueryError, LOKI_DEFAULT_QUERY_RANGE,
    QueryWindowNanos, TimeExt, current_unix_time_ns, decode_query_pair, parse_usize_query_param,
    split_query_param_pairs, start_or_since,
};

pub(crate) fn parse_detected_labels_params(
    raw_query: Option<&str>,
) -> Result<DetectedLabelsParams, HttpQueryError> {
    let mut query = None;
    let mut window = QueryWindowNanos::default();
    let mut limit = None;

    if let Some(raw_query) = raw_query {
        for pair in split_query_param_pairs(
            raw_query,
            &[
                "query",
                "start",
                "end",
                "since",
                "limit",
                "field_limit",
                "step",
            ],
        ) {
            let pair = decode_query_pair(pair)?;
            window.record(&pair)?;
            let DecodedQueryPair { key, value } = pair;

            match key.as_str() {
                // Grafana's Logs Drilldown sends `detected_labels?query=` (empty)
                // on load to discover all labels. Treat an empty/blank query as
                // "match all streams" (None) — the same as Loki — instead of
                // parsing "" as a stream selector, which fails with
                // `parse error: syntax error: unexpected $end, expecting '{'`.
                // `execute_detected_labels_query` already maps `None` to no
                // matchers (all series).
                "query" if query.is_none() && !value.trim().is_empty() => query = Some(value),
                "limit" | "field_limit" if limit.is_none() => {
                    limit = parse_usize_query_param("limit", &value).ok().or(limit);
                }
                _ => {}
            }
        }
    }

    let end = window.end.unwrap_or_else(current_unix_time_ns);
    let start = start_or_since(window.start, window.since, Some(end))?
        .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));

    Ok(DetectedLabelsParams {
        query,
        start,
        end,
        limit: limit.unwrap_or(1000),
    })
}
