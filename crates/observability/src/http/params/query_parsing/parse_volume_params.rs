use super::*;

pub(crate) fn parse_volume_params(raw_query: Option<&str>) -> Result<VolumeParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let mut limit = None;
    let mut target_labels = None;
    let mut aggregate_by = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "start",
            "end",
            "step",
            "limit",
            "targetLabels",
            "aggregateBy",
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
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "targetLabels" if target_labels.is_none() => {
                target_labels = Some(
                    value
                        .split(',')
                        .filter(|label| !label.is_empty())
                        .map(ToString::to_string)
                        .collect(),
                );
            }
            "aggregateBy" if aggregate_by.is_none() => {
                aggregate_by = Some(match value.as_str() {
                    "series" => VolumeAggregateBy::Series,
                    "labels" => VolumeAggregateBy::Labels,
                    _ => return Err(HttpQueryError::InvalidVolumeAggregation),
                });
            }
            _ => {}
        }
    }

    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start.unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));

    Ok(VolumeParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start,
        end,
        step,
        limit: limit.unwrap_or(100),
        target_labels,
        aggregate_by: aggregate_by.unwrap_or(VolumeAggregateBy::Series),
    })
}
