use crate::{
    DetectedFieldsParams, DetectedLabelsParams, HttpQueryError, LOKI_DEFAULT_QUERY_RANGE,
    LOKI_MAX_TAIL_DELAY, OffsetDateTime, PatternsParams, QueryParams, Rfc3339, VolumeAggregateBy,
    VolumeParams, current_unix_time_ns, decode_form_component, parse_decimal_seconds_timestamp,
    parse_usize_query_param, start_or_since,
};
use krabka_units::convert::TimeExt;
pub(crate) fn parse_query_params(raw_query: Option<&str>) -> Result<QueryParams, HttpQueryError> {
    let mut query = None;
    let mut time = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
    let mut step = None;
    let mut interval = None;
    let mut limit = None;
    let mut direction = None;
    let mut delay_for = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "time",
            "start",
            "end",
            "since",
            "step",
            "interval",
            "limit",
            "direction",
            "delay_for",
        ],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "time" if time.is_none() => {
                time = Some(parse_loki_timestamp_query_param("time", &value)?);
            }
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
            "interval" if interval.is_none() => {
                interval = Some(parse_loki_duration_query_param("interval", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "direction" if direction.is_none() => direction = Some(value),
            "delay_for" if delay_for.is_none() => {
                delay_for = Some(parse_loki_tail_delay_for_query_param(&value)?);
            }
            _ => {}
        }
    }

    Ok(QueryParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        time,
        start,
        end,
        since,
        step,
        interval,
        limit,
        direction,
        delay_for,
    })
}

pub(crate) fn split_query_param_pairs<'a>(raw_query: &'a str, known_keys: &[&str]) -> Vec<&'a str> {
    let mut pairs = Vec::new();
    let mut pair_start = 0;
    for (index, byte) in raw_query.bytes().enumerate() {
        if byte == b'&'
            && known_keys.iter().any(|key| {
                raw_query[index + 1..]
                    .strip_prefix(key)
                    .is_some_and(|rest| rest.starts_with('='))
            })
        {
            if pair_start != index {
                pairs.push(&raw_query[pair_start..index]);
            }
            pair_start = index + 1;
        }
    }
    if pair_start < raw_query.len() {
        pairs.push(&raw_query[pair_start..]);
    }
    pairs
}

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

pub(crate) fn parse_detected_labels_params(
    raw_query: Option<&str>,
) -> Result<DetectedLabelsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
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
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_form_component(key)?;
            let value = decode_form_component(value)?;

            match key.as_str() {
                // Grafana's Logs Drilldown sends `detected_labels?query=` (empty)
                // on load to discover all labels. Treat an empty/blank query as
                // "match all streams" (None) — the same as Loki — instead of
                // parsing "" as a stream selector, which fails with
                // `parse error: syntax error: unexpected $end, expecting '{'`.
                // `execute_detected_labels_query` already maps `None` to no
                // matchers (all series).
                "query" if query.is_none() && !value.trim().is_empty() => query = Some(value),
                "start" if start.is_none() => {
                    start = Some(parse_loki_timestamp_query_param("start", &value)?);
                }
                "end" if end.is_none() => {
                    end = Some(parse_loki_timestamp_query_param("end", &value)?);
                }
                "since" if since.is_none() => {
                    since = Some(parse_loki_duration_query_param("since", &value)?);
                }
                "limit" | "field_limit" if limit.is_none() => {
                    limit = parse_usize_query_param("limit", &value).ok().or(limit);
                }
                _ => {}
            }
        }
    }

    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start_or_since(start, since, Some(end))?
        .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));

    Ok(DetectedLabelsParams {
        query,
        start,
        end,
        limit: limit.unwrap_or(1000),
    })
}

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

pub(crate) fn parse_loki_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(timestamp_ns) = value.parse::<i64>() {
        return Ok(timestamp_ns);
    }

    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns);
    }

    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .and_then(|timestamp| i64::try_from(timestamp.unix_timestamp_nanos()).ok())
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}

pub(crate) fn parse_loki_duration_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    let duration = if let Ok(seconds) = value.parse::<i64>() {
        seconds.checked_mul(1_000_000_000).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })?
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        duration_ns
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            if name == "since" {
                HttpQueryError::InvalidSinceQueryParameter {
                    value: value.to_string(),
                }
            } else {
                HttpQueryError::InvalidDurationQueryParameter {
                    value: value.to_string(),
                }
            }
        })?
    };

    if name == "since" && duration <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: value.to_string(),
        });
    }

    Ok(duration)
}

pub(crate) fn parse_loki_tail_delay_for_query_param(value: &str) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        seconds
            .checked_mul(1_000_000_000)
            .ok_or_else(|| HttpQueryError::InvalidQueryParameter {
                name: "delay_for",
                value: value.to_string(),
            })
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        Ok(duration_ns)
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })
    }
}

pub(crate) fn validate_loki_tail_delay_for(delay_for: i64) -> Result<(), HttpQueryError> {
    if !(0..=LOKI_MAX_TAIL_DELAY.nanos_i64()).contains(&delay_for) {
        return Err(HttpQueryError::TailDelayForTooLarge);
    }

    Ok(())
}

pub(crate) fn parse_prometheus_duration(value: &str) -> Option<i64> {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut total_ns = 0_i128;

    while pos < value.len() {
        let amount_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == amount_start {
            return None;
        }
        let amount = value[amount_start..pos].parse::<i128>().ok()?;

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let (unit_order, _, multiplier) = prometheus_duration_unit(&value[unit_start..pos])?;
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}

pub(crate) fn prometheus_duration_unit(unit: &str) -> Option<(u8, u16, i128)> {
    match unit {
        "y" => Some((0, 1 << 0, 31_536_000_000_000_000)),
        "w" => Some((1, 1 << 1, 604_800_000_000_000)),
        "d" => Some((2, 1 << 2, 86_400_000_000_000)),
        "h" => Some((3, 1 << 3, 3_600_000_000_000)),
        "m" => Some((4, 1 << 4, 60_000_000_000)),
        "s" => Some((5, 1 << 5, 1_000_000_000)),
        "ms" => Some((6, 1 << 6, 1_000_000)),
        "us" => Some((7, 1 << 7, 1_000)),
        "ns" => Some((8, 1 << 8, 1)),
        _ => None,
    }
}
