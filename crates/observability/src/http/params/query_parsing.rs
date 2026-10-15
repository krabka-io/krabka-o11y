pub(crate) use krabka_logql::parse_prometheus_duration_literal as parse_prometheus_duration;
use krabka_units::convert::TimeExt;

use crate::{
    DecodedQueryPair, DetectedFieldsParams, DetectedLabelsParams, HttpQueryError,
    LOKI_DEFAULT_QUERY_RANGE, LOKI_MAX_TAIL_DELAY, OffsetDateTime, PatternsParams, QueryParams,
    Rfc3339, VolumeAggregateBy, VolumeParams, current_unix_time_ns, decode_query_pair,
    parse_decimal_seconds_timestamp, parse_usize_query_param, start_or_since,
};

pub(crate) fn decoded_query_param_pairs(
    raw_query: Option<&str>,
    known_keys: &[&str],
) -> Result<Vec<DecodedQueryPair>, HttpQueryError> {
    let raw_query = raw_query.ok_or(HttpQueryError::MissingQueryParameter("query"))?;
    split_query_param_pairs(raw_query, known_keys)
        .into_iter()
        .map(decode_query_pair)
        .collect()
}

pub(crate) fn validate_loki_tail_delay_for(delay_for: i64) -> Result<(), HttpQueryError> {
    if !(0..=LOKI_MAX_TAIL_DELAY.nanos_i64()).contains(&delay_for) {
        return Err(HttpQueryError::TailDelayForTooLarge);
    }

    Ok(())
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

/// Fills `slot` from `parse` unless an earlier pair of the query already did.
///
/// Loki reads the first occurrence of a repeated parameter, so a later one is
/// neither parsed nor checked.
pub(crate) fn set_first_query_param<T>(
    slot: &mut Option<T>,
    parse: impl FnOnce() -> Result<T, HttpQueryError>,
) -> Result<(), HttpQueryError> {
    if slot.is_none() {
        *slot = Some(parse()?);
    }
    Ok(())
}

pub(crate) fn split_query_param_pairs<'a>(raw_query: &'a str, known_keys: &[&str]) -> Vec<&'a str> {
    let mut pairs = Vec::new();
    let mut pair_start = 0;
    for byte_position in raw_query.bytes().enumerate() {
        let index = byte_position.0;
        let byte = byte_position.1;
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

pub(crate) fn parse_loki_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(timestamp) = value.parse::<i64>() {
        return if value.len() <= 10 {
            timestamp.checked_mul(1_000_000_000).ok_or_else(|| {
                HttpQueryError::InvalidTimestampQueryParameter {
                    name,
                    value: value.to_string(),
                }
            })
        } else {
            Ok(timestamp)
        };
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

#[cfg(test)]
mod tests {
    use super::{HttpQueryError, parse_loki_timestamp_query_param};

    #[test]
    fn second_precision_timestamp_that_overflows_nanoseconds_is_rejected() {
        let value = "9999999999";
        let error = parse_loki_timestamp_query_param("time", value)
            .expect_err("the nanosecond timestamp overflows i64");

        assert!(matches!(
            error,
            HttpQueryError::InvalidTimestampQueryParameter {
                name: "time",
                value: rejected,
            } if rejected == value
        ));
    }
}

pub(crate) fn parse_query_params(raw_query: Option<&str>) -> Result<QueryParams, HttpQueryError> {
    let mut query = None;
    let mut time = None;
    let mut window = QueryWindowNanos::default();
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
            "targetBytesPerShard",
        ],
    ) {
        let pair = decode_query_pair(pair)?;
        window.record(&pair)?;
        let DecodedQueryPair { key, value } = pair;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "time" if time.is_none() => {
                let mut timestamp = parse_loki_timestamp_query_param("time", &value)?;
                // Loki's instant-query frontend forwards the parsed value as
                // an integer. Its querier parses every nonzero integer whose
                // decimal form has at most ten characters as seconds again.
                if timestamp != 0 && timestamp.to_string().len() <= 10 {
                    timestamp = timestamp
                        .checked_mul(1_000_000_000)
                        .or_else(|| {
                            timestamp
                                .checked_div(1_000_000)?
                                .checked_mul(1_000_000_000_000_000)
                        })
                        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
                            name: "time",
                            value: value.clone(),
                        })?;
                }
                time = Some(timestamp);
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

    let QueryWindowNanos { start, end, since } = window;
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
        let pair = decode_query_pair(pair)?;

        match pair.key.as_str() {
            "query" if query.is_none() => query = Some(pair.value),
            "start" if start.is_none() => {
                start = Some(parse_loki_timestamp_query_param("start", &pair.value)?);
            }
            "end" if end.is_none() => {
                end = Some(parse_loki_timestamp_query_param("end", &pair.value)?);
            }
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &pair.value)?);
            }
            "limit" if limit.is_none() => {
                limit = Some(parse_usize_query_param("limit", &pair.value)?);
            }
            "targetLabels" if target_labels.is_none() => {
                target_labels = Some(
                    pair.value
                        .split(',')
                        .filter(|label| !label.is_empty())
                        .map(ToString::to_string)
                        .collect(),
                );
            }
            "aggregateBy" if aggregate_by.is_none() => {
                aggregate_by = Some(match pair.value.as_str() {
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

/// The first `start`, `end` and `since` a query string carries, in
/// nanoseconds. A repeated parameter keeps its first value, as Loki's does.
#[derive(Default)]
pub(crate) struct QueryWindowNanos {
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
}

impl QueryWindowNanos {
    /// Parses `pair` when it is the first `start`, `end` or `since`, and
    /// ignores every other pair.
    pub(crate) fn record(&mut self, pair: &DecodedQueryPair) -> Result<(), HttpQueryError> {
        let value = pair.value.as_str();
        match pair.key.as_str() {
            "start" if self.start.is_none() => {
                self.start = Some(parse_loki_timestamp_query_param("start", value)?);
            }
            "end" if self.end.is_none() => {
                self.end = Some(parse_loki_timestamp_query_param("end", value)?);
            }
            "since" if self.since.is_none() => {
                self.since = Some(parse_loki_duration_query_param("since", value)?);
            }
            _ => {}
        }
        Ok(())
    }
}
