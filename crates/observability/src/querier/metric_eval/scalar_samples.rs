use krabka_units::convert::TimeExt;

use crate::{
    ByteSize, ByteSizeExt, HttpQueryError, LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS,
    METRIC_DECIMAL_SCALE, MetricQuery, QuerierState, QueryKind, QueryParams, ScalarComparisonOp,
    Time, TimeRange, Value, active_log_delete_filters, add_loki_query_stats_for_metric_plan,
    add_loki_query_stats_for_metric_plan_with_hot_tail, default_metric_range_step,
    execute_http_metric_instant_query, execute_http_metric_range_query, hot_tail_snapshot,
    metric_query_uses_approx_topk, metric_query_uses_count_values, metric_scan_range,
    parse_decimal_sample_literal, plan_stream_query, validate_query_bytes_limit,
    validate_query_series_limit,
};

mod execute_http_metric_query;
mod rational_arithmetic;
mod scalar_sample;

pub(crate) use execute_http_metric_query::{HttpMetricQuery, execute_http_metric_query};
pub(crate) use rational_arithmetic::{
    Rational, decimal_scaled_numerator, impl_rational_division_ops, impl_rational_float_conversion,
    rational_to_f64,
};
pub(crate) use scalar_sample::ScalarSample;

pub(crate) fn validate_approximate_topk(
    state: &QuerierState,
    kind: QueryKind,
) -> Result<(), HttpQueryError> {
    validate_approximate_topk_enabled(&state.limits.shard_aggregations, kind)
}

pub(crate) fn validate_approximate_topk_enabled(
    enabled_aggregations: &[String],
    kind: QueryKind,
) -> Result<(), HttpQueryError> {
    if !enabled_aggregations
        .iter()
        .any(|name| name == "approx_topk")
    {
        return Err(HttpQueryError::ApproxTopKDisabled);
    }
    if matches!(kind, QueryKind::Range) {
        return Err(HttpQueryError::ApproxTopKRangeQuery);
    }
    Ok(())
}

pub(crate) fn gcd_signed(left: i128, right: u128) -> u128 {
    let mut left = left.unsigned_abs();
    let mut right = right;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

pub(crate) fn parse_scalar_sample(value: &str) -> Option<ScalarSample> {
    let ratio = parse_decimal_sample_literal(value)?;
    Some(ScalarSample::new(ratio.numerator, ratio.denominator))
}

/// Renders an extent the way `Loki` spells a query length in its own error text.
///
/// The whole seconds come from the nanosecond count by integer division, not
/// from [`TimeExt::secs_i64`]. That method rounds to nearest and would report a
/// second more than `Loki` does for the same window.
pub(crate) fn format_loki_query_length(range: Time) -> String {
    let total_seconds = range.nanos_i64().max(0) / 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;

    format!("{hours}h{minutes}m{seconds}s")
}

/// Resolves a range query's step in nanoseconds, defaulting it from the range.
///
/// `Loki` refuses a non-positive step outright rather than dividing by it, and
/// every range-vector response resolves its step through here.
pub(crate) fn resolved_range_step(
    step: Option<i64>,
    time_range: TimeRange,
) -> Result<i64, HttpQueryError> {
    let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    Ok(step_ns)
}

pub(crate) fn validate_loki_query_range_resolution(
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if !matches!(kind, QueryKind::Range) {
        return Ok(());
    }
    let step_ns = resolved_range_step(params.step, time_range)?;
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryResolutionTooHigh)?;
    // Loki truncates the point count, so the division stays over whole
    // nanoseconds rather than fractional seconds.
    if query_range.nanos_i64() / step_ns > LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS {
        return Err(HttpQueryError::QueryResolutionTooHigh);
    }
    Ok(())
}

pub(crate) fn validate_loki_range_query_range_limit(
    state: &QuerierState,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(state, time_range)?;
    }
    Ok(())
}

/// Applies `Loki`'s `max_entries_limit_per_query` to the request's `limit`
/// parameter.
///
/// A request that names no `limit` is not checked, as in `Loki`: the default
/// the handler then uses is well under any sane cap.
pub(crate) fn validate_query_entries_limit(
    state: &QuerierState,
    limit: Option<usize>,
) -> Result<(), HttpQueryError> {
    let max = state.limits.max_entries_limit_per_query;
    let Some(limit) = limit else {
        return Ok(());
    };
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    if max == 0 || limit <= max {
        return Ok(());
    }
    Err(HttpQueryError::MaxEntriesLimitPerQuery { limit, max })
}

/// Applies the tenant's cap on the bytes of `LogQL` source text.
///
/// Krabka's own limit, and not `Loki`'s `max_query_length`, which caps the
/// `[start, end]` window instead.
pub(crate) fn validate_query_string_bytes_limit(
    state: &QuerierState,
    query: &str,
) -> Result<(), HttpQueryError> {
    let limit = state.limits.max_query_string_bytes;
    if limit <= ByteSize::ZERO || query.len() <= limit.bytes_usize() {
        return Ok(());
    }
    Err(HttpQueryError::QueryStringTooLong {
        query_bytes: query.len(),
        max_bytes: limit.bytes_usize(),
    })
}

struct DurationUnit {
    suffix: &'static str,
    seconds: i64,
}

/// Renders an extent the way `Loki` spells a *limit* in its own error text.
///
/// This is Prometheus `model.Duration`'s own format, which `Loki` uses for the
/// limit side of `ErrQueryTooLong`: it writes days, then hours, then minutes,
/// then seconds, and it skips a unit whose count is zero. The default cap of
/// 721 hours reads as `30d1h`, not as `721h0m0s`.
///
/// It is deliberately not [`format_loki_query_length`](super::format_loki_query_length),
/// which renders the *observed* side of the same message. `Loki` formats the
/// two sides with two different Go types, and the differential suite compares
/// the rendered text.
pub(crate) fn format_loki_model_duration(extent: Time) -> String {
    let mut remaining = extent.nanos_i64().max(0) / 1_000_000_000;
    if remaining == 0 {
        return "0s".to_string();
    }
    let mut rendered = String::new();
    for unit in [
        DurationUnit {
            suffix: "d",
            seconds: 86_400,
        },
        DurationUnit {
            suffix: "h",
            seconds: 3_600,
        },
        DurationUnit {
            suffix: "m",
            seconds: 60,
        },
        DurationUnit {
            suffix: "s",
            seconds: 1,
        },
    ] {
        let count = remaining / unit.seconds;
        if count > 0 {
            rendered.push_str(&count.to_string());
            rendered.push_str(unit.suffix);
            remaining -= count * unit.seconds;
        }
    }
    rendered
}

/// Applies the tenant's `max_query_length`: the widest `[start, end]` a query
/// may cover.
///
/// `Loki` refuses such a query with `ErrQueryTooLong`, and the message here is
/// that text, with the tenant's own limit in it.
pub(crate) fn validate_loki_volume_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let limit = state.limits.max_query_length;
    if limit <= Time::ZERO {
        return Ok(());
    }
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(Time::from_nanos(i64::MAX)),
            limit: format_loki_model_duration(limit),
        })?;
    if query_range > limit {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range),
            limit: format_loki_model_duration(limit),
        });
    }
    Ok(())
}

/// Applies Krabka's own `[start, end]` cap, which is stricter than `Loki`'s
/// `max_query_length`.
pub(crate) fn validate_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let max_query_range = state.limits.max_query_range;
    if max_query_range <= Time::ZERO {
        return Ok(());
    }
    // `start_ns` and `end_ns` are instants; only their difference is an extent.
    // The error carries plain nanoseconds so its rendered message is fixed by
    // the `#[error]` format string alone.
    let max_range_ns = max_query_range.nanos_i64();
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryRangeTooLarge {
            range_ns: i64::MAX,
            max_range_ns,
        })?;
    if query_range > max_query_range {
        return Err(HttpQueryError::QueryRangeTooLarge {
            range_ns: query_range.nanos_i64(),
            max_range_ns,
        });
    }
    Ok(())
}
