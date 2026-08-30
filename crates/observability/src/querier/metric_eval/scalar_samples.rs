use krabka_units::convert::TimeExt;
use num_traits::ToPrimitive;

use crate::{
    ByteSizeExt, HttpQueryError, LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS,
    LOKI_VOLUME_MAX_QUERY_RANGE, METRIC_DECIMAL_SCALE, MetricQuery, QuerierState, QueryKind,
    QueryParams, ScalarComparisonOp, Time, TimeRange, Value, active_log_delete_filters,
    add_loki_query_stats_for_metric_plan, add_loki_query_stats_for_metric_plan_with_hot_tail,
    default_metric_range_step, execute_http_metric_instant_query, execute_http_metric_range_query,
    hot_tail_snapshot, metric_query_uses_approx_topk, metric_query_uses_count_values,
    metric_scan_range, parse_decimal_sample_literal, plan_stream_query, validate_query_bytes_limit,
    validate_query_series_limit,
};
#[derive(Clone, Copy)]
pub(crate) struct ScalarSample {
    pub(crate) numerator: i128,
    pub(crate) denominator: u128,
}

impl ScalarSample {
    pub(crate) fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self {
                numerator: 0,
                denominator: 1,
            };
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).unwrap_or(i128::MAX),
            denominator: denominator / divisor,
        }
    }

    pub(crate) fn add(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_add(right?)?, denominator))
    }

    pub(crate) fn subtract(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_sub(right?)?, denominator))
    }

    pub(crate) fn multiply(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator.checked_mul(other.numerator)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }

    pub(crate) fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        // `< 0` against `<= 0` is a permanent survivor: `ScalarSample::new`
        // normalises a zero denominator to one, and the divisor's numerator was
        // rejected above, so this product is never zero.
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
    }

    pub(crate) fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    pub(crate) fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    pub(crate) fn compare(self, operator: ScalarComparisonOp, other: Self) -> Option<bool> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?)?;
        Some(match operator {
            ScalarComparisonOp::Equal => left == right,
            ScalarComparisonOp::NotEqual => left != right,
            ScalarComparisonOp::Greater => left > right,
            ScalarComparisonOp::GreaterOrEqual => left >= right,
            ScalarComparisonOp::Less => left < right,
            ScalarComparisonOp::LessOrEqual => left <= right,
        })
    }

    pub(crate) fn to_f64(self) -> Option<f64> {
        let value = self.numerator.to_f64()? / self.denominator.to_f64()?;
        value.is_finite().then_some(value)
    }

    pub(crate) fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64()?).round();
        Some(Self::new(i128::from_f64(scaled)?, METRIC_DECIMAL_SCALE))
    }

    pub(crate) fn format(self) -> String {
        let negative = self.numerator < 0;
        let numerator = self.numerator.unsigned_abs();
        let whole = numerator / self.denominator;
        let mut remainder = numerator % self.denominator;
        let sign = if negative { "-" } else { "" };
        if remainder == 0 {
            return format!("{sign}{whole}");
        }

        let mut decimals = String::new();
        while remainder != 0 && decimals.len() < 9 {
            remainder *= 10;
            let digit =
                u8::try_from(remainder / self.denominator).expect("decimal digit is less than 10");
            decimals.push(char::from(b'0' + digit));
            remainder %= self.denominator;
        }
        while decimals.ends_with('0') {
            decimals.pop();
        }
        format!("{sign}{whole}.{decimals}")
    }

    pub(crate) fn format_fixed_six(self) -> String {
        format!("{:.6}", self.to_f64().unwrap_or_default())
    }
}

pub(crate) fn parse_scalar_sample(value: &str) -> Option<ScalarSample> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(ScalarSample::new(numerator, denominator))
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

pub(crate) fn validate_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let Some(max_query_range) = state.max_query_range else {
        return Ok(());
    };
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

pub(crate) fn validate_loki_volume_query_range_limit(
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(Time::from_nanos(i64::MAX)),
        })?;
    if query_range > LOKI_VOLUME_MAX_QUERY_RANGE {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range),
        });
    }
    Ok(())
}

pub(crate) fn validate_loki_range_query_range_limit(
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(time_range)?;
    }
    Ok(())
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

pub(crate) fn validate_query_length_limit(
    state: &QuerierState,
    query: &str,
) -> Result<(), HttpQueryError> {
    let Some(max_query_length) = state.max_query_length.map(ByteSizeExt::bytes_usize) else {
        return Ok(());
    };
    let query_length = query.len();
    if query_length > max_query_length {
        return Err(HttpQueryError::QueryLengthTooLarge {
            query_length,
            max_query_length,
        });
    }
    Ok(())
}

pub(crate) async fn execute_http_metric_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: MetricQuery,
) -> Result<Value, HttpQueryError> {
    if metric_query_uses_approx_topk(&query) {
        return Err(HttpQueryError::ApproxTopKDisabled);
    }
    if metric_query_uses_count_values(&query) {
        return Err(HttpQueryError::CountValuesQuery);
    }
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                (time_range, step_ns),
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }
    let response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            (eval_range, 1),
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}
use num_traits::FromPrimitive as _;
