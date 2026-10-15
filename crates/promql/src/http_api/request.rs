use std::time::{SystemTime, UNIX_EPOCH};

use krabka_blockstore::TenantId;
use krabka_metrics::QueryEnforcer;
use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use promql_parser::parser::Expr;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{ApiError, PrometheusApiState};
use crate::{
    MetricStore, PromqlError, PromqlMatcher as LabelMatcher, QueryResult,
    engine::{label_matcher_sets, range_resolution_exceeds_limit}, parse_promql,
};
mod parse_cardinality_params;
mod parse_discovery_params;
mod prometheus_duration_ms;
mod query_timeout;
pub(super) use parse_cardinality_params::parse_cardinality_params;
pub(super) use parse_discovery_params::parse_discovery_params;
use prometheus_duration_ms::prometheus_duration_ms;
pub(super) use query_timeout::query_timeout;

#[derive(Debug, Default)]
pub(crate) struct CardinalityParams {
    pub(crate) selector: Option<String>,
    pub(crate) label_names: Vec<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Default)]
pub(crate) struct DiscoveryParams {
    pub(crate) matches: Vec<String>,
    pub(crate) start: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) limit: Option<usize>,
}

pub(crate) struct DiscoveryWindow {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
}

pub(crate) fn discovery_window(params: &DiscoveryParams) -> Result<DiscoveryWindow, ApiError> {
    let start_ms = match params.start.as_deref() {
        Some(start) => timestamp_ms(start)?,
        None => 0,
    };
    let end_ms = match params.end.as_deref() {
        Some(end) => timestamp_ms(end)?,
        None => i64::MAX,
    };
    validate_timestamp_range(start_ms, end_ms)?;
    Ok(DiscoveryWindow { start_ms, end_ms })
}

pub(crate) fn check_range_resolution(
    start_ms: i64,
    end_ms: i64,
    step: Time,
) -> Result<(), ApiError> {
    let step_ms = step.millis_i64();
    if range_resolution_exceeds_limit(start_ms, end_ms, step_ms) {
        return Err(ApiError::bad_data(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)",
        ));
    }
    Ok(())
}

pub(crate) fn apply_limit<T>(values: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit.filter(|limit| *limit > 0) {
        values.truncate(limit);
    }
}

pub(crate) fn apply_result_limit(result: &mut QueryResult, limit: Option<usize>) {
    match result {
        QueryResult::InstantVector(samples) => apply_limit(samples, limit),
        QueryResult::RangeMatrix(series) => apply_limit(series, limit),
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => {}
    }
}

pub(crate) fn enforce_sample_count<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    processed: u64,
) -> Result<(), ApiError> {
    QueryEnforcer::check_sample_count(state.query_limits.for_tenant(tenant.as_str()), processed)
        .map_err(ApiError::from)
}

pub(crate) fn optional_timestamp_ms(value: Option<&str>) -> Result<i64, ApiError> {
    match value {
        Some(value) => timestamp_ms(value),
        None => unix_now_ms(),
    }
}

pub(crate) fn parse_cardinality_form(body: &[u8]) -> Result<CardinalityParams, ApiError> {
    parse_cardinality_params(std::str::from_utf8(body).ok())
}

pub(crate) fn parse_discovery_form(body: &[u8]) -> Result<DiscoveryParams, ApiError> {
    parse_discovery_params(std::str::from_utf8(body).ok())
}

pub(crate) fn parse_limit_parameter(value: &str) -> Result<usize, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data("invalid limit parameter"))
}

pub(crate) fn required_form_param(value: Option<String>, name: &str) -> Result<String, ApiError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_data(format!("missing {name} parameter")))
}

pub(crate) fn rfc3339_to_ms(value: &str) -> Result<i64, ()> {
    let time = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
    i64::try_from(time.unix_timestamp_nanos() / 1_000_000).map_err(|_| ())
}

pub(crate) fn seconds_to_ms(value: &str) -> Result<i64, ()> {
    let seconds = value.parse::<f64>().map_err(|_| ())?;
    (seconds * 1000.0).round().to_i64().ok_or(())
}

pub(crate) fn timestamp_ms(value: &str) -> Result<i64, ApiError> {
    seconds_to_ms(value)
        .or_else(|()| rfc3339_to_ms(value))
        .map_err(|()| ApiError::bad_data("invalid timestamp"))
}

pub(crate) fn unix_now_ms() -> Result<i64, ApiError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::internal(format!("system time before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ApiError::internal("system time exceeds supported timestamp range"))
}

pub(crate) fn validate_timestamp_range(start_ms: i64, end_ms: i64) -> Result<(), ApiError> {
    if end_ms < start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok(())
}

pub(crate) fn discovery_matchers(
    params: &DiscoveryParams,
) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    if params.matches.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    let mut out = Vec::new();
    for selector in &params.matches {
        out.extend(selector_matchers(selector).map_err(ApiError::from)?);
    }
    Ok(out)
}

/// Returns a Prometheus query-API duration parameter as an extent.
///
/// The parameter is a `step` or a lookback. This function accepts both
/// encodings the API allows: a bare second count, which can be fractional, and
/// a Prometheus duration string such as `5m` or `1h30m`.
pub(crate) fn duration_param(value: &str) -> Result<Time, ApiError> {
    let millis = seconds_to_ms(value)
        .or_else(|()| prometheus_duration_ms(value).ok_or(()))
        .map_err(|()| ApiError::bad_data("invalid duration"))?;
    if millis <= 0 {
        return Err(ApiError::bad_data("duration must be positive"));
    }
    Ok(Time::from_millis(millis))
}

pub(crate) fn enforce_query_range_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    start_ms: i64,
    end_ms: i64,
) -> Result<(), ApiError> {
    let now_ms = unix_now_ms()?;
    QueryEnforcer::check_range(
        state.query_limits.for_tenant(tenant.as_str()),
        start_ms,
        end_ms,
        now_ms,
    )
    .map_err(ApiError::from)
}

pub(crate) fn enforce_selected_series_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    selected: usize,
) -> Result<(), ApiError> {
    QueryEnforcer::check_series_count(
        state.query_limits.for_tenant(tenant.as_str()),
        u64::try_from(selected).unwrap_or(u64::MAX),
    )
    .map_err(ApiError::from)
}

pub(crate) fn selector_matchers(selector: &str) -> Result<Vec<Vec<LabelMatcher>>, PromqlError> {
    match parse_promql(selector)? {
        Expr::VectorSelector(selector) => Ok(label_matcher_sets(&selector)),
        Expr::Extension(extension)
            if extension
                .expr
                .as_any()
                .is::<crate::planner::byte_selector_expr::ByteSelectorExpr>() =>
        {
            Ok(extension
                .expr
                .as_any()
                .downcast_ref::<crate::planner::byte_selector_expr::ByteSelectorExpr>()
                .expect("checked type")
                .matcher_sets
                .clone())
        }
        Expr::MatrixSelector(selector) => Ok(label_matcher_sets(&selector.vs)),
        other => Err(PromqlError::Plan(format!(
            "metadata matcher must be a vector selector, got {other}"
        ))),
    }
}
