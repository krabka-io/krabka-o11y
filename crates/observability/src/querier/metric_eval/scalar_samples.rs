use krabka_units::convert::TimeExt;
use num_traits::{FromPrimitive as _, ToPrimitive};

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

mod execute_http_metric_query;
mod format_loki_query_length;
mod gcd_signed;
mod parse_scalar_sample;
mod resolved_range_step;
mod scalar_sample;
mod validate_loki_query_range_resolution;
mod validate_loki_range_query_range_limit;
mod validate_loki_volume_query_range_limit;
mod validate_query_length_limit;
mod validate_query_range_limit;

pub(crate) use execute_http_metric_query::execute_http_metric_query;
pub(crate) use format_loki_query_length::format_loki_query_length;
pub(crate) use gcd_signed::gcd_signed;
pub(crate) use parse_scalar_sample::parse_scalar_sample;
pub(crate) use resolved_range_step::resolved_range_step;
pub(crate) use scalar_sample::ScalarSample;
pub(crate) use validate_loki_query_range_resolution::validate_loki_query_range_resolution;
pub(crate) use validate_loki_range_query_range_limit::validate_loki_range_query_range_limit;
pub(crate) use validate_loki_volume_query_range_limit::validate_loki_volume_query_range_limit;
pub(crate) use validate_query_length_limit::validate_query_length_limit;
pub(crate) use validate_query_range_limit::validate_query_range_limit;
