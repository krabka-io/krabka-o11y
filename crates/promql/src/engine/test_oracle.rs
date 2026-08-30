use futures::{FutureExt, future::BoxFuture};
use krabka_blockstore::Labels;
use krabka_units::prelude::*;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr, MatrixSelector, UnaryExpr, VectorSelector,
    token::{T_BOTTOMK, T_COUNT_VALUES, T_LIMIT_RATIO, T_LIMITK, T_QUANTILE, T_TOPK},
};

use super::{
    AggregateOp, ExtendedSelectorExpr, ExtendedSelectorModifier, HistogramAccessor, InstantValue,
    IrateFn, OuterRangeFn, OverTimeFn, PromqlEngine, RangeFn, aggregate_k, aggregate_quantile,
    apply_count_values_aggregate, apply_histogram_accessor, apply_histogram_fraction,
    apply_histogram_quantile, apply_info, apply_k_aggregate, apply_outer_range_fn,
    apply_quantile_aggregate, apply_simple_aggregate, combine_instant_binary, emit_warning,
    info::parse_info_call,
    invalid_quantile_warning, is_valid_quantile, label_ops,
    labels::{absent_labels, labels_without_metric_name},
    range_functions::range_has_samples,
    scalar::{
        CalendarFn, ClampKind, SortDirection, UnaryFloatFn, clamp_float, negate_query_result,
        round_to_nearest,
    },
    selector::timestamp_seconds,
};
#[cfg(feature = "experimental-functions")]
use super::{
    apply_histogram_quantiles, apply_limit_ratio_aggregate, apply_limitk_aggregate,
    scalar::{DurationHelper, ScalarExtremaFn},
    validate_smoothing_factor,
};
use crate::{
    PromqlError,
    error::Result,
    planner::rate_range::RateUdfKind,
    result::{InstantSample, QueryResult, SampleValue},
    store::MetricStore,
};

mod calendar_function;
mod over_time_function;
mod promql_engine;
mod string_literal_arg;
mod unary_float_function;

use calendar_function::calendar_function;
use over_time_function::over_time_function;
#[cfg(test)]
use string_literal_arg::string_literal_arg;
use unary_float_function::unary_float_function;
