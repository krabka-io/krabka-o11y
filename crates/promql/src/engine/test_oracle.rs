use futures::{FutureExt, future::BoxFuture};
use krabka_units::prelude::*;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr, UnaryExpr, VectorSelector,
    token::{T_BOTTOMK, T_COUNT_VALUES, T_LIMIT_RATIO, T_LIMITK, T_QUANTILE, T_TOPK},
};

use super::{
    AggregateOp, FractionBounds, HistogramAccessor, InstantOperands, InstantValue, IrateFn,
    MatrixSelectorAt, OuterRangeFn, OverTimeFn, PromqlEngine, RangeFn,
    apply_count_values_aggregate, apply_histogram_accessor, apply_histogram_fraction,
    apply_histogram_quantile, apply_info, apply_k_aggregate, apply_outer_range_fn,
    apply_quantile_aggregate, apply_simple_aggregate, combine_instant_binary, emit_warning,
    info::parse_info_call,
    invalid_quantile_warning, is_valid_quantile, label_ops,
    labels::{absent_labels, labels_without_metric_name},
    scalar::{
        CalendarFn, ClampKind, SortDirection, UnaryFloatFn, calendar_fn_from_function_name,
        clamp_float, negate_query_result, round_to_nearest,
    },
    selector::timestamp_seconds,
    with_histogram_stats,
};
#[cfg(feature = "experimental-functions")]
use super::{
    apply_histogram_quantiles, apply_limit_ratio_aggregate, apply_limitk_aggregate,
    scalar::{DurationHelper, ScalarExtremaFn},
    validate_smoothing_factor,
};
use crate::{
    PromqlError, PromqlLabels as Labels,
    error::Result,
    planner::rate_range::RateUdfKind,
    result::{InstantSample, QueryResult, SampleValue},
    store::MetricStore,
};
mod promql_engine;
#[cfg(test)]

pub(crate) fn over_time_function(name: &str) -> Option<OverTimeFn> {
    Some(match name {
        "sum_over_time" => OverTimeFn::Sum,
        "avg_over_time" => OverTimeFn::Avg,
        "count_over_time" => OverTimeFn::Count,
        "min_over_time" => OverTimeFn::Min,
        "max_over_time" => OverTimeFn::Max,
        "stddev_over_time" => OverTimeFn::Stddev,
        "stdvar_over_time" => OverTimeFn::Stdvar,
        "mad_over_time" => OverTimeFn::Mad,
        "first_over_time" => OverTimeFn::First,
        "last_over_time" => OverTimeFn::Last,
        "ts_of_first_over_time" => OverTimeFn::TsOfFirst,
        "ts_of_last_over_time" => OverTimeFn::TsOfLast,
        "ts_of_min_over_time" => OverTimeFn::TsOfMin,
        "ts_of_max_over_time" => OverTimeFn::TsOfMax,
        "present_over_time" => OverTimeFn::Present,
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) fn string_literal_arg(
    call: &Call,
    index: usize,
    name: &str,
) -> Result<crate::PromqlString> {
    let Some(arg) = call.args.args.get(index) else {
        return Err(PromqlError::Plan(format!(
            "{} missing {name} argument",
            call.func.name
        )));
    };
    crate::planner::byte_string_expr::string_expr_value(arg).ok_or_else(|| {
        PromqlError::Plan(format!(
            "{} {name} argument must be a string",
            call.func.name
        ))
    })
}

pub(crate) fn unary_float_function(name: &str) -> Option<UnaryFloatFn> {
    Some(match name {
        "ceil" => UnaryFloatFn::Ceil,
        "floor" => UnaryFloatFn::Floor,
        "sgn" => UnaryFloatFn::Sgn,
        "abs" => UnaryFloatFn::Abs,
        "sqrt" => UnaryFloatFn::Sqrt,
        "exp" => UnaryFloatFn::Exp,
        "ln" => UnaryFloatFn::Ln,
        "log2" => UnaryFloatFn::Log2,
        "log10" => UnaryFloatFn::Log10,
        "sin" => UnaryFloatFn::Sin,
        "sinh" => UnaryFloatFn::Sinh,
        "cos" => UnaryFloatFn::Cos,
        "cosh" => UnaryFloatFn::Cosh,
        "tan" => UnaryFloatFn::Tan,
        "tanh" => UnaryFloatFn::Tanh,
        "asin" => UnaryFloatFn::Asin,
        "asinh" => UnaryFloatFn::Asinh,
        "acos" => UnaryFloatFn::Acos,
        "acosh" => UnaryFloatFn::Acosh,
        "atan" => UnaryFloatFn::Atan,
        "atanh" => UnaryFloatFn::Atanh,
        "deg" => UnaryFloatFn::Deg,
        "rad" => UnaryFloatFn::Rad,
        _ => return None,
    })
}
