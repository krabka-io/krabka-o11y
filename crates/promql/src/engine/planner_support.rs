#[cfg(feature = "experimental-functions")]
use promql_parser::parser::token::{T_LIMIT_RATIO, T_LIMITK};
use promql_parser::parser::{
    AggregateExpr, Call, Expr, LabelModifier, MatrixSelector, SubqueryExpr,
    token::{
        T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_GROUP, T_MAX, T_MIN, T_QUANTILE, T_STDDEV,
        T_STDVAR, T_SUM, T_TOPK, TokenType,
    },
    value::ValueType,
};

use super::{
    aggregation::AggregateOp,
    histogram::histogram_accessor_from_function_name,
    range_functions::{IrateFn, OuterRangeFn, OverTimeFn, RangeFn},
    scalar::calendar_fn_from_function_name,
};
use crate::{
    PromqlError,
    error::Result,
    functions::{OverTimeFamily, ScalarMathOp},
    planner::{
        ExtendedSelectorExpr, ExtendedSelectorModifier,
        aggregate::{Grouping, SimpleAggregateOp},
        label_ops::SortOrder,
        over_time_range::over_time_family_from_function_name,
        rate_range::RateUdfKind,
    },
};
mod float_scalar_math_values;
mod instant_expr_is_plannable;
mod is_extended_range_fold_call;
mod label_ops_kind;
mod match_experimental_over_time_range_call;
mod match_over_time_range_call;
mod match_rate_range_call;
mod match_subquery_range_call;
mod no_param_outer_range_fn;
mod param_aggregate_op_is_plannable;
mod range_expr_routes_through_planner;
mod range_fold_range_arg_index;
mod scalar_math_op_from_function_name;
mod subquery_outer_fn;
mod util_call_is_plannable;
pub(super) use float_scalar_math_values::float_scalar_math_values;
pub(super) use instant_expr_is_plannable::instant_expr_is_plannable;
pub(super) use is_extended_range_fold_call::is_extended_range_fold_call;
pub(super) use label_ops_kind::LabelOpsKind;
pub(super) use match_experimental_over_time_range_call::match_experimental_over_time_range_call;
pub(super) use match_over_time_range_call::match_over_time_range_call;
pub(super) use match_rate_range_call::match_rate_range_call;
pub(super) use match_subquery_range_call::match_subquery_range_call;
pub(super) use no_param_outer_range_fn::no_param_outer_range_fn;
use param_aggregate_op_is_plannable::param_aggregate_op_is_plannable;
pub(super) use range_expr_routes_through_planner::range_expr_routes_through_planner;
pub(super) use range_fold_range_arg_index::range_fold_range_arg_index;
pub(super) use scalar_math_op_from_function_name::scalar_math_op_from_function_name;
pub(super) use subquery_outer_fn::SubqueryOuterFn;
use util_call_is_plannable::util_call_is_plannable;

/// Maps an aggregation `by`/`without` modifier to the planner [`Grouping`].
///
/// `None` means the aggregation has no modifier. The caller treats that as
/// `by ()`, which collapses all series into one group.
pub(crate) fn aggregate_grouping(modifier: Option<&LabelModifier>) -> Option<Grouping> {
    match modifier? {
        LabelModifier::Include(include) => Some(Grouping::By(include.labels.clone())),
        LabelModifier::Exclude(exclude) => Some(Grouping::Without(exclude.labels.clone())),
    }
}

/// Returns the value of the string-literal call argument at `index`.
///
/// Parentheses around the literal are transparent, because Prometheus's parser
/// treats `(("dst"))` as the string `dst`. Returns `None` when the argument is
/// absent or is not a string literal. Unlike `string_literal_arg`, this function
/// never returns an error. The label-ops planner uses it to probe the call shape
/// and falls back on any mismatch.
pub(crate) fn string_literal_value(call: &Call, index: usize) -> Option<crate::PromqlString> {
    crate::planner::byte_string_expr::string_expr_value(call.args.args.get(index)?.as_ref())
}

/// Returns `true` when the operator path can carry one binary operand.
///
/// The planner folds a scalar operand through the pure scalar path of the
/// interpreter, which is always plannable. A vector operand must itself be
/// structurally plannable. A matrix or string operand is never plannable.
pub(crate) fn binary_operand_is_plannable(operand: &Expr) -> bool {
    match operand.value_type() {
        ValueType::Scalar => true,
        ValueType::Vector => instant_expr_is_plannable(operand),
        ValueType::Matrix | ValueType::String => false,
    }
}

/// Maps a `PromQL` function name to its label-rewrite or ordering kind. Returns
/// `None` for any function outside this set.
pub(crate) fn label_ops_kind_from_function_name(name: &str) -> Option<LabelOpsKind> {
    Some(match name {
        "label_replace" => LabelOpsKind::LabelReplace,
        "label_join" => LabelOpsKind::LabelJoin,
        "sort" => LabelOpsKind::Sort(SortOrder::Ascending),
        "sort_desc" => LabelOpsKind::Sort(SortOrder::Descending),
        "sort_by_label" => LabelOpsKind::SortByLabel(SortOrder::Ascending),
        "sort_by_label_desc" => LabelOpsKind::SortByLabel(SortOrder::Descending),
        _ => return None,
    })
}

/// Maps an [`OverTimeFamily`] to the shared [`OuterRangeFn`] of the same name.
///
/// [`OverTimeFamily`] is the output of the `*_over_time` matcher, and the
/// `eval_over_time_call` of the interpreter applies the [`OuterRangeFn`].
/// `quantile_over_time` carries its resolved `phi`. Every other member maps to
/// the matching [`OverTimeFn`]. The matcher returns only the non-experimental
/// members, so the experimental [`OverTimeFn`] variants (`Mad`/`First`/`TsOf*`)
/// are unreachable here.
pub(crate) fn over_time_family_to_outer_range_fn(family: OverTimeFamily, phi: f64) -> OuterRangeFn {
    match family {
        OverTimeFamily::Sum => OuterRangeFn::OverTime(OverTimeFn::Sum),
        OverTimeFamily::Avg => OuterRangeFn::OverTime(OverTimeFn::Avg),
        OverTimeFamily::Count => OuterRangeFn::OverTime(OverTimeFn::Count),
        OverTimeFamily::Min => OuterRangeFn::OverTime(OverTimeFn::Min),
        OverTimeFamily::Max => OuterRangeFn::OverTime(OverTimeFn::Max),
        OverTimeFamily::Stddev => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        OverTimeFamily::Stdvar => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        OverTimeFamily::Last => OuterRangeFn::OverTime(OverTimeFn::Last),
        OverTimeFamily::Present => OuterRangeFn::OverTime(OverTimeFn::Present),
        OverTimeFamily::Quantile => OuterRangeFn::QuantileOverTime(phi),
    }
}

/// Maps a [`RateUdfKind`] to the shared [`OuterRangeFn`] of the same name.
///
/// [`RateUdfKind`] is the output of the rate-family matcher, and the
/// `eval_*_call` of the interpreter applies the [`OuterRangeFn`].
/// `rate`/`increase`/`delta` are extrapolated range folds. `irate`/`idelta` are
/// instant-delta folds. This is the seam that lets a histogram-bearing
/// rate-family call route through the shared `apply_outer_range_fn` kernel
/// instead of the float-only UDF chain.
pub(crate) fn rate_udf_kind_to_outer_range_fn(kind: RateUdfKind) -> OuterRangeFn {
    match kind {
        RateUdfKind::Rate => OuterRangeFn::Range(RangeFn::Rate),
        RateUdfKind::Increase => OuterRangeFn::Range(RangeFn::Increase),
        RateUdfKind::Delta => OuterRangeFn::Range(RangeFn::Delta),
        RateUdfKind::Irate => OuterRangeFn::InstantDelta(IrateFn::Irate),
        RateUdfKind::Idelta => OuterRangeFn::InstantDelta(IrateFn::Idelta),
    }
}

/// Maps an aggregation token to its simple-aggregation lowering. Returns `None`
/// for an op outside the simple set, such as a param op, `stddev`, or `stdvar`.
pub(crate) fn simple_aggregate_op(token: TokenType) -> Option<SimpleAggregateOp> {
    match token.id() {
        T_SUM => Some(SimpleAggregateOp::Sum),
        T_AVG => Some(SimpleAggregateOp::Avg),
        T_MIN => Some(SimpleAggregateOp::Min),
        T_MAX => Some(SimpleAggregateOp::Max),
        T_COUNT => Some(SimpleAggregateOp::Count),
        T_GROUP => Some(SimpleAggregateOp::Group),
        _ => None,
    }
}

/// Maps the planner [`SimpleAggregateOp`] to the interpreter [`AggregateOp`].
///
/// [`SimpleAggregateOp`] shapes the `DataFusion` plan, and [`AggregateOp`]
/// drives the shared `apply_simple_aggregate` kernel. Both enumerate the same
/// six simple ops, so the map is total. This is the seam that lets the
/// histogram-bearing operator path reuse the reduction core of the interpreter.
pub(crate) fn simple_aggregate_op_to_aggregate_op(op: SimpleAggregateOp) -> AggregateOp {
    match op {
        SimpleAggregateOp::Sum => AggregateOp::Sum,
        SimpleAggregateOp::Avg => AggregateOp::Avg,
        SimpleAggregateOp::Min => AggregateOp::Min,
        SimpleAggregateOp::Max => AggregateOp::Max,
        SimpleAggregateOp::Count => AggregateOp::Count,
        SimpleAggregateOp::Group => AggregateOp::Group,
    }
}

pub(crate) fn validate_extended_selector_modifier(
    function_name: &str,
    modifier: ExtendedSelectorModifier,
) -> Result<()> {
    let allowed = match modifier {
        ExtendedSelectorModifier::Anchored => matches!(
            function_name,
            "changes" | "delta" | "increase" | "rate" | "resets"
        ),
        ExtendedSelectorModifier::Smoothed => {
            matches!(function_name, "delta" | "increase" | "rate")
        }
    };
    if allowed {
        return Ok(());
    }

    let allowed_functions = match modifier {
        ExtendedSelectorModifier::Anchored => "changes, delta, increase, rate, resets",
        ExtendedSelectorModifier::Smoothed => "delta, increase, rate",
    };
    Err(PromqlError::Exec(format!(
        "{} modifier can only be used with: {allowed_functions} - not with {function_name}",
        modifier.keyword()
    )))
}
