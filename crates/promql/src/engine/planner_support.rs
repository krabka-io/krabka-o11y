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

mod aggregate_grouping;
mod binary_operand_is_plannable;
mod instant_expr_is_plannable;
mod is_extended_range_fold_call;
mod label_ops_kind;
mod label_ops_kind_from_function_name;
mod match_experimental_over_time_range_call;
mod match_over_time_range_call;
mod match_rate_range_call;
mod match_subquery_range_call;
mod no_param_outer_range_fn;
mod over_time_family_to_outer_range_fn;
mod param_aggregate_op_is_plannable;
mod range_expr_routes_through_planner;
mod range_fold_range_arg_index;
mod rate_udf_kind_to_outer_range_fn;
mod scalar_math_op_from_function_name;
mod simple_aggregate_op;
mod simple_aggregate_op_to_aggregate_op;
mod string_literal_value;
mod subquery_outer_fn;
mod util_call_is_plannable;
mod validate_extended_selector_modifier;

pub(super) use aggregate_grouping::aggregate_grouping;
use binary_operand_is_plannable::binary_operand_is_plannable;
pub(super) use instant_expr_is_plannable::instant_expr_is_plannable;
pub(super) use is_extended_range_fold_call::is_extended_range_fold_call;
pub(super) use label_ops_kind::LabelOpsKind;
pub(super) use label_ops_kind_from_function_name::label_ops_kind_from_function_name;
pub(super) use match_experimental_over_time_range_call::match_experimental_over_time_range_call;
pub(super) use match_over_time_range_call::match_over_time_range_call;
pub(super) use match_rate_range_call::match_rate_range_call;
pub(super) use match_subquery_range_call::match_subquery_range_call;
use no_param_outer_range_fn::no_param_outer_range_fn;
pub(super) use over_time_family_to_outer_range_fn::over_time_family_to_outer_range_fn;
use param_aggregate_op_is_plannable::param_aggregate_op_is_plannable;
pub(super) use range_expr_routes_through_planner::range_expr_routes_through_planner;
pub(super) use range_fold_range_arg_index::range_fold_range_arg_index;
pub(super) use rate_udf_kind_to_outer_range_fn::rate_udf_kind_to_outer_range_fn;
pub(super) use scalar_math_op_from_function_name::scalar_math_op_from_function_name;
pub(super) use simple_aggregate_op::simple_aggregate_op;
pub(super) use simple_aggregate_op_to_aggregate_op::simple_aggregate_op_to_aggregate_op;
pub(super) use string_literal_value::string_literal_value;
pub(super) use subquery_outer_fn::SubqueryOuterFn;
use util_call_is_plannable::util_call_is_plannable;
pub(super) use validate_extended_selector_modifier::validate_extended_selector_modifier;
