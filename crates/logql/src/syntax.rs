use std::fmt;

use crate::{
    ComparisonOp, DestinationLabel, DurationNanos, FieldFilter, FieldFilterExpression,
    FieldFilterLogicOp, FieldValue, IpMatcher, JsonExpressionPath, JsonExtraction,
    JsonParserConfig, LabelFormat, LabelFormatAssignment, LabelMatcher, LabelSelection,
    LabelSelectionSet, LineFilter, LineFilterOp, LineFormat, LogfmtExtraction, LogfmtParserConfig,
    MatchOp, OffsetNanos, ParseError, ParserStage, PatternParser, PipelineStage,
    QuantileDenominator, QuantileNumerator, RegexpParser, SourceLabel, StreamQuery,
    UnwrapExpression,
    filters::field_filter_expression_to_pipeline_stage,
    util::{
        QuotedChar, decode_quoted_escape, duration_unit, gcd_u64, is_ident_char, is_ident_start,
        parse_bytes_literal, parse_prometheus_duration_literal,
    },
};

mod arithmetic_text;
mod comparison_text;
mod expr_operator;
mod format_labels;
mod format_matching;
mod function_args;
mod logql_expr;
mod metric_binary_arithmetic;
mod metric_binary_comparison;
mod metric_binary_set;
mod metric_binary_set_op;
mod metric_label_join;
mod metric_label_replace;
mod metric_query;
mod metric_scalar_arithmetic;
mod metric_scalar_arithmetic_op;
mod metric_scalar_comparison;
mod metric_vector_group_modifier;
mod metric_vector_matching;
mod operator_at;
mod outer_metric_parentheses_inner;
mod parse_expr;
mod parse_expr_primary;
mod parse_logql_expr;
mod parse_metric_binary_arithmetic_query;
mod parse_metric_binary_comparison_query;
mod parse_metric_binary_set_query;
mod parse_metric_label_join_query;
mod parse_metric_label_replace_query;
mod parse_metric_query;
mod parse_metric_scalar_arithmetic_query;
mod parse_metric_scalar_comparison_query;
mod parse_metric_subexpression;
mod parse_query;
mod parse_scalar_text;
mod parse_string_arg;
mod parser;
mod quantile;
mod quoted;
mod range_aggregation;
mod range_aggregation_kind;
mod range_aggregation_supports_grouping;
mod scan_top_level;
mod set_text;
mod sign_is_unary_or_exponent;
mod strip_outer_metric_parentheses;
mod syntax_error;
mod vector_aggregation;
mod vector_aggregation_op;
mod vector_grouping;

use arithmetic_text::arithmetic_text;
use comparison_text::comparison_text;
use expr_operator::ExprOperator;
use format_labels::format_labels;
use format_matching::format_matching;
use function_args::function_args;
pub use logql_expr::LogqlExpr;
pub use metric_binary_arithmetic::MetricBinaryArithmetic;
pub use metric_binary_comparison::MetricBinaryComparison;
pub use metric_binary_set::MetricBinarySet;
pub use metric_binary_set_op::MetricBinarySetOp;
pub use metric_label_join::MetricLabelJoin;
pub use metric_label_replace::MetricLabelReplace;
pub use metric_query::MetricQuery;
pub use metric_scalar_arithmetic::MetricScalarArithmetic;
pub use metric_scalar_arithmetic_op::MetricScalarArithmeticOp;
pub use metric_scalar_comparison::MetricScalarComparison;
pub use metric_vector_group_modifier::MetricVectorGroupModifier;
pub use metric_vector_matching::MetricVectorMatching;
use operator_at::operator_at;
use outer_metric_parentheses_inner::outer_metric_parentheses_inner;
use parse_expr::parse_expr;
use parse_expr_primary::parse_expr_primary;
pub use parse_logql_expr::parse_logql_expr;
pub use parse_metric_binary_arithmetic_query::parse_metric_binary_arithmetic_query;
pub use parse_metric_binary_comparison_query::parse_metric_binary_comparison_query;
pub use parse_metric_binary_set_query::parse_metric_binary_set_query;
pub use parse_metric_label_join_query::parse_metric_label_join_query;
pub use parse_metric_label_replace_query::parse_metric_label_replace_query;
pub use parse_metric_query::parse_metric_query;
pub use parse_metric_scalar_arithmetic_query::parse_metric_scalar_arithmetic_query;
pub use parse_metric_scalar_comparison_query::parse_metric_scalar_comparison_query;
use parse_metric_subexpression::parse_metric_subexpression;
pub use parse_query::parse_query;
use parse_scalar_text::parse_scalar_text;
use parse_string_arg::parse_string_arg;
use parser::Parser;
pub use quantile::Quantile;
use quoted::Quoted;
pub use range_aggregation::RangeAggregation;
use range_aggregation_kind::RangeAggregationKind;
use range_aggregation_supports_grouping::range_aggregation_supports_grouping;
use scan_top_level::scan_top_level;
use set_text::set_text;
use sign_is_unary_or_exponent::sign_is_unary_or_exponent;
use strip_outer_metric_parentheses::strip_outer_metric_parentheses;
use syntax_error::syntax_error;
pub use vector_aggregation::VectorAggregation;
pub use vector_aggregation_op::VectorAggregationOp;
pub use vector_grouping::VectorGrouping;
