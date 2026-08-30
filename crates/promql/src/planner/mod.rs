//! `PromQL` parser/planner entry points.

pub mod aggregate;
pub mod label_ops;
pub mod leaf;
pub mod over_time_range;
pub mod rate_range;
pub mod scalar_math;

use std::{any::Any, sync::Arc, time::Duration};

use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use promql_parser::{
    parser::{
        Call, Expr, Extension, Function, FunctionArgs, ast::ExtensionExpr, parse, value::ValueType,
    },
    util::display_duration,
};

use crate::{PromqlError, error::Result};

#[cfg(test)]
mod tests {

    use promql_parser::parser::Expr;

    use super::*;
    use crate::PromqlError;

    #[test]
    fn parse_promql_wraps_parser_success() {
        let expr = parse_promql("up").unwrap();

        assert2::assert!(matches!(expr, Expr::VectorSelector(_)));
    }

    #[test]
    fn parse_promql_maps_parser_errors() {
        let err = parse_promql("up {{{").unwrap_err();

        assert2::assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn parse_promql_folds_range_duration_expressions() {
        let expr = parse_promql_with_duration_context(
            "metric[step()+1ms]",
            DurationExprContext::range(50_000, 60_000, secs(5)),
        )
        .unwrap();

        assert2::assert!(expr.to_string() == "metric[5s1ms]");
    }

    #[test]
    fn parse_promql_folds_parenthesized_offset_expression() {
        let expr = parse_promql_with_duration_context(
            "metric offset (-2 * 2)",
            DurationExprContext::instant(1_000_000),
        )
        .unwrap();

        assert2::assert!(expr.to_string() == "metric offset -4s");
    }

    #[test]
    fn parse_promql_rejects_huge_finite_duration_expression() {
        // `10s ^ 22` folds to a finite ~1e22 seconds, which overflows the
        // `Duration::from_secs_f64` representable range. It must surface a
        // parse error rather than panicking.
        let err = parse_promql_with_duration_context(
            "metric[10s ^ 22]",
            DurationExprContext::instant(1_000_000),
        )
        .unwrap_err();

        assert2::assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn parse_promql_preserves_unparenthesized_offset_precedence() {
        let expr = parse_promql_with_duration_context(
            "metric offset step()*0",
            DurationExprContext::range(50_000, 60_000, secs(5)),
        )
        .unwrap();

        assert2::assert!(expr.to_string() == "metric offset 5s * 0");
    }
}

// === split-modules: generated submodules ===
mod consume_ident;
mod consume_number_duration;
mod duration_expr_context;
mod duration_expr_parser;
mod duration_unit_seconds;
mod extended_modifier_at;
mod extended_selector_expr;
mod extended_selector_modifier;
mod is_ident_char;
mod is_ident_start;
mod is_zero;
mod matching_delimiter;
mod ms_to_seconds;
mod normalize_duration_expressions;
mod normalize_range_duration_content;
mod offset_operand;
mod parse_experimental_zero_arg_helper;
mod parse_promql;
mod parse_promql_with_duration_context;
mod seconds_to_duration_literal;
mod skip_ws;
mod starts_offset_keyword;
mod strip_extended_selector_modifiers;
mod top_level_colon;
mod wrap_extended_selectors;

use consume_ident::consume_ident;
use consume_number_duration::consume_number_duration;
pub use duration_expr_context::DurationExprContext;
use duration_expr_parser::DurationExprParser;
use duration_unit_seconds::duration_unit_seconds;
use extended_modifier_at::extended_modifier_at;
pub use extended_selector_expr::ExtendedSelectorExpr;
pub use extended_selector_modifier::ExtendedSelectorModifier;
use is_ident_char::is_ident_char;
use is_ident_start::is_ident_start;
use is_zero::is_zero;
use matching_delimiter::matching_delimiter;
use ms_to_seconds::ms_to_seconds;
use normalize_duration_expressions::normalize_duration_expressions;
use normalize_range_duration_content::normalize_range_duration_content;
use offset_operand::offset_operand;
use parse_experimental_zero_arg_helper::parse_experimental_zero_arg_helper;
pub use parse_promql::parse_promql;
pub use parse_promql_with_duration_context::parse_promql_with_duration_context;
use seconds_to_duration_literal::seconds_to_duration_literal;
use skip_ws::skip_ws;
use starts_offset_keyword::starts_offset_keyword;
use strip_extended_selector_modifiers::strip_extended_selector_modifiers;
use top_level_colon::top_level_colon;
use wrap_extended_selectors::wrap_extended_selectors;
