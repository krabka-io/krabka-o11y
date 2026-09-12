//! `PromQL` parser/planner entry points.

pub mod aggregate;
pub mod label_ops;
pub mod leaf;
pub mod over_time_range;
pub mod rate_range;
pub mod scalar_math;

use std::{any::Any, sync::Arc, time::Duration};

use krabka_blockstore::{Labels, SeriesFingerprint};
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
    fn a_step_grid_places_only_its_own_instants() {
        let grid = StepGrid {
            start: 1_000,
            end: 4_000,
            step: 1_000,
        };

        assert2::assert!(grid.point_count() == 4);
        assert2::assert!(grid.index_of(1_000) == Some(0));
        assert2::assert!(grid.index_of(4_000) == Some(3));
        // Between two instants, and outside the bounds on either side: the
        // range driver's leaf memo must miss on all three, so a subquery's
        // sub-step falls through to a plan of its own.
        assert2::assert!(grid.index_of(1_500) == None);
        assert2::assert!(grid.index_of(999) == None);
        assert2::assert!(grid.index_of(4_001) == None);
    }

    #[test]
    fn a_one_point_step_grid_holds_exactly_its_instant() {
        let grid = StepGrid::instant(7_000, 0);

        // A zero stride is raised to one, so the operators keep their
        // positive-stride invariant.
        assert2::assert!(grid.step == 1);
        assert2::assert!(grid.point_count() == 1);
        assert2::assert!(grid.index_of(7_000) == Some(0));
        assert2::assert!(grid.index_of(7_001) == None);
    }

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

    /// A subquery's colon is found by scanning the bracket content, and the
    /// slice that splits range from step is taken with the index that scan
    /// returns. The two have to agree about what an index counts. `x[é:]` is
    /// the smallest query where a character index and a byte index differ, so
    /// it is the one that decides it: malformed either way, it must come back
    /// as a parse error rather than panic inside a multi-byte character.
    #[test]
    fn a_subquery_range_holding_a_multibyte_character_is_a_parse_error() {
        for query in ["x[é:]", "x[é:1m]", "x[1m:é]", "x[é]", "x[é1m:é1m]"] {
            let err = parse_promql(query).unwrap_err();

            assert2::assert!(matches!(err, PromqlError::Parse(_)), "query `{query}`");
        }
    }

    /// The colon index is relative to the bracket content, not to the query,
    /// and a multi-byte character earlier in the query must not move it. Both
    /// of these are well-formed subqueries and have to keep parsing.
    #[test]
    fn a_multibyte_character_outside_the_brackets_leaves_a_subquery_alone() {
        let expr = parse_promql(r#"x{label="é"}[10m:1m]"#).unwrap();
        assert2::assert!(expr.to_string() == r#"x{label="é"}[10m:1m]"#);

        let expr = parse_promql(r#"x{label="é"}[10m:]"#).unwrap();
        assert2::assert!(expr.to_string() == r#"x{label="é"}[10m:]"#);
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

mod consume_ident;
mod consume_number_duration;
mod duration_expr_context;
mod duration_expr_parser;
mod duration_unit_seconds;
mod extended_modifier_at;
mod extended_selector_expr;
mod extended_selector_modifier;
mod inject_created_timestamp_zeros;
mod is_ident_char;
mod is_ident_start;
mod is_zero;
mod labeled_series;
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
mod step_grid;
mod strip_extended_selector_modifiers;
mod timed_value;
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
pub(crate) use inject_created_timestamp_zeros::inject_created_timestamp_zeros;
use is_ident_char::is_ident_char;
use is_ident_start::is_ident_start;
use is_zero::is_zero;
pub use labeled_series::LabeledSeries;
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
pub use step_grid::StepGrid;
use strip_extended_selector_modifiers::strip_extended_selector_modifiers;
pub use timed_value::TimedValue;
use top_level_colon::top_level_colon;
use wrap_extended_selectors::wrap_extended_selectors;
