use std::collections::BTreeSet;

use krabka_units::convert::ByteSizeExt as _;
use num_traits::ToPrimitive;
use regex::Regex;

use crate::{
    Labels, ParseError, UNWRAP_SAMPLE_VALUE_LABEL,
    stream::anchored_regex_pattern,
    template::{LineFormat, template_parse_error},
    util::{format_decimal_ratio, parse_bytes_literal, parse_prometheus_duration_literal},
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn label_format_accessors_return_configured_assignments() {
        let route = LabelFormatAssignment::rename("route", "path").unwrap();
        let summary =
            LabelFormatAssignment::template("summary", "{{.method}} {{.status}}").unwrap();
        let format = LabelFormat::new(vec![route.clone(), summary.clone()]).unwrap();

        assert_eq!(format.assignments(), &[route.clone(), summary]);
        assert_eq!(route.destination(), "route");
        assert!(matches!(route.value(), LabelFormatValue::Rename(source) if source == "path"));
    }

    #[test]
    fn unwrap_expression_accessors_and_validation_use_label() {
        let expression = UnwrapExpression::bytes("size").unwrap();

        assert_eq!(expression.label(), "size");
        assert_eq!(expression.conversion(), UnwrapConversion::Bytes);
        check!(UnwrapExpression::new("").is_err());
        check!(UnwrapExpression::bytes("").is_err());
        check!(UnwrapExpression::duration("").is_err());
    }

    #[test]
    fn unwrap_bytes_conversion_accepts_only_integer_bytes_in_range() {
        let expression = UnwrapExpression::bytes("size").unwrap();

        assert_eq!(expression.convert_sample_value("1B"), Some("1".to_string()));
        assert_eq!(expression.convert_sample_value("1.5B"), None);
    }

    #[test]
    fn raw_sample_literals_preserve_zero_and_signs() {
        assert_eq!(parse_raw_sample_literal("0"), Some("0".to_string()));
        assert_eq!(parse_raw_sample_literal("+12.5"), Some("12.5".to_string()));
        assert_eq!(parse_raw_sample_literal("-12.5"), Some("-12.5".to_string()));
    }

    #[test]
    fn raw_sample_literals_accept_fractional_boundary_forms() {
        assert_eq!(parse_raw_sample_literal(".5"), Some("0.5".to_string()));
        assert_eq!(parse_raw_sample_literal("1."), Some("1".to_string()));
        assert_eq!(parse_raw_sample_literal("1e2"), Some("100".to_string()));
        assert_eq!(parse_raw_sample_literal("1e-2"), Some("0.01".to_string()));
    }

    #[test]
    fn raw_sample_literals_reject_invalid_digits() {
        assert_eq!(parse_raw_sample_literal("12a.3"), None);
        assert_eq!(parse_raw_sample_literal("12.3a"), None);
        assert_eq!(parse_raw_sample_literal("1e2e3"), None);
    }

    #[test]
    fn label_selection_set_accessors_and_drop_apply_selection() {
        let drop_level = LabelSelection::name("level").unwrap();
        let drop_debug_app = LabelSelection::regex("app", "debug-.*").unwrap();
        let selections =
            LabelSelectionSet::new(vec![drop_level.clone(), drop_debug_app.clone()]).unwrap();

        assert_eq!(selections.selections(), &[drop_level, drop_debug_app]);

        let mut fields = labels(&[("app", "debug-api"), ("level", "warn"), ("status", "500")]);
        selections.apply_drop(&mut fields);

        assert_eq!(fields.get("status"), Some(&"500".to_string()));
        assert!(!fields.contains_key("app"));
        assert!(!fields.contains_key("level"));
    }

    #[test]
    fn label_selection_matcher_accessor_returns_matcher() {
        let exact = LabelSelection::equal("status", "500").unwrap();
        let regex = LabelSelection::regex("app", "api|worker").unwrap();
        let bare = LabelSelection::name("level").unwrap();

        assert_eq!(
            exact.matcher(),
            Some(&LabelSelectionMatcher::Equal("500".to_string()))
        );
        assert_eq!(
            regex.matcher(),
            Some(&LabelSelectionMatcher::Regex("api|worker".to_string()))
        );
        assert_eq!(bare.matcher(), None);
    }

    #[test]
    fn label_selection_matches_requires_present_matching_value() {
        let bare = LabelSelection::name("level").unwrap();
        let exact = LabelSelection::equal("status", "500").unwrap();
        let regex = LabelSelection::regex("app", "api|worker").unwrap();
        let fields = labels(&[("app", "api"), ("status", "500")]);
        let wrong_status = labels(&[("status", "200")]);
        let frontend = labels(&[("app", "frontend")]);

        for (selection, candidate, expected) in [
            (&bare, &fields, false),
            (&exact, &fields, true),
            (&exact, &wrong_status, false),
            (&regex, &fields, true),
            (&regex, &frontend, false),
        ] {
            assert_eq!(
                selection.matches(candidate),
                expected,
                "{selection:?} against {candidate:?}"
            );
        }
    }

    #[test]
    fn label_selection_validation_rejects_empty_names_and_invalid_regex() {
        check!(LabelSelection::name("").is_err());
        check!(LabelSelection::equal("", "value").is_err());
        check!(LabelSelection::regex("app", "[").is_err());
    }
}

// === split-modules: generated submodules ===
mod label_format;
mod label_format_assignment;
mod label_format_value;
mod label_selection;
mod label_selection_matcher;
mod label_selection_set;
mod parse_decimal_exponent;
mod parse_decimal_sample_literal;
mod parse_raw_sample_literal;
mod unwrap_conversion;
mod unwrap_expression;

pub use label_format::LabelFormat;
pub use label_format_assignment::LabelFormatAssignment;
pub use label_format_value::LabelFormatValue;
pub use label_selection::LabelSelection;
pub use label_selection_matcher::LabelSelectionMatcher;
pub use label_selection_set::LabelSelectionSet;
use parse_decimal_exponent::parse_decimal_exponent;
use parse_decimal_sample_literal::parse_decimal_sample_literal;
use parse_raw_sample_literal::parse_raw_sample_literal;
pub use unwrap_conversion::UnwrapConversion;
pub use unwrap_expression::UnwrapExpression;
