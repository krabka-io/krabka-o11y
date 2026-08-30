use std::{cmp::Ordering, net::IpAddr};

use krabka_units::ByteSize;
use regex::Regex;

use crate::{
    Labels, ParseError,
    stream::{PipelineStage, insert_extracted_field},
    util::{parse_bytes_literal, parse_prometheus_duration_literal},
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Labels;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn number_filter(name: &str, op: ComparisonOp, expected: f64) -> FieldFilter {
        FieldFilter::new(name, op, FieldValue::Number(expected))
    }

    fn string_filter(name: &str, op: ComparisonOp, expected: &str) -> FieldFilter {
        FieldFilter::new(name, op, FieldValue::String(expected.to_string()))
    }

    #[test]
    fn field_filter_matches_returns_candidate_result() {
        let filter = number_filter("status", ComparisonOp::GreaterEqual, 500.0);

        assert!(filter.matches(&labels(&[("status", "500")])));
        assert!(!filter.matches(&labels(&[("status", "499")])));
    }

    #[test]
    fn field_filter_expression_matches_honors_logic() {
        let expression = FieldFilterExpression::Chain {
            first: Box::new(FieldFilterExpression::Group(Box::new(
                FieldFilterExpression::Filter(number_filter(
                    "status",
                    ComparisonOp::GreaterEqual,
                    500.0,
                )),
            ))),
            rest: vec![(
                FieldFilterLogicOp::Or,
                FieldFilterExpression::Filter(string_filter("level", ComparisonOp::Equal, "warn")),
            )],
        };

        for (pairs, expected) in [
            ([("status", "500"), ("level", "info")], true),
            ([("status", "200"), ("level", "warn")], true),
            ([("status", "200"), ("level", "info")], false),
        ] {
            assert_eq!(expression.matches(&labels(&pairs)), expected, "{pairs:?}");
        }
    }

    #[test]
    fn field_filter_chain_matches_and_exposes_rest_filters() {
        let rest_filter = string_filter("path", ComparisonOp::NotEqual, "/health");
        let chain = FieldFilterChain::new(
            number_filter("status", ComparisonOp::GreaterEqual, 500.0),
            vec![(FieldFilterLogicOp::And, rest_filter.clone())],
        );

        assert!(chain.matches(&labels(&[("status", "500"), ("path", "/checkout")])));
        assert!(!chain.matches(&labels(&[("status", "500"), ("path", "/health")])));
        assert_eq!(chain.rest(), &[(FieldFilterLogicOp::And, rest_filter)]);
    }

    #[test]
    fn field_filter_validation_rejects_invalid_combinations() {
        for (name, op, value) in [
            (
                "path",
                ComparisonOp::RegexEqual,
                FieldValue::String("[".to_string()),
            ),
            ("path", ComparisonOp::RegexEqual, FieldValue::Number(1.0)),
            (
                "remote_addr",
                ComparisonOp::Greater,
                FieldValue::Ip(IpMatcher::parse("192.168.1.1").unwrap()),
            ),
        ] {
            assert!(
                FieldFilter::try_new(name, op, value.clone()).is_err(),
                "{name} {op:?} {value:?}"
            );
        }
    }

    #[test]
    fn comparison_ops_compare_number_boundaries() {
        for (op, candidate, expected, result) in [
            (ComparisonOp::Equal, 500.0, 500.0, true),
            (ComparisonOp::Equal, 499.0, 500.0, false),
            (ComparisonOp::NotEqual, 499.0, 500.0, true),
            (ComparisonOp::NotEqual, 500.0, 500.0, false),
            (ComparisonOp::Greater, 501.0, 500.0, true),
            (ComparisonOp::Greater, 500.0, 500.0, false),
            (ComparisonOp::GreaterEqual, 501.0, 500.0, true),
            (ComparisonOp::GreaterEqual, 500.0, 500.0, true),
            (ComparisonOp::GreaterEqual, 499.0, 500.0, false),
            (ComparisonOp::Less, 499.0, 500.0, true),
            (ComparisonOp::Less, 500.0, 500.0, false),
            (ComparisonOp::LessEqual, 499.0, 500.0, true),
            (ComparisonOp::LessEqual, 500.0, 500.0, true),
            (ComparisonOp::LessEqual, 501.0, 500.0, false),
        ] {
            assert_eq!(
                op.compare_numbers(candidate, expected),
                result,
                "{candidate} {op:?} {expected}"
            );
        }
    }

    #[test]
    fn comparison_ops_compare_string_boundaries() {
        for (op, candidate, expected, result) in [
            (ComparisonOp::Greater, "n", "m", true),
            (ComparisonOp::Greater, "m", "m", false),
            (ComparisonOp::GreaterEqual, "n", "m", true),
            (ComparisonOp::GreaterEqual, "m", "m", true),
            (ComparisonOp::GreaterEqual, "l", "m", false),
            (ComparisonOp::Less, "l", "m", true),
            (ComparisonOp::Less, "m", "m", false),
            (ComparisonOp::Less, "n", "m", false),
            (ComparisonOp::LessEqual, "l", "m", true),
            (ComparisonOp::LessEqual, "m", "m", true),
            (ComparisonOp::LessEqual, "n", "m", false),
        ] {
            assert_eq!(
                op.compare_strings(candidate, expected),
                result,
                "{candidate} {op:?} {expected}"
            );
        }
    }

    #[test]
    fn line_filter_reports_ip_matcher_mode() {
        assert!(
            !LineFilter::new(LineFilterOp::Contains, "error")
                .unwrap()
                .is_ip_matcher()
        );
        assert!(
            LineFilter::ip(LineFilterOp::Contains, "192.168.1.0/24")
                .unwrap()
                .is_ip_matcher()
        );
    }

    #[test]
    fn line_filter_validation_rejects_invalid_regex_and_ip_ops() {
        assert!(LineFilter::new(LineFilterOp::Regex, "[").is_err());
        assert!(LineFilter::ip(LineFilterOp::Regex, "192.168.1.1").is_err());
    }

    #[test]
    fn ip_matcher_returns_original_pattern() {
        let matcher = IpMatcher::parse("192.168.1.0/24").unwrap();

        assert_eq!(matcher.pattern(), "192.168.1.0/24");
    }

    #[test]
    fn ip_matcher_rejects_invalid_ranges_and_prefixes() {
        for pattern in [
            "192.168.1.10-192.168.1.1",
            "192.168.1.1-2001:db8::1",
            "192.168.1.1/33",
        ] {
            assert!(IpMatcher::parse(pattern).is_err(), "{pattern}");
        }
    }

    #[test]
    fn ip_matcher_accepts_single_address_ranges_and_host_prefixes() {
        let range = IpMatcher::parse("192.168.1.1-192.168.1.1").unwrap();
        assert!(range.matches_ip_text("192.168.1.1"));
        assert!(!range.matches_ip_text("192.168.1.2"));

        let cidr = IpMatcher::parse("192.168.1.1/32").unwrap();
        assert!(cidr.matches_ip_text("192.168.1.1"));
        assert!(!cidr.matches_ip_text("192.168.1.2"));
    }

    #[test]
    fn line_pattern_matches_wildcard_only_pattern() {
        assert!(line_matches_pattern("anything", "<_>"));
        assert!(!line_matches_pattern("anything", ""));
    }
}

mod comparison_op;
mod field_filter;
mod field_filter_chain;
mod field_filter_expression;
mod field_filter_expression_to_pipeline_stage;
mod field_filter_logic_op;
mod field_value;
mod ip_candidate_tokens;
mod ip_family;
mod ip_matcher;
mod ip_range;
mod ip_to_value;
mod line_filter;
mod line_filter_op;
mod line_matches_pattern;
mod parse_ip_addr;

pub use comparison_op::ComparisonOp;
pub use field_filter::FieldFilter;
pub use field_filter_chain::FieldFilterChain;
pub use field_filter_expression::FieldFilterExpression;
pub(crate) use field_filter_expression_to_pipeline_stage::field_filter_expression_to_pipeline_stage;
pub use field_filter_logic_op::FieldFilterLogicOp;
pub use field_value::FieldValue;
use ip_candidate_tokens::ip_candidate_tokens;
use ip_family::IpFamily;
pub use ip_matcher::IpMatcher;
use ip_range::IpRange;
use ip_to_value::ip_to_value;
pub use line_filter::LineFilter;
pub use line_filter_op::LineFilterOp;
use line_matches_pattern::line_matches_pattern;
use parse_ip_addr::parse_ip_addr;
