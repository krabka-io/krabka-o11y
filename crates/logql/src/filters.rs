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
    use crate::test_support::{LabelPair, labels};

    struct FieldExpressionMatchCase<'a> {
        pairs: [LabelPair<'a>; 2],
        expected: bool,
    }

    struct InvalidFieldFilterCase {
        name: &'static str,
        operation: ComparisonOp,
        value: FieldValue,
    }

    struct NumberComparisonCase {
        operation: ComparisonOp,
        candidate: f64,
        expected: f64,
        result: bool,
    }

    struct StringComparisonCase {
        operation: ComparisonOp,
        candidate: &'static str,
        expected: &'static str,
        result: bool,
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

        assert!(filter.matches(&labels(&[LabelPair::new("status", "500")])));
        assert!(!filter.matches(&labels(&[LabelPair::new("status", "499")])));
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
            rest: vec![FieldFilterStep {
                operation: FieldFilterLogicOp::Or,
                filter: FieldFilterExpression::Filter(string_filter(
                    "level",
                    ComparisonOp::Equal,
                    "warn",
                )),
            }],
        };

        for case in [
            FieldExpressionMatchCase {
                pairs: [
                    LabelPair::new("status", "500"),
                    LabelPair::new("level", "info"),
                ],
                expected: true,
            },
            FieldExpressionMatchCase {
                pairs: [
                    LabelPair::new("status", "200"),
                    LabelPair::new("level", "warn"),
                ],
                expected: true,
            },
            FieldExpressionMatchCase {
                pairs: [
                    LabelPair::new("status", "200"),
                    LabelPair::new("level", "info"),
                ],
                expected: false,
            },
        ] {
            assert_eq!(
                expression.matches(&labels(&case.pairs)),
                case.expected,
                "{:?}",
                case.pairs
            );
        }
    }

    #[test]
    fn field_filter_chain_matches_and_exposes_rest_filters() {
        let rest_filter = string_filter("path", ComparisonOp::NotEqual, "/health");
        let chain = FieldFilterChain::new(
            number_filter("status", ComparisonOp::GreaterEqual, 500.0),
            vec![FieldFilterStep {
                operation: FieldFilterLogicOp::And,
                filter: rest_filter.clone(),
            }],
        );

        assert!(chain.matches(&labels(&[
            LabelPair::new("status", "500"),
            LabelPair::new("path", "/checkout"),
        ])));
        assert!(!chain.matches(&labels(&[
            LabelPair::new("status", "500"),
            LabelPair::new("path", "/health"),
        ])));
        assert_eq!(
            chain.rest(),
            &[FieldFilterStep {
                operation: FieldFilterLogicOp::And,
                filter: rest_filter,
            }]
        );
    }

    #[test]
    fn field_filter_validation_rejects_invalid_combinations() {
        for case in [
            InvalidFieldFilterCase {
                name: "path",
                operation: ComparisonOp::RegexEqual,
                value: FieldValue::String("[".to_string()),
            },
            InvalidFieldFilterCase {
                name: "path",
                operation: ComparisonOp::RegexEqual,
                value: FieldValue::Number(1.0),
            },
            InvalidFieldFilterCase {
                name: "remote_addr",
                operation: ComparisonOp::Greater,
                value: FieldValue::Ip(IpMatcher::parse("192.168.1.1").unwrap()),
            },
        ] {
            assert!(
                FieldFilter::try_new(case.name, case.operation, case.value.clone()).is_err(),
                "{} {:?} {:?}",
                case.name,
                case.operation,
                case.value
            );
        }
    }

    #[test]
    fn comparison_ops_compare_number_boundaries() {
        for case in [
            NumberComparisonCase {
                operation: ComparisonOp::Equal,
                candidate: 500.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::Equal,
                candidate: 499.0,
                expected: 500.0,
                result: false,
            },
            NumberComparisonCase {
                operation: ComparisonOp::NotEqual,
                candidate: 499.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::NotEqual,
                candidate: 500.0,
                expected: 500.0,
                result: false,
            },
            NumberComparisonCase {
                operation: ComparisonOp::Greater,
                candidate: 501.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::Greater,
                candidate: 500.0,
                expected: 500.0,
                result: false,
            },
            NumberComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: 501.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: 500.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: 499.0,
                expected: 500.0,
                result: false,
            },
            NumberComparisonCase {
                operation: ComparisonOp::Less,
                candidate: 499.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::Less,
                candidate: 500.0,
                expected: 500.0,
                result: false,
            },
            NumberComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: 499.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: 500.0,
                expected: 500.0,
                result: true,
            },
            NumberComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: 501.0,
                expected: 500.0,
                result: false,
            },
        ] {
            assert_eq!(
                case.operation
                    .compare_numbers(case.candidate, case.expected),
                case.result,
                "{} {:?} {}",
                case.candidate,
                case.operation,
                case.expected
            );
        }
    }

    #[test]
    fn comparison_ops_compare_string_boundaries() {
        for case in [
            StringComparisonCase {
                operation: ComparisonOp::Greater,
                candidate: "n",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::Greater,
                candidate: "m",
                expected: "m",
                result: false,
            },
            StringComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: "n",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: "m",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::GreaterEqual,
                candidate: "l",
                expected: "m",
                result: false,
            },
            StringComparisonCase {
                operation: ComparisonOp::Less,
                candidate: "l",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::Less,
                candidate: "m",
                expected: "m",
                result: false,
            },
            StringComparisonCase {
                operation: ComparisonOp::Less,
                candidate: "n",
                expected: "m",
                result: false,
            },
            StringComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: "l",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: "m",
                expected: "m",
                result: true,
            },
            StringComparisonCase {
                operation: ComparisonOp::LessEqual,
                candidate: "n",
                expected: "m",
                result: false,
            },
        ] {
            assert_eq!(
                case.operation
                    .compare_strings(case.candidate, case.expected),
                case.result,
                "{} {:?} {}",
                case.candidate,
                case.operation,
                case.expected
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
mod field_filter_logic_op;
mod field_value;
mod ip_family;
mod ip_matcher;
mod ip_range;
mod line_filter;
mod line_filter_op;

pub use comparison_op::ComparisonOp;
pub use field_filter::FieldFilter;
pub use field_filter_chain::{FieldFilterChain, FieldFilterStep};
pub use field_filter_expression::FieldFilterExpression;
pub use field_filter_logic_op::FieldFilterLogicOp;
pub use field_value::FieldValue;
use ip_family::IpFamily;
pub use ip_matcher::IpMatcher;
use ip_range::IpRange;
pub use line_filter::LineFilter;
pub use line_filter_op::LineFilterOp;

pub(crate) fn ip_candidate_tokens(line: &str) -> impl Iterator<Item = &str> {
    line.split(|ch: char| !(ch.is_ascii_hexdigit() || ch == '.' || ch == ':'))
        .filter(|candidate| !candidate.is_empty())
}

pub(crate) fn line_matches_pattern(line: &str, pattern: &str) -> bool {
    let mut remaining = line;
    let mut matched_any_literal = false;
    for literal in pattern.split("<_>").filter(|literal| !literal.is_empty()) {
        let Some(offset) = remaining.find(literal) else {
            return false;
        };
        matched_any_literal = true;
        remaining = &remaining[offset + literal.len()..];
    }
    matched_any_literal || pattern.contains("<_>")
}

pub(crate) fn parse_ip_addr(value: &str) -> Result<IpAddr, ParseError> {
    value.parse().map_err(|_| ParseError::Syntax {
        message: "invalid ip pattern".to_string(),
        position: 0,
    })
}

// Field Filter Expression To Pipeline Stage
pub(crate) fn field_filter_expression_to_pipeline_stage(
    expression: FieldFilterExpression,
) -> PipelineStage {
    match expression {
        FieldFilterExpression::Filter(filter) => PipelineStage::FieldFilter(filter),
        FieldFilterExpression::Chain { first, rest } => {
            let first = match *first {
                FieldFilterExpression::Filter(filter) => filter,
                first => {
                    return PipelineStage::FieldFilterExpression(FieldFilterExpression::Chain {
                        first: Box::new(first),
                        rest,
                    });
                }
            };

            let mut flat_rest: Vec<FieldFilterStep<FieldFilter>> = Vec::new();
            for step in rest {
                let operation = step.operation;
                let filter = match step.filter {
                    FieldFilterExpression::Filter(filter) => filter,
                    expression => {
                        let first = Box::new(FieldFilterExpression::Filter(first));
                        let rest = flat_rest
                            .into_iter()
                            .map(|step| FieldFilterStep {
                                operation: step.operation,
                                filter: FieldFilterExpression::Filter(step.filter),
                            })
                            .chain(std::iter::once(FieldFilterStep {
                                operation,
                                filter: expression,
                            }))
                            .collect();
                        return PipelineStage::FieldFilterExpression(
                            FieldFilterExpression::Chain { first, rest },
                        );
                    }
                };
                flat_rest.push(FieldFilterStep { operation, filter });
            }

            PipelineStage::FieldFilterChain(FieldFilterChain::new(first, flat_rest))
        }
        expression @ FieldFilterExpression::Group(_) => {
            PipelineStage::FieldFilterExpression(expression)
        }
    }
}

// Ip To Value
pub(crate) struct IpValue {
    pub(crate) family: IpFamily,
    pub(crate) value: u128,
}

pub(crate) fn ip_to_value(addr: IpAddr) -> IpValue {
    match addr {
        IpAddr::V4(addr) => IpValue {
            family: IpFamily::V4,
            value: u128::from(u32::from(addr)),
        },
        IpAddr::V6(addr) => IpValue {
            family: IpFamily::V6,
            value: u128::from(addr),
        },
    }
}
