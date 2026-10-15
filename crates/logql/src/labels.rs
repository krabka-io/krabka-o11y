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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalSampleRatio {
    pub numerator: i128,
    pub denominator: u128,
}

struct SignedDigits<'a> {
    negative: bool,
    digits: &'a str,
}

struct DecimalMantissa<'a> {
    digits: &'a str,
    exponent: i32,
}

struct WholeAndFraction<'a> {
    whole: &'a str,
    fractional: &'a str,
}

/// Parses a decimal sample literal, such as `-1.25e3`, into an exact
/// `(numerator, denominator)` ratio.
///
/// The literal is an optional sign, ASCII digits with at most one `.`, and an
/// optional `e`/`E` exponent. Returns `None` for anything else, or when the
/// ratio overflows `i128`/`u128`.
#[must_use]
pub fn parse_decimal_sample_literal(value: &str) -> Option<DecimalSampleRatio> {
    if value.is_empty() {
        return None;
    }
    let signed_digits = match value.as_bytes().first() {
        Some(b'-') => SignedDigits {
            negative: true,
            digits: &value[1..],
        },
        Some(b'+') => SignedDigits {
            negative: false,
            digits: &value[1..],
        },
        _ => SignedDigits {
            negative: false,
            digits: value,
        },
    };
    let value = signed_digits.digits;
    let negative = signed_digits.negative;
    if value.is_empty() {
        return None;
    }

    let decimal_mantissa = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            DecimalMantissa {
                digits: &value[..index],
                exponent: parse_decimal_exponent(exponent_text)?,
            }
        }
        None => DecimalMantissa {
            digits: value,
            exponent: 0,
        },
    };
    let mantissa = decimal_mantissa.digits;
    let exponent = decimal_mantissa.exponent;
    if mantissa.is_empty() {
        return None;
    }

    let whole_and_fraction = match mantissa.split_once('.') {
        Some(parts) => WholeAndFraction {
            whole: parts.0,
            fractional: parts.1,
        },
        None => WholeAndFraction {
            whole: mantissa,
            fractional: "",
        },
    };
    let whole = whole_and_fraction.whole;
    let fractional = whole_and_fraction.fractional;
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let numerator = i128::try_from(numerator).ok()?;
    Some(DecimalSampleRatio {
        numerator: if negative { -numerator } else { numerator },
        denominator,
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::test_support::{LabelPair, labels};

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

        let mut fields = labels(&[
            LabelPair::new("app", "debug-api"),
            LabelPair::new("level", "warn"),
            LabelPair::new("status", "500"),
        ]);
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
    #[allow(clippy::items_after_statements)]
    fn label_selection_matches_requires_present_matching_value() {
        let bare = LabelSelection::name("level").unwrap();
        let exact = LabelSelection::equal("status", "500").unwrap();
        let regex = LabelSelection::regex("app", "api|worker").unwrap();
        let fields = labels(&[
            LabelPair::new("app", "api"),
            LabelPair::new("status", "500"),
        ]);
        let wrong_status = labels(&[LabelPair::new("status", "200")]);
        let frontend = labels(&[LabelPair::new("app", "frontend")]);

        struct SelectionCase<'a> {
            selection: &'a LabelSelection,
            candidate: &'a Labels,
            expected: bool,
        }

        for case in [
            SelectionCase {
                selection: &bare,
                candidate: &fields,
                expected: false,
            },
            SelectionCase {
                selection: &exact,
                candidate: &fields,
                expected: true,
            },
            SelectionCase {
                selection: &exact,
                candidate: &wrong_status,
                expected: false,
            },
            SelectionCase {
                selection: &regex,
                candidate: &fields,
                expected: true,
            },
            SelectionCase {
                selection: &regex,
                candidate: &frontend,
                expected: false,
            },
        ] {
            assert_eq!(
                case.selection.matches(case.candidate),
                case.expected,
                "{:?} against {:?}",
                case.selection,
                case.candidate
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

mod label_format;
mod label_format_assignment;
mod label_selection;
mod label_selection_set;
mod unwrap_expression;

pub use label_format::LabelFormat;
pub use label_format_assignment::LabelFormatAssignment;
pub use label_selection::LabelSelection;
pub use label_selection_set::LabelSelectionSet;
pub use unwrap_expression::UnwrapExpression;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelFormatValue {
    Rename(String),
    Template(LineFormat),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelSelectionMatcher {
    Equal(String),
    Regex(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnwrapConversion {
    Raw,
    Bytes,
    Duration,
}

pub(crate) fn parse_decimal_exponent(value: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    value.parse::<i32>().ok()
}

pub(crate) fn parse_raw_sample_literal(value: &str) -> Option<String> {
    let ratio = parse_decimal_sample_literal(value)?;
    let numerator = ratio.numerator;
    let negative = numerator < 0;
    let formatted = format_decimal_ratio(numerator.unsigned_abs(), ratio.denominator);
    Some(if negative {
        format!("-{formatted}")
    } else {
        formatted
    })
}
