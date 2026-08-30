use std::{cmp::Ordering, collections::BTreeMap, fmt::Write as _};

use base64::{Engine as _, prelude::BASE64_STANDARD};
use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use krabka_units::convert::ByteSizeExt as _;
use regex::{NoExpand, Regex};
use time::OffsetDateTime;

use crate::{
    Labels, ParseError,
    util::{format_decimal_ratio, parse_bytes_literal, parse_prometheus_duration_literal},
};

#[cfg(test)]
mod tests {

    /// `evaluate_template_function` is the whole Go-template function surface
    /// `LogQL` exposes through `line_format`, and it carried by far the largest
    /// concentration of surviving mutants in this crate: thirty-nine dispatch
    /// arms, each an arithmetic or comparison the tests never pinned to a
    /// value. An arm that returns the wrong operand, or a comparison read the
    /// other way, still renders a plausible line.
    ///
    /// Inputs are chosen so no two operations agree: 7 and 3 separate add from
    /// sub, mul from div, min from max, and mod from all of them.
    #[test]
    fn template_functions_render_their_documented_results() {
        let s = |v: &str| TemplateRuntimeValue::String(v.to_string());

        let cases: &[(&str, Vec<TemplateRuntimeValue>, &str)] = &[
            ("add", vec![s("7"), s("3")], "10"),
            ("sub", vec![s("7"), s("3")], "4"),
            ("mul", vec![s("7"), s("3")], "21"),
            ("div", vec![s("7"), s("3")], "2"),
            ("mod", vec![s("7"), s("3")], "1"),
            ("max", vec![s("7"), s("3")], "7"),
            ("min", vec![s("7"), s("3")], "3"),
            ("addf", vec![s("7.5"), s("3.25")], "10.75"),
            ("mulf", vec![s("2.5"), s("4")], "10"),
            ("divf", vec![s("7.5"), s("2.5")], "3"),
            ("maxf", vec![s("2.5"), s("7.5")], "7.5"),
            ("minf", vec![s("2.5"), s("7.5")], "2.5"),
            ("ceil", vec![s("2.1")], "3"),
            ("floor", vec![s("2.9")], "2"),
            // `int` parses an integer, it does not truncate a float.
            ("int", vec![s("42")], "42"),
            ("len", vec![s("abcd")], "4"),
            // Comparisons: both polarities, so a flipped operator shows.
            ("eq", vec![s("a"), s("a")], "true"),
            ("eq", vec![s("a"), s("b")], "false"),
            ("ne", vec![s("a"), s("b")], "true"),
            ("ne", vec![s("a"), s("a")], "false"),
            ("lt", vec![s("3"), s("7")], "true"),
            ("lt", vec![s("7"), s("3")], "false"),
            ("gt", vec![s("7"), s("3")], "true"),
            ("gt", vec![s("3"), s("7")], "false"),
            // Boundaries, where `<` and `<=` part company.
            ("le", vec![s("3"), s("3")], "true"),
            ("lt", vec![s("3"), s("3")], "false"),
            ("ge", vec![s("3"), s("3")], "true"),
            ("gt", vec![s("3"), s("3")], "false"),
        ];

        for (name, args, want) in cases {
            let got = evaluate_template_function(name, args).as_rendered_string();
            assert2::check!(got == *want, "{name}({args:?})");
        }
    }
    use std::{cmp::Ordering, collections::BTreeMap};

    use super::{
        LineFormat, TemplatePart, TemplateRuntimeValue, ensure_template_parenthesized_token,
        ensure_template_quoted_token, evaluate_template_function, evaluate_template_index,
        evaluate_template_slice, format_go_time_layout, format_template_bytes,
        format_template_date, format_template_float, format_template_float_round,
        format_template_integer_binary, format_template_ordering, format_template_to_date,
        format_template_to_date_in_zone, is_template_control_assignment_variable_char,
        is_template_variable_name_char_invalid, js_escape_template_string,
        parse_go_time_layout_value, parse_template_fractional_nanoseconds,
        parse_template_parenthesized_token, parse_template_parts, parse_template_quoted_token,
        parse_template_timezone_offset, parse_variable_template_digits,
        push_template_unicode_escape, quoted_template_token_value,
        skip_leading_template_whitespace, substring_template_string, template_compare_values,
        template_index_value, template_slice_bounds, template_value_is_collection,
        tokenize_template_command, trim_template_body_end, urldecode_template_string,
    };

    #[test]
    fn template_helpers_trim_body_suffixes_and_literal_boundaries() {
        assert_eq!(trim_template_body_end("prefix body \n\t", 7, 14), 11);
        assert_eq!(trim_template_body_end("prefix \n\t", 7, 9), 7);

        assert!(parse_template_parts("").unwrap().is_empty());
        assert_eq!(
            parse_template_parts("literal").unwrap(),
            vec![TemplatePart::Literal("literal".to_string())]
        );
    }

    #[test]
    fn template_helpers_skip_leading_whitespace_from_current_position() {
        assert_eq!(skip_leading_template_whitespace("abc \n\tdef", 3), 6);
        assert_eq!(skip_leading_template_whitespace("abc", 1), 1);
        assert_eq!(skip_leading_template_whitespace("abc", 10), 3);
    }

    #[test]
    fn template_helpers_classify_invalid_variable_boundaries() {
        for (ch, expected) in [('|', true), (' ', true), ('\n', true), ('_', false)] {
            assert_eq!(
                is_template_control_assignment_variable_char(ch),
                expected,
                "control-assignment boundary: {ch:?}"
            );
        }

        for (ch, expected) in [('.', true), (' ', true), ('\t', true), ('_', false)] {
            assert_eq!(
                is_template_variable_name_char_invalid(ch),
                expected,
                "variable-name boundary: {ch:?}"
            );
        }
    }

    #[test]
    fn template_token_guards_reject_non_advancing_or_unwrapped_results() {
        for (token, end, expected_ok) in [
            ("`ok`", 4, true),
            ("`ok`", 0, false),
            ("`ok`", 5, false),
            ("`ok", 3, false),
        ] {
            assert_eq!(
                ensure_template_quoted_token("`ok`", 0, token, end, '`').is_ok(),
                expected_ok,
                "quoted token {token:?} ending at {end}"
            );
        }

        for (token, end, expected_ok) in [
            ("(ok)", 4, true),
            ("(ok)", 0, false),
            ("(ok)", 5, false),
            ("ok)", 3, false),
            ("(ok", 3, false),
        ] {
            assert_eq!(
                ensure_template_parenthesized_token("(ok)", 0, token, end).is_ok(),
                expected_ok,
                "parenthesized token {token:?} ending at {end}"
            );
        }
    }

    #[test]
    fn template_parenthesized_tokens_ignore_parentheses_inside_strings() {
        assert_eq!(
            parse_template_parenthesized_token(r#"(printf "a)b") tail"#, 0).unwrap(),
            (r#"(printf "a)b")"#.to_string(), 14)
        );
        assert_eq!(
            parse_template_parenthesized_token(r"(printf `c)d`) tail", 0).unwrap(),
            ("(printf `c)d`)".to_string(), 14)
        );
        assert_eq!(
            tokenize_template_command(r#"print (printf "a)b") (printf `c)d`)"#).unwrap(),
            vec![
                "print".to_string(),
                r#"(printf "a)b")"#.to_string(),
                "(printf `c)d`)".to_string(),
            ]
        );
    }

    #[test]
    fn template_quoted_tokens_advance_from_the_opening_quote() {
        assert_eq!(
            parse_template_quoted_token(r#"x "a\"b" tail"#, 2, '"').unwrap(),
            (r#""a\"b""#.to_string(), 8)
        );
        assert_eq!(
            parse_template_quoted_token("x `a b` tail", 2, '`').unwrap(),
            ("`a b`".to_string(), 7)
        );
        assert_eq!(
            tokenize_template_command(r#"print "a b" `c d`"#).unwrap(),
            vec![
                "print".to_string(),
                r#""a b""#.to_string(),
                "`c d`".to_string(),
            ]
        );
    }

    #[test]
    fn quoted_template_token_values_require_matching_wrappers() {
        assert_eq!(quoted_template_token_value("abc").unwrap(), None);
        assert_eq!(quoted_template_token_value("`").unwrap(), None);
        assert_eq!(quoted_template_token_value("\"").unwrap(), None);
        assert_eq!(
            quoted_template_token_value("``").unwrap(),
            Some(String::new())
        );
        assert_eq!(
            quoted_template_token_value("\"\"").unwrap(),
            Some(String::new())
        );
        assert_eq!(quoted_template_token_value("`unterminated").unwrap(), None);
        assert_eq!(quoted_template_token_value("unterminated`").unwrap(), None);
        assert_eq!(quoted_template_token_value("\"unterminated").unwrap(), None);
        assert_eq!(quoted_template_token_value("unterminated\"").unwrap(), None);
    }

    #[test]
    fn template_trim_right_allows_adjacent_literal() {
        let format = LineFormat::new(r#"{{ "ok" -}}tail"#).unwrap();
        assert_eq!(format.render("line", &BTreeMap::new()), "oktail");
    }

    #[test]
    fn template_helpers_tolerate_missing_arguments() {
        for (template, expected) in [
            ("{{ alignLeft 5 }}", ""),
            ("{{ alignRight 5 }}", ""),
            ("{{ replace \"a\" \"b\" }}", ""),
            ("{{ default \"fallback\" }}", "fallback"),
            ("{{ contains \"needle\" }}", "false"),
            ("{{ eq \"x\" }}", "false"),
            ("{{ ne \"x\" }}", "false"),
            ("{{ hasPrefix \"api\" }}", "false"),
            ("{{ hasSuffix \"api\" }}", "false"),
            ("{{ indent 2 }}", ""),
            ("{{ nindent 2 }}", ""),
            ("{{ repeat 3 }}", ""),
            ("{{ count \"o\" }}", ""),
            ("{{ regexReplaceAll \"o\" \"foo\" }}", ""),
            ("{{ regexReplaceAllLiteral \"o\" \"foo\" }}", ""),
            ("{{ trunc 3 }}", ""),
            ("{{ substr 1 3 }}", ""),
            ("{{ trimAll \"/\" }}", ""),
            ("{{ trimPrefix \"/\" }}", ""),
            ("{{ trimSuffix \"/\" }}", ""),
        ] {
            let format = LineFormat::new(template).unwrap();
            assert_eq!(
                format.render("raw", &BTreeMap::new()),
                expected,
                "template should tolerate missing args: {template}"
            );
        }
    }

    #[test]
    fn template_numeric_helpers_cover_missing_and_non_finite_inputs() {
        let one = vec!["9".to_string()];
        let two = vec!["9".to_string(), "4".to_string()];

        assert_eq!(
            format_template_integer_binary(&one, |left, right| Some(left - right)),
            ""
        );
        assert_eq!(
            format_template_integer_binary(&two, |left, right| Some(left - right)),
            "5"
        );
        assert_eq!(format_template_ordering(&one, Ordering::is_lt), "false");
        assert_eq!(format_template_ordering(&two, Ordering::is_gt), "true");

        assert_eq!(template_compare_values("NaN", "2"), Some(Ordering::Greater));
        assert_eq!(template_compare_values("1", "inf"), Some(Ordering::Less));
    }

    #[test]
    fn template_collection_helpers_index_and_slice_strings() {
        let plain = TemplateRuntimeValue::String("abc".to_string());
        let json_string = TemplateRuntimeValue::Json(serde_json::Value::String("xyz".to_string()));
        let scalar = TemplateRuntimeValue::Integer(7);

        for (value, expected) in [(&plain, true), (&json_string, true), (&scalar, false)] {
            assert_eq!(
                template_value_is_collection(value),
                expected,
                "collection check: {value:?}"
            );
        }

        assert_eq!(
            template_index_value(&plain, "1"),
            Some(TemplateRuntimeValue::Integer(i64::from(b'b')))
        );
        assert_eq!(
            template_index_value(&json_string, "1"),
            Some(TemplateRuntimeValue::Integer(i64::from(b'y')))
        );
        assert_eq!(template_index_value(&scalar, "0"), None);

        assert_eq!(
            evaluate_template_index(&[
                plain.clone(),
                TemplateRuntimeValue::String("2".to_string())
            ]),
            TemplateRuntimeValue::Integer(i64::from(b'c'))
        );
        assert_eq!(
            evaluate_template_index(&[
                json_string.clone(),
                TemplateRuntimeValue::String("0".to_string())
            ]),
            TemplateRuntimeValue::Integer(i64::from(b'x'))
        );
        assert_eq!(
            evaluate_template_slice(&[
                json_string,
                TemplateRuntimeValue::String("1".to_string()),
                TemplateRuntimeValue::String("3".to_string()),
            ]),
            TemplateRuntimeValue::String("yz".to_string())
        );
    }

    #[test]
    fn template_slice_bounds_validate_length_capacity_and_order() {
        let bounds = |values: &[usize]| {
            values
                .iter()
                .map(|value| TemplateRuntimeValue::String(value.to_string()))
                .collect::<Vec<_>>()
        };

        assert_eq!(template_slice_bounds(5, &bounds(&[0, 2, 5])), Some((0, 2)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 2, 5, 5])), None);

        assert_eq!(template_slice_bounds(5, &bounds(&[0, 3, 3])), Some((0, 3)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 5, 5])), Some((0, 5)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 4, 3])), None);
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 3, 6])), None);

        assert_eq!(template_slice_bounds(5, &bounds(&[4, 2])), None);
        assert_eq!(template_slice_bounds(5, &bounds(&[1, 6])), None);
    }

    #[test]
    fn template_float_and_bytes_formatting_preserves_edge_cases() {
        let args = |values: &[&str]| {
            values
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
        };

        assert_eq!(format_template_float(-0.0), "0");
        assert_eq!(format_template_float_round(&args(&["1"])), "");
        assert_eq!(format_template_float_round(&args(&["-1.6", "0"])), "-2");
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "0.5"])),
            "1.2"
        );
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "NaN"])),
            ""
        );
        assert_eq!(format_template_float_round(&args(&["1.2", "400"])), "");

        assert_eq!(format_template_bytes("1.5"), "1.5");
        assert_eq!(format_template_bytes("1kB"), "1000");
        assert_eq!(
            format_template_bytes("100000000000000000000"),
            "100000000000000000000"
        );
    }

    #[test]
    fn template_date_helpers_accept_extra_args_and_cover_layout_tokens() {
        let args = |values: &[&str]| {
            values
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
        };
        let timestamp_ns = "1704197045123456789";

        assert_eq!(
            format_template_date(&args(&[
                "2006-01-02T15:04:05.000000000Z07:00",
                timestamp_ns,
                "ignored",
            ])),
            "2024-01-02T12:04:05.123456789Z"
        );
        assert_eq!(
            format_template_to_date(&args(&[
                "2006-01-02T15:04:05.999999999 -07:00",
                "2024-01-02T12:04:05.123456789 +00:00",
                "ignored",
            ])),
            timestamp_ns
        );
        assert_eq!(
            format_template_to_date_in_zone(&args(&[
                "2006-01-02 15:04:05",
                "America/New_York",
                "2024-01-02 07:04:05",
                "ignored",
            ])),
            "1704197045000000000"
        );
        assert_eq!(format_template_date(&args(&["2006"])), "");
        assert_eq!(format_template_to_date(&args(&["2006"])), "");
        assert_eq!(format_template_to_date_in_zone(&args(&["2006", "UTC"])), "");
    }

    #[test]
    fn go_time_layout_helpers_format_and_parse_each_supported_token() {
        let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(1_704_197_045_123_456_789)
            .expect("test timestamp should be valid");

        assert_eq!(
            format_go_time_layout(
                "2006|06|15|04|05|01|1|02|2|Z07:00|-07:00|.000000000|.",
                timestamp,
            ),
            "2024|24|12|04|05|01|1|02|2|Z|+00:00|.123456789|."
        );

        let parsed = parse_go_time_layout_value(
            "06|1|2|15|04|05|.999999999|Z07:00|-07:00|.",
            "24|7|8|09|10|11|.123456789|+02:30|-03:15|.",
        )
        .expect("layout value should parse");
        assert_eq!(parsed.year, 2024);
        assert_eq!(parsed.month, 7);
        assert_eq!(parsed.day, 8);
        assert_eq!(parsed.hour, 9);
        assert_eq!(parsed.minute, 10);
        assert_eq!(parsed.second, 11);
        assert_eq!(parsed.nanosecond, 123_456_789);
        assert_eq!(parsed.offset_seconds, Some(-11_700));
    }

    #[test]
    fn go_time_low_level_parsers_consume_expected_widths() {
        let mut pos = 0;
        assert_eq!(
            parse_variable_template_digits("123x", &mut pos, 2),
            Some(12)
        );
        assert_eq!(pos, 2);

        pos = 0;
        assert_eq!(parse_variable_template_digits("x12", &mut pos, 2), None);
        assert_eq!(pos, 0);

        pos = 0;
        assert_eq!(
            parse_template_fractional_nanoseconds(".1234x", &mut pos, 3),
            Some(123_000_000)
        );
        assert_eq!(pos, 4);

        pos = 0;
        assert_eq!(
            parse_template_fractional_nanoseconds(".x", &mut pos, 3),
            None
        );
        assert_eq!(pos, 1);

        pos = 0;
        assert_eq!(parse_template_timezone_offset("Z!", &mut pos), Some(0));
        assert_eq!(pos, 1);

        pos = 0;
        assert_eq!(
            parse_template_timezone_offset("+02:30", &mut pos),
            Some(9_000)
        );
        assert_eq!(pos, 6);

        pos = 0;
        assert_eq!(
            parse_template_timezone_offset("+12:34", &mut pos),
            Some(45_240)
        );
        assert_eq!(pos, 6);

        pos = 1;
        assert_eq!(
            parse_template_timezone_offset("x-03:15", &mut pos),
            Some(-11_700)
        );
        assert_eq!(pos, 7);

        pos = 0;
        assert_eq!(parse_template_timezone_offset("UTC", &mut pos), None);
        assert_eq!(pos, 0);
    }

    #[test]
    fn template_string_escape_helpers_cover_special_bytes() {
        assert_eq!(substring_template_string("abcdef", 2, 0), "");
        assert_eq!(substring_template_string("abcdef", 2, -1), "cdef");

        assert_eq!(
            js_escape_template_string("\\'\"\n\r\t\u{2028}\u{2029}\u{0001}"),
            r#"\\\'\"\u000A\u000D\u0009\u2028\u2029\u0001"#
        );

        let mut escaped = "prefix".to_string();
        push_template_unicode_escape(&mut escaped, 0x1f);
        assert_eq!(escaped, r"prefix\u001F");

        assert_eq!(urldecode_template_string("%7a%2F%3f%zz%A"), "z/?%zz%A");
    }
}

mod advance_template_pos;
mod align_left_template_string;
mod align_right_template_string;
mod consume_template_printf_number;
mod current_template_timestamp;
mod decode_quoted_fragment;
mod ensure_template_parenthesized_token;
mod ensure_template_quoted_token;
mod epoch_template_timestamp;
mod evaluate_common_string_function;
mod evaluate_template_function;
mod evaluate_template_index;
mod evaluate_template_slice;
mod evaluate_template_value_function;
mod find_template_control_action;
mod format_go_time_layout;
mod format_template_bytes;
mod format_template_date;
mod format_template_duration_seconds;
mod format_template_float;
mod format_template_float_fold;
mod format_template_float_min_max;
mod format_template_float_product;
mod format_template_float_round;
mod format_template_float_sum;
mod format_template_float_unary;
mod format_template_integer_binary;
mod format_template_integer_min_max;
mod format_template_integer_product;
mod format_template_integer_sum;
mod format_template_ordering;
mod format_template_print;
mod format_template_printf;
mod format_template_printf_string;
mod format_template_to_date;
mod format_template_to_date_in_zone;
mod hex_digit;
mod html_escape_template_string;
mod indent_template_string;
mod is_template_comment_action;
mod is_template_control_assignment_variable_char;
mod is_template_function_name;
mod is_template_variable_name_char_invalid;
mod is_unexpected_template_control_action;
mod is_wrapped_template_token;
mod js_escape_template_string;
mod line_format;
mod match_template_literal;
mod parse_fixed_template_digits;
mod parse_go_time_layout_to_unix_nanos;
mod parse_go_time_layout_value;
mod parse_template_action;
mod parse_template_assignment;
mod parse_template_bound;
mod parse_template_conditional;
mod parse_template_control_assignment;
mod parse_template_float;
mod parse_template_fractional_nanoseconds;
mod parse_template_integer;
mod parse_template_parenthesized_token;
mod parse_template_parts;
mod parse_template_quoted_token;
mod parse_template_range;
mod parse_template_range_expression;
mod parse_template_timezone_offset;
mod parse_template_variable_name;
mod parse_template_with;
mod parse_variable_template_digits;
mod parsed_template_action;
mod parsed_template_date;
mod push_template_unicode_escape;
mod quoted_template_token_value;
mod render_template_parts;
mod resolve_template_datetime;
mod skip_leading_template_whitespace;
mod split_template_pipeline;
mod substring_template_string;
mod template_action_trim_left;
mod template_action_trim_right;
mod template_assignment;
mod template_collection_first_args;
mod template_command;
mod template_compare_values;
mod template_conditional;
mod template_control_action;
mod template_control_expression;
mod template_current_dot_field_value;
mod template_expression;
mod template_float_args;
mod template_index_value;
mod template_integer_args;
mod template_json_value_to_string;
mod template_json_value_truthy;
mod template_parse_error;
mod template_part;
mod template_range;
mod template_range_binding;
mod template_render_context;
mod template_root_field_value;
mod template_runtime_value;
mod template_slice_array;
mod template_slice_bounds;
mod template_slice_string;
mod template_string_truthy;
mod template_value;
mod template_value_is_collection;
mod template_variable_path_value;
mod template_with;
mod title_template_string;
mod tokenize_template_command;
mod trim_template_body_end;
mod truncate_template_string;
mod unix_to_template_timestamp;
mod urldecode_template_string;
mod urlencode_template_string;
mod urlquery_template_string;

use advance_template_pos::advance_template_pos;
use align_left_template_string::align_left_template_string;
use align_right_template_string::align_right_template_string;
use consume_template_printf_number::consume_template_printf_number;
use current_template_timestamp::current_template_timestamp;
use decode_quoted_fragment::decode_quoted_fragment;
use ensure_template_parenthesized_token::ensure_template_parenthesized_token;
use ensure_template_quoted_token::ensure_template_quoted_token;
use epoch_template_timestamp::epoch_template_timestamp;
use evaluate_common_string_function::evaluate_common_string_function;
use evaluate_template_function::evaluate_template_function;
use evaluate_template_index::evaluate_template_index;
use evaluate_template_slice::evaluate_template_slice;
use evaluate_template_value_function::evaluate_template_value_function;
use find_template_control_action::find_template_control_action;
use format_go_time_layout::format_go_time_layout;
use format_template_bytes::format_template_bytes;
use format_template_date::format_template_date;
use format_template_duration_seconds::format_template_duration_seconds;
use format_template_float::format_template_float;
use format_template_float_fold::format_template_float_fold;
use format_template_float_min_max::format_template_float_min_max;
use format_template_float_product::format_template_float_product;
use format_template_float_round::format_template_float_round;
use format_template_float_sum::format_template_float_sum;
use format_template_float_unary::format_template_float_unary;
use format_template_integer_binary::format_template_integer_binary;
use format_template_integer_min_max::format_template_integer_min_max;
use format_template_integer_product::format_template_integer_product;
use format_template_integer_sum::format_template_integer_sum;
use format_template_ordering::format_template_ordering;
use format_template_print::format_template_print;
use format_template_printf::format_template_printf;
use format_template_printf_string::format_template_printf_string;
use format_template_to_date::format_template_to_date;
use format_template_to_date_in_zone::format_template_to_date_in_zone;
use hex_digit::hex_digit;
use html_escape_template_string::html_escape_template_string;
use indent_template_string::indent_template_string;
use is_template_comment_action::is_template_comment_action;
use is_template_control_assignment_variable_char::is_template_control_assignment_variable_char;
use is_template_function_name::is_template_function_name;
use is_template_variable_name_char_invalid::is_template_variable_name_char_invalid;
use is_unexpected_template_control_action::is_unexpected_template_control_action;
use is_wrapped_template_token::is_wrapped_template_token;
use js_escape_template_string::js_escape_template_string;
pub use line_format::LineFormat;
use match_template_literal::match_template_literal;
use parse_fixed_template_digits::parse_fixed_template_digits;
use parse_go_time_layout_to_unix_nanos::parse_go_time_layout_to_unix_nanos;
use parse_go_time_layout_value::parse_go_time_layout_value;
use parse_template_action::parse_template_action;
use parse_template_assignment::parse_template_assignment;
use parse_template_bound::parse_template_bound;
use parse_template_conditional::parse_template_conditional;
use parse_template_control_assignment::parse_template_control_assignment;
use parse_template_float::parse_template_float;
use parse_template_fractional_nanoseconds::parse_template_fractional_nanoseconds;
use parse_template_integer::parse_template_integer;
use parse_template_parenthesized_token::parse_template_parenthesized_token;
use parse_template_parts::parse_template_parts;
use parse_template_quoted_token::parse_template_quoted_token;
use parse_template_range::parse_template_range;
use parse_template_range_expression::parse_template_range_expression;
use parse_template_timezone_offset::parse_template_timezone_offset;
use parse_template_variable_name::parse_template_variable_name;
use parse_template_with::parse_template_with;
use parse_variable_template_digits::parse_variable_template_digits;
use parsed_template_action::ParsedTemplateAction;
use parsed_template_date::ParsedTemplateDate;
use push_template_unicode_escape::push_template_unicode_escape;
use quoted_template_token_value::quoted_template_token_value;
use render_template_parts::render_template_parts;
use resolve_template_datetime::resolve_template_datetime;
use skip_leading_template_whitespace::skip_leading_template_whitespace;
use split_template_pipeline::split_template_pipeline;
use substring_template_string::substring_template_string;
use template_action_trim_left::template_action_trim_left;
use template_action_trim_right::template_action_trim_right;
use template_assignment::TemplateAssignment;
use template_collection_first_args::template_collection_first_args;
use template_command::TemplateCommand;
use template_compare_values::template_compare_values;
use template_conditional::TemplateConditional;
use template_control_action::{TemplateControlAction, template_control_action};
use template_control_expression::TemplateControlExpression;
use template_current_dot_field_value::template_current_dot_field_value;
use template_expression::TemplateExpression;
use template_float_args::template_float_args;
use template_index_value::template_index_value;
use template_integer_args::template_integer_args;
use template_json_value_to_string::template_json_value_to_string;
use template_json_value_truthy::template_json_value_truthy;
pub(crate) use template_parse_error::template_parse_error;
use template_part::TemplatePart;
use template_range::TemplateRange;
use template_range_binding::TemplateRangeBinding;
use template_render_context::TemplateRenderContext;
use template_root_field_value::template_root_field_value;
use template_runtime_value::TemplateRuntimeValue;
use template_slice_array::template_slice_array;
use template_slice_bounds::template_slice_bounds;
use template_slice_string::template_slice_string;
use template_string_truthy::template_string_truthy;
use template_value::TemplateValue;
use template_value_is_collection::template_value_is_collection;
use template_variable_path_value::template_variable_path_value;
use template_with::TemplateWith;
use title_template_string::title_template_string;
use tokenize_template_command::tokenize_template_command;
use trim_template_body_end::trim_template_body_end;
use truncate_template_string::truncate_template_string;
use unix_to_template_timestamp::unix_to_template_timestamp;
use urldecode_template_string::urldecode_template_string;
use urlencode_template_string::urlencode_template_string;
use urlquery_template_string::urlquery_template_string;
