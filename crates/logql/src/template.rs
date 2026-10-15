use std::{cmp::Ordering, collections::BTreeMap, fmt::Write as _};

use base64::{Engine as _, prelude::BASE64_STANDARD};
use regex::{NoExpand, Regex};
use time::OffsetDateTime;

use crate::{Labels, ParseError};

// Parse Template Action
pub(crate) fn parse_template_action(
    template: &str,
    open: usize,
) -> Result<ParsedTemplateAction<'_>, ParseError> {
    let mut expression_start = open + 2;
    let trim_left = template_action_trim_left(template, open)?;
    if trim_left {
        expression_start += 1;
    }
    let close_offset = action_end(&template[expression_start..])
        .ok_or_else(|| template_parse_error("expected closing template action"))?;
    let close = expression_start + close_offset;
    let trim_right = template_action_trim_right(template, expression_start, close);
    let expression_end = if trim_right { close - 1 } else { close };
    let untrimmed_next_pos = close + 2;
    let mut next_pos = untrimmed_next_pos;
    if trim_right {
        next_pos = skip_leading_template_whitespace(template, next_pos);
        if next_pos < untrimmed_next_pos {
            return Err(template_parse_error(
                "template action parser did not advance",
            ));
        }
    }
    Ok(ParsedTemplateAction {
        expression: template[expression_start..expression_end].trim(),
        next_pos,
        trim_left,
    })
}

fn action_end(value: &str) -> Option<usize> {
    if value.trim_start().starts_with("/*") {
        let end = value.find("*/")? + 2;
        return value[end..].find("}}").map(|offset| end + offset);
    }
    let mut quote = None;
    let mut escaped = false;
    let mut offset = 0;
    while offset < value.len() {
        let ch = value[offset..]
            .chars()
            .next()
            .expect("index is within value");
        if escaped {
            escaped = false;
            offset += ch.len_utf8();
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != '`' && ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
        } else if matches!(ch, '\"' | '\'' | '`') {
            quote = Some(ch);
        } else if value[offset..].starts_with("}}") {
            return Some(offset);
        }
        offset += ch.len_utf8();
    }
    None
}

// Parse Template Assignment
pub(crate) fn parse_template_assignment(
    expression: &str,
) -> Result<Option<TemplateAssignment>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let expression_declaration = expression.contains(":=");
    let parts = if let Some(parts) = expression.split_once(":=") {
        parts
    } else if let Some(parts) = expression.split_once('=') {
        if parts
            .0
            .trim()
            .contains(is_template_control_assignment_variable_char)
        {
            return Ok(None);
        }
        parts
    } else {
        return Ok(None);
    };
    let variable = parts.0;
    let expression = parts.1;
    let variable = parse_template_variable_name(variable.trim(), "expected template variable")?;
    Ok(Some(TemplateAssignment {
        variable,
        declare: expression_declaration,
        expression: TemplateExpression::parse(expression.trim())?,
    }))
}

// Parse Template Conditional
pub(crate) fn parse_template_conditional(
    template: &str,
    mut branch_start: usize,
    first_condition: &str,
) -> Result<ParsedTemplateValue<TemplateConditional>, ParseError> {
    let mut branches = Vec::new();
    let mut condition = TemplateControlExpression::parse(first_condition)?;
    loop {
        let Some(control) = find_template_control_action(template, branch_start)? else {
            return Err(template_parse_error("expected template end action"));
        };
        let branch_parts = parse_template_parts(&template[branch_start..control.body_end])?;
        if let Some(next_condition) = control.expression.strip_prefix("else if ") {
            branches.push(TemplateConditionalBranch {
                condition,
                parts: branch_parts,
            });
            condition = TemplateControlExpression::parse(next_condition.trim())?;
            branch_start = control.next_position;
            continue;
        }
        if control.expression == "else" {
            branches.push(TemplateConditionalBranch {
                condition,
                parts: branch_parts,
            });
            let Some(else_control) = find_template_control_action(template, control.next_position)?
            else {
                return Err(template_parse_error("expected template end action"));
            };
            if else_control.expression != "end" {
                return Err(template_parse_error("unexpected template control action"));
            }
            let else_parts =
                parse_template_parts(&template[control.next_position..else_control.body_end])?;
            return Ok(ParsedTemplateValue {
                value: TemplateConditional {
                    branches,
                    else_parts,
                },
                next_position: else_control.next_position,
            });
        }
        branches.push(TemplateConditionalBranch {
            condition,
            parts: branch_parts,
        });
        return Ok(ParsedTemplateValue {
            value: TemplateConditional {
                branches,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
}

// Parse Template Control Assignment
pub(crate) struct TemplateControlAssignment<'a> {
    pub(crate) variable: String,
    pub(crate) expression: &'a str,
}

pub(crate) fn parse_template_control_assignment(
    expression: &str,
) -> Result<Option<TemplateControlAssignment<'_>>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let Some(parts) = expression.split_once(":=") else {
        return Ok(None);
    };
    let variable = parts.0;
    let expression = parts.1;
    if variable
        .trim()
        .contains(is_template_control_assignment_variable_char)
    {
        return Ok(None);
    }
    Ok(Some(TemplateControlAssignment {
        variable: parse_template_variable_name(variable.trim(), "expected template variable")?,
        expression: expression.trim(),
    }))
}

// Parse Template Float
pub(crate) fn parse_template_float(value: &str) -> String {
    template_value::number::parse_float(value)
        .map_or_else(|| "0".to_string(), format_template_float)
}

// Parse Template Parenthesized Token
pub(crate) fn parse_template_parenthesized_token(
    command: &str,
    start: usize,
) -> Result<ParsedTemplateToken, ParseError> {
    let mut depth = 0usize;
    let mut quotes = TemplateQuoteScanner::default();
    let mut index = start;
    while index < command.len() {
        let ch = command[index..]
            .chars()
            .next()
            .expect("index is within command");
        if quotes.consume_quoted(ch) {
            index += ch.len_utf8();
            continue;
        }
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
                if depth == 0 {
                    return Ok(ParsedTemplateToken {
                        token: command[start..=index].to_string(),
                        next_position: index.saturating_add(ch.len_utf8()),
                    });
                }
            }
            _ => {}
        }
        index += ch.len_utf8();
    }
    Err(template_parse_error("unterminated template parenthesis"))
}

// Parse Template Quoted Token
pub(crate) fn parse_template_quoted_token(
    command: &str,
    start: usize,
    quote: char,
) -> Result<ParsedTemplateToken, ParseError> {
    let mut escaped = false;
    let value_start = start.saturating_add(quote.len_utf8());
    let mut index = value_start;
    while index < command.len() {
        let ch = command[index..]
            .chars()
            .next()
            .expect("index is within command");
        if escaped {
            escaped = false;
            index += ch.len_utf8();
            continue;
        }
        if matches!(quote, '"' | '\'') && ch == '\\' {
            escaped = true;
            index += ch.len_utf8();
            continue;
        }
        if ch == quote {
            return Ok(ParsedTemplateToken {
                token: command[start..=index].to_string(),
                next_position: index.saturating_add(quote.len_utf8()),
            });
        }
        index += ch.len_utf8();
    }
    Err(template_parse_error("unterminated template string"))
}

// Parse Template Range
pub(crate) fn parse_template_range(
    template: &str,
    body_start: usize,
    range_expression: &str,
) -> Result<ParsedTemplateValue<TemplateRange>, ParseError> {
    let parsed_expression = parse_template_range_expression(range_expression)?;
    let control = parse_template_control_body(template, body_start)?;
    if control.expression == "end" {
        return Ok(ParsedTemplateValue {
            value: TemplateRange {
                binding: parsed_expression.binding,
                expression: parsed_expression.expression,
                parts: control.parts,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
    if control.expression != "else" {
        return Err(template_parse_error("unexpected template control action"));
    }

    let parsed_else = parse_template_else_body(template, control.next_position)?;
    Ok(ParsedTemplateValue {
        value: TemplateRange {
            binding: parsed_expression.binding,
            expression: parsed_expression.expression,
            parts: control.parts,
            else_parts: parsed_else.value,
        },
        next_position: parsed_else.next_position,
    })
}

// Parsed Template Action
pub(crate) struct ParsedTemplateAction<'a> {
    pub(crate) expression: &'a str,
    pub(crate) next_pos: usize,
    pub(crate) trim_left: bool,
}

// Parsed Template Date
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
pub(crate) struct ParsedTemplateDate {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) nanosecond: u32,
    pub(crate) offset_seconds: Option<i32>,
}

// Template Assignment
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateAssignment {
    pub(crate) variable: String,
    pub(crate) declare: bool,
    pub(crate) expression: TemplateExpression,
}

// Template Conditional
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditional {
    pub(crate) branches: Vec<TemplateConditionalBranch>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditionalBranch {
    pub(crate) condition: TemplateControlExpression,
    pub(crate) parts: Vec<TemplatePart>,
}

// Template Part
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplatePart {
    Literal(String),
    Comment,
    Definition {
        name: String,
        parts: Vec<TemplatePart>,
        block: bool,
    },
    Invocation {
        name: String,
        argument: Option<TemplateExpression>,
    },
    Break,
    Continue,
    Expression(TemplateExpression),
    Conditional(TemplateConditional),
    Range(TemplateRange),
    With(TemplateWith),
    Assignment(TemplateAssignment),
}

// Template Range Binding
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateRangeBinding {
    Dot,
    Value(String),
    AssignValue(String),
    IndexValue { index: String, value: String },
    AssignIndexValue { index: String, value: String },
}

// Template With
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateWith {
    pub(crate) expression: TemplateControlExpression,
    pub(crate) parts: Vec<TemplatePart>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

#[cfg(test)]
mod tests {

    struct TemplateFunctionCase<'a> {
        name: &'a str,
        arguments: Vec<TemplateRuntimeValue>,
        expected: &'a str,
    }

    fn template_function_case<'a>(
        name: &'a str,
        arguments: Vec<TemplateRuntimeValue>,
        expected: &'a str,
    ) -> TemplateFunctionCase<'a> {
        TemplateFunctionCase {
            name,
            arguments,
            expected,
        }
    }

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

        let cases = [
            template_function_case("add", vec![s("7"), s("3")], "10"),
            template_function_case("sub", vec![s("7"), s("3")], "4"),
            template_function_case("mul", vec![s("7"), s("3")], "21"),
            template_function_case("div", vec![s("7"), s("3")], "2"),
            template_function_case("mod", vec![s("7"), s("3")], "1"),
            template_function_case("max", vec![s("7"), s("3")], "7"),
            template_function_case("min", vec![s("7"), s("3")], "3"),
            template_function_case("addf", vec![s("7.5"), s("3.25")], "10.75"),
            template_function_case("mulf", vec![s("2.5"), s("4")], "10"),
            template_function_case("divf", vec![s("7.5"), s("2.5")], "3"),
            template_function_case("maxf", vec![s("2.5"), s("7.5")], "7.5"),
            template_function_case("minf", vec![s("2.5"), s("7.5")], "2.5"),
            template_function_case("ceil", vec![s("2.1")], "3"),
            template_function_case("floor", vec![s("2.9")], "2"),
            // `int` parses an integer, it does not truncate a float.
            template_function_case("int", vec![s("42")], "42"),
            template_function_case("len", vec![s("abcd")], "4"),
            // Comparisons: both polarities, so a flipped operator shows.
            template_function_case("eq", vec![s("a"), s("a")], "true"),
            template_function_case("eq", vec![s("a"), s("b")], "false"),
            template_function_case("ne", vec![s("a"), s("b")], "true"),
            template_function_case("ne", vec![s("a"), s("a")], "false"),
            template_function_case("lt", vec![s("3"), s("7")], "true"),
            template_function_case("lt", vec![s("7"), s("3")], "false"),
            template_function_case("gt", vec![s("7"), s("3")], "true"),
            template_function_case("gt", vec![s("3"), s("7")], "false"),
            // Boundaries, where `<` and `<=` part company.
            template_function_case("le", vec![s("3"), s("3")], "true"),
            template_function_case("lt", vec![s("3"), s("3")], "false"),
            template_function_case("ge", vec![s("3"), s("3")], "true"),
            template_function_case("gt", vec![s("3"), s("3")], "false"),
        ];

        for case in cases {
            let got = evaluate_template_function(case.name, &case.arguments).as_rendered_string();
            assert2::check!(got == case.expected, "{}({:?})", case.name, case.arguments);
        }
    }
    use std::{cmp::Ordering, collections::BTreeMap};

    use super::{
        LineFormat, ParsedTemplateToken, TemplatePart, TemplateRuntimeValue,
        ensure_template_parenthesized_token, ensure_template_quoted_token,
        evaluate_template_function, evaluate_template_index, evaluate_template_slice,
        format_go_time_layout, format_template_bytes, format_template_date, format_template_float,
        format_template_float_round, format_template_integer_binary, format_template_ordering,
        format_template_to_date, format_template_to_date_in_zone,
        is_template_control_assignment_variable_char, is_template_variable_name_char_invalid,
        js_escape_template_string, parse_go_time_layout_value, parse_template_bytes,
        parse_template_fractional_nanoseconds, parse_template_parenthesized_token,
        parse_template_parts, parse_template_quoted_token, parse_template_timezone_offset,
        parse_variable_template_digits, push_template_unicode_escape, quoted_template_token_value,
        skip_leading_template_whitespace, substring_template_string, template_compare_values,
        template_index_value, template_slice_bounds, template_value_is_collection,
        tokenize_template_command, trim_template_body_end, urldecode_template_string,
        value_access::TemplateSliceBounds,
    };

    #[test]
    fn template_helpers_trim_body_suffixes_and_literal_boundaries() {
        assert_eq!(trim_template_body_end("prefix body \n\t", 7, 14), 11);
        assert_eq!(trim_template_body_end("prefix \n\t", 7, 9), 7);

        assert_eq!(parse_template_parts("").unwrap(), []);
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

    #[yare::parameterized(
        pipe = { '|', true },
        space = { ' ', true },
        newline = { '\n', true },
        underscore = { '_', false },
    )]
    fn control_assignment_variable_boundary(ch: char, expected: bool) {
        assert_eq!(is_template_control_assignment_variable_char(ch), expected);
    }

    #[yare::parameterized(
        dot = { '.', true },
        space = { ' ', true },
        tab = { '\t', true },
        underscore = { '_', false },
    )]
    fn invalid_variable_name_boundary(ch: char, expected: bool) {
        assert_eq!(is_template_variable_name_char_invalid(ch), expected);
    }

    #[yare::parameterized(
        good = { "`ok`", 4, true },
        no_progress = { "`ok`", 0, false },
        past_end = { "`ok`", 5, false },
        unclosed = { "`ok", 3, false },
    )]
    fn quoted_token_guard(token: &str, end: usize, expected_ok: bool) {
        assert_eq!(
            ensure_template_quoted_token("`ok`", 0, token, end, '`').is_ok(),
            expected_ok
        );
    }

    #[yare::parameterized(
        good = { "(ok)", 4, true },
        no_progress = { "(ok)", 0, false },
        past_end = { "(ok)", 5, false },
        unmatched_start = { "ok)", 3, false },
        unclosed = { "(ok", 3, false },
    )]
    fn parenthesized_token_guard(token: &str, end: usize, expected_ok: bool) {
        assert_eq!(
            ensure_template_parenthesized_token("(ok)", 0, token, end).is_ok(),
            expected_ok
        );
    }

    #[test]
    fn template_parenthesized_tokens_ignore_parentheses_inside_strings() {
        assert_eq!(
            parse_template_parenthesized_token(r#"(printf "a)b") tail"#, 0).unwrap(),
            ParsedTemplateToken {
                token: r#"(printf "a)b")"#.to_string(),
                next_position: 14
            }
        );
        assert_eq!(
            parse_template_parenthesized_token(r"(printf `c)d`) tail", 0).unwrap(),
            ParsedTemplateToken {
                token: "(printf `c)d`)".to_string(),
                next_position: 14
            }
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
            ParsedTemplateToken {
                token: r#""a\"b""#.to_string(),
                next_position: 8
            }
        );
        assert_eq!(
            parse_template_quoted_token("x `a b` tail", 2, '`').unwrap(),
            ParsedTemplateToken {
                token: "`a b`".to_string(),
                next_position: 7
            }
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

    #[yare::parameterized(
        align_left = { "{{ alignLeft 5 }}", "" },
        align_right = { "{{ alignRight 5 }}", "" },
        replace = { "{{ replace \"a\" \"b\" }}", "" },
        default = { "{{ default \"fallback\" }}", "fallback" },
        contains = { "{{ contains \"needle\" }}", "" },
        eq = { "{{ eq \"x\" }}", "" },
        ne = { "{{ ne \"x\" }}", "" },
        has_prefix = { "{{ hasPrefix \"api\" }}", "" },
        has_suffix = { "{{ hasSuffix \"api\" }}", "" },
        indent = { "{{ indent 2 }}", "" },
        nindent = { "{{ nindent 2 }}", "" },
        repeat = { "{{ repeat 3 }}", "" },
        count = { "{{ count \"o\" }}", "" },
        regex_replace = { "{{ regexReplaceAll \"o\" \"foo\" }}", "" },
        regex_replace_literal = { "{{ regexReplaceAllLiteral \"o\" \"foo\" }}", "" },
        trunc = { "{{ trunc 3 }}", "" },
        substr = { "{{ substr 1 3 }}", "" },
        trim_all = { "{{ trimAll \"/\" }}", "" },
        trim_prefix = { "{{ trimPrefix \"/\" }}", "" },
        trim_suffix = { "{{ trimSuffix \"/\" }}", "" },
    )]
    fn template_helper_wrong_arity(template: &str, expected: &str) {
        let format = LineFormat::new(template).unwrap();
        assert2::assert!(format.render("raw", &BTreeMap::new()) == expected);
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
    #[allow(clippy::items_after_statements)]
    fn template_collection_helpers_index_and_slice_strings() {
        let plain = TemplateRuntimeValue::String("abc".to_string());
        let json_string = TemplateRuntimeValue::Json(serde_json::Value::String("xyz".to_string()));
        let scalar = TemplateRuntimeValue::Integer(7);

        struct CollectionExpectation<'a> {
            value: &'a TemplateRuntimeValue,
            expected: bool,
        }

        for case in [
            CollectionExpectation {
                value: &plain,
                expected: true,
            },
            CollectionExpectation {
                value: &json_string,
                expected: true,
            },
            CollectionExpectation {
                value: &scalar,
                expected: false,
            },
        ] {
            assert_eq!(
                template_value_is_collection(case.value),
                case.expected,
                "collection check: {:?}",
                case.value
            );
        }

        assert_eq!(
            template_index_value(&plain, "1"),
            Some(TemplateRuntimeValue::Byte(b'b'))
        );
        assert_eq!(
            template_index_value(&json_string, "1"),
            Some(TemplateRuntimeValue::Byte(b'y'))
        );
        assert_eq!(template_index_value(&scalar, "0"), None);

        assert_eq!(
            evaluate_template_index(&[
                plain.clone(),
                TemplateRuntimeValue::String("2".to_string())
            ]),
            TemplateRuntimeValue::Byte(b'c')
        );
        assert_eq!(
            evaluate_template_index(&[
                json_string.clone(),
                TemplateRuntimeValue::String("0".to_string())
            ]),
            TemplateRuntimeValue::Byte(b'x')
        );
        assert2::assert!(
            evaluate_template_slice(&[
                json_string,
                TemplateRuntimeValue::String("1".to_string()),
                TemplateRuntimeValue::String("3".to_string()),
            ]) == TemplateRuntimeValue::Bytes(b"yz".to_vec())
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

        assert_eq!(
            template_slice_bounds(5, &bounds(&[0, 2, 5])),
            Some(TemplateSliceBounds { start: 0, end: 2 })
        );
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 2, 5, 5])), None);

        assert_eq!(
            template_slice_bounds(5, &bounds(&[0, 3, 3])),
            Some(TemplateSliceBounds { start: 0, end: 3 })
        );
        assert_eq!(
            template_slice_bounds(5, &bounds(&[0, 5, 5])),
            Some(TemplateSliceBounds { start: 0, end: 5 })
        );
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

        assert2::assert!(format_template_float(-0.0) == "-0");
        assert_eq!(format_template_float_round(&args(&["1"])), "");
        assert_eq!(format_template_float_round(&args(&["-1.6", "0"])), "-2");
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "0.5"])),
            "1.2"
        );
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "NaN"])),
            "1.2"
        );
        assert2::assert!(format_template_float_round(&args(&["1.2", "400"])) == "NaN");

        assert2::assert!(format_template_bytes("1.5") == "1");
        assert2::assert!(format_template_bytes("1kB") == "1000");
        assert2::assert!(parse_template_bytes("100000000000000000000").is_none());
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

mod control;
use control::{
    TemplateControlAction, find_template_control_action, parse_template_with,
    split_template_pipeline, template_control_action, tokenize_template_command,
};
mod value_access;
use value_access::{
    template_index_value, template_json_value_to_string, template_slice_bounds,
    template_variable_path_value,
};
mod decode_quoted_fragment;

fn ensure_template_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
    validate: impl FnOnce(&str) -> Result<(), ParseError>,
) -> Result<(), ParseError> {
    if next <= pos {
        return Err(template_parse_error(
            "template token parser did not advance",
        ));
    }
    if next > command.len() {
        return Err(template_parse_error(
            "template token parser advanced past command",
        ));
    }
    validate(token)
}
mod evaluate_common_string_function;
mod evaluate_template_byte_function;
pub use evaluate_template_byte_function::go_string_equal_fold;
mod evaluate_template_function;
mod evaluate_template_value_function;
mod float_format;
mod format_template_bytes;
mod format_template_duration_seconds;
mod format_template_print;
mod format_template_printf;
mod line_format;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedTemplateToken {
    pub(crate) token: String,
    pub(crate) next_position: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedTemplateValue<T> {
    pub(crate) value: T,
    pub(crate) next_position: usize,
}

mod parse_template_integer;
mod parse_template_parts;
mod parse_template_range_expression;
pub(super) mod prometheus_functions;
mod render_template_parts;
mod template_command;
mod template_control_expression;
mod template_expression;
mod template_quote_scanner;
use template_quote_scanner::TemplateQuoteScanner;
mod collection;
mod template_range;
mod template_render_context;
mod template_render_error;
mod template_runtime_value;
mod template_time;
mod template_value;

pub(crate) use collection::{evaluate_template_slice, template_collection_first_args};
use decode_quoted_fragment::{decode_quoted_bytes, decode_quoted_fragment};
use evaluate_common_string_function::evaluate_common_string_function;
use evaluate_template_byte_function::{evaluate_template_byte_function, template_bytes_to_string};
use evaluate_template_function::evaluate_template_function;
use evaluate_template_value_function::evaluate_template_value_function;
pub(crate) use float_format::{format_template_float, format_template_float_fold};
use format_template_bytes::{format_template_bytes, parse_template_bytes};
use format_template_duration_seconds::{format_template_duration_seconds, parse_template_duration};
use format_template_print::{format_template_print, format_template_print_bytes};
pub use format_template_printf::quote_go_bytes;
use format_template_printf::{format_template_printf, format_template_printf_bytes};
pub use line_format::LineFormat;
use parse_template_integer::parse_template_integer;
use parse_template_parts::{parse_template_control_body, parse_template_parts};
use parse_template_range_expression::parse_template_range_expression;
pub use prometheus_functions::{
    histogram::{TemplateFloatHistogram, TemplateHistogramBucket, TemplateHistogramSpan},
    histogram_value::{
        TemplateBucket, TemplateBucketIterator, TemplateBucketStep, TemplateHistogram,
        TemplateHistogramError, TemplateHistogramField, TemplateHistogramReference,
        TemplateHistogramSlice, TemplateHistogramView,
    },
    query_result::TemplateQueryResult,
};
use render_template_parts::render_template_parts;
use template_command::TemplateCommand;
use template_control_expression::TemplateControlExpression;
use template_expression::TemplateExpression;
use template_range::TemplateRange;
use template_render_context::TemplateRenderContext;
pub use template_render_error::TemplateRenderError;
pub use template_runtime_value::TemplateRuntimeValue;
pub use template_time::{TemplateTime, TemplateTimeLocation};
use template_value::TemplateValue;
pub use template_value::number::parse_float as parse_go_float;

pub(crate) use crate::error::syntax_parse_error as template_parse_error;

#[cfg(test)]
pub(crate) fn advance_template_pos(pos: &mut usize, amount: usize) -> Option<()> {
    *pos = pos.checked_add(amount)?;
    Some(())
}

pub(crate) fn align_left_template_string(width: usize, value: &str) -> String {
    let mut chars = value.chars().take(width).collect::<String>();
    let padding = width.saturating_sub(chars.chars().count());
    chars.extend(std::iter::repeat_n(' ', padding));
    chars
}

pub(crate) fn align_right_template_string(width: usize, value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() >= width {
        return chars[chars.len() - width..].iter().collect();
    }
    let mut aligned = " ".repeat(width - chars.len());
    aligned.extend(chars);
    aligned
}

pub(crate) fn current_template_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map_or_else(Default::default, |duration| duration.as_nanos().to_string())
}

pub(crate) fn epoch_template_timestamp(args: &[String], divisor: i64) -> String {
    let Some(timestamp) = args.first() else {
        return String::new();
    };
    let Ok(timestamp_ns) = timestamp.parse::<i64>() else {
        return String::new();
    };
    timestamp_ns.div_euclid(divisor).to_string()
}

pub(crate) fn format_go_time_layout(layout: &str, timestamp: OffsetDateTime) -> String {
    TemplateTime::from_offset(
        timestamp.unix_timestamp(),
        timestamp.nanosecond(),
        timestamp.offset().whole_seconds(),
    )
    .format(layout)
}

pub(crate) fn format_template_float_unary(value: &str, op: impl FnOnce(f64) -> f64) -> String {
    format_template_float(op(value.parse::<f64>().unwrap_or_default()))
}

pub(crate) fn format_template_integer_min_max(
    args: &[String],
    op: impl Fn(i64, i64) -> i64,
) -> String {
    let values = template_integer_args(args);
    values
        .into_iter()
        .reduce(op)
        .map_or_else(String::new, |value| value.to_string())
}

pub(crate) fn format_template_integer_product(args: &[String]) -> String {
    let values = template_integer_args(args);
    values.into_iter().fold(1i64, i64::wrapping_mul).to_string()
}

pub(crate) fn format_template_integer_sum(args: &[String]) -> String {
    let values = template_integer_args(args);
    values.into_iter().fold(0i64, i64::wrapping_add).to_string()
}

pub(crate) fn format_template_to_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], "Local", &args[1])
}

pub(crate) fn format_template_to_date_in_zone(args: &[String]) -> String {
    if args.len() < 3 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], &args[1], &args[2])
}

pub(crate) fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + value - 10),
        _ => '0',
    }
}

pub(crate) fn indent_template_string(spaces: usize, value: &str) -> String {
    let prefix = " ".repeat(spaces);
    value
        .split('\n')
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn is_template_comment_action(expression: &str) -> bool {
    expression.starts_with("/*") && expression.ends_with("*/")
}

pub(crate) fn is_template_control_assignment_variable_char(ch: char) -> bool {
    match ch {
        '|' => true,
        _ => ch.is_whitespace(),
    }
}

pub(crate) fn is_template_variable_name_char_invalid(ch: char) -> bool {
    match ch {
        '.' => true,
        _ => ch.is_whitespace(),
    }
}

pub(crate) fn is_unexpected_template_control_action(expression: &str) -> bool {
    matches!(
        template_control_action(expression),
        TemplateControlAction::Else
            | TemplateControlAction::ElseIf
            | TemplateControlAction::ElseWith
            | TemplateControlAction::End
    )
}

pub(crate) fn is_wrapped_template_token(token: &str, quote: char) -> bool {
    if token.len() < quote.len_utf8().saturating_mul(2) {
        return false;
    }
    token.starts_with(quote) && token.ends_with(quote)
}

#[cfg(test)]
pub(crate) fn match_template_literal(value: &str, pos: &mut usize, expected: char) -> Option<()> {
    let ch = value.get(*pos..)?.chars().next()?;
    if ch != expected {
        return None;
    }
    advance_template_pos(pos, ch.len_utf8())?;
    Some(())
}

pub(crate) fn parse_go_time_layout_to_unix_nanos(layout: &str, zone: &str, value: &str) -> String {
    TemplateTime::parse(layout, zone, value)
        .unwrap_or_else(TemplateTime::zero)
        .unix_nanos()
        .to_string()
}

pub(crate) fn parse_template_bound(value: &TemplateRuntimeValue) -> Option<usize> {
    value.as_rendered_string().parse::<usize>().ok()
}

pub(crate) fn push_template_unicode_escape(output: &mut String, value: u32) {
    let _ = write!(output, "\\u{value:04X}");
}

pub(crate) fn quoted_template_token_value(token: &str) -> Result<Option<String>, ParseError> {
    if is_wrapped_template_token(token, '`') {
        return Ok(Some(token[1..token.len() - 1].to_string()));
    }
    if is_wrapped_template_token(token, '"') {
        return Ok(Some(decode_quoted_fragment(&token[1..token.len() - 1])?));
    }
    Ok(None)
}

pub(crate) fn skip_leading_template_whitespace(template: &str, mut pos: usize) -> usize {
    let Some(rest) = template.get(pos..) else {
        return template.len();
    };
    let trimmed = rest.trim_start_matches(char::is_whitespace);
    pos = template
        .len()
        .checked_sub(trimmed.len())
        .expect("trimmed suffix cannot be longer than template");
    pos
}

pub(crate) fn substring_template_string(value: &str, start: i64, end: i64) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let len = chars.len();
    let start = usize::try_from(start.max(0)).unwrap_or(usize::MAX).min(len);
    let end = usize::try_from(end).ok().map_or(len, |end| end.min(len));
    if end <= start {
        return String::new();
    }
    chars[start..end].iter().collect()
}

pub(crate) fn template_action_trim_left(template: &str, open: usize) -> Result<bool, ParseError> {
    let expression_start = open + 2;
    if !template[expression_start..].starts_with('-') {
        return Ok(false);
    }
    let Some(next) = template[expression_start + 1..].chars().next() else {
        return Err(template_parse_error("expected closing template action"));
    };
    Ok(next.is_whitespace())
}

pub(crate) fn template_compare_values(left: &str, right: &str) -> Option<Ordering> {
    if let Ok(left_number) = left.parse::<f64>()
        && let Ok(right_number) = right.parse::<f64>()
        && left_number.is_finite()
        && right_number.is_finite()
    {
        return left_number.partial_cmp(&right_number);
    }
    Some(left.cmp(right))
}

pub(crate) fn template_float_args(args: &[String]) -> Vec<f64> {
    args.iter()
        .map(|value| value.parse::<f64>().unwrap_or_default())
        .collect()
}

pub(crate) fn template_integer_args(args: &[String]) -> Vec<i64> {
    args.iter()
        .map(|value| parse_template_integer(value))
        .collect()
}

pub(crate) fn template_slice_string(
    value: &str,
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some(bounds) = template_slice_bounds(value.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Bytes(value.as_bytes()[bounds.start..bounds.end].to_vec())
}

pub(crate) fn template_string_truthy(value: &str) -> bool {
    !value.is_empty()
}

pub(crate) fn trim_template_body_end(template: &str, start: usize, end: usize) -> usize {
    template[..end]
        .trim_end_matches(char::is_whitespace)
        .len()
        .max(start)
}

pub(crate) fn truncate_template_string(value: &str, count: i64) -> String {
    if count >= 0 {
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        return value.chars().take(count).collect();
    }
    let count = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
    let len = value.chars().count();
    value.chars().skip(len.saturating_sub(count)).collect()
}

pub(crate) fn urlencode_template_string(value: &str) -> String {
    urlquery_template_string(value)
}

pub(crate) fn ensure_template_parenthesized_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
) -> Result<(), ParseError> {
    ensure_template_token(command, pos, token, next, |token| {
        if !token.starts_with('(') {
            return Err(template_parse_error(
                "template token parser returned token without opening parenthesis",
            ));
        }
        if !token.ends_with(')') {
            return Err(template_parse_error(
                "template token parser returned token without closing parenthesis",
            ));
        }
        Ok(())
    })
}

pub(crate) fn ensure_template_quoted_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
    quote: char,
) -> Result<(), ParseError> {
    ensure_template_token(command, pos, token, next, |token| {
        if is_wrapped_template_token(token, quote) {
            Ok(())
        } else {
            Err(template_parse_error(
                "template token parser returned unwrapped quoted token",
            ))
        }
    })
}

pub(crate) fn evaluate_template_index(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some(split) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    let mut current = split.collection.clone();
    for index in split.args {
        let Some(element) = template_index_value(&current, &index.as_rendered_string()) else {
            return TemplateRuntimeValue::String(String::new());
        };
        current = element;
    }
    current
}

pub(crate) fn format_template_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let Ok(timestamp_ns) = args[1].parse::<i128>() else {
        return String::new();
    };
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns) else {
        return String::new();
    };
    format_go_time_layout(&args[0], timestamp)
}

pub(crate) fn format_template_float_min_max(
    args: &[String],
    op: impl Fn(f64, f64) -> f64,
) -> String {
    let values = template_float_args(args);
    values
        .into_iter()
        .reduce(|left, right| {
            if left.is_nan() || right.is_nan() {
                f64::NAN
            } else {
                op(left, right)
            }
        })
        .map_or_else(String::new, format_template_float)
}

pub(crate) fn format_template_float_round(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let value = args[0].parse::<f64>().unwrap_or_default();
    let precision = args[1].parse::<f64>().unwrap_or_default();
    let round_on = args
        .get(2)
        .map_or(0.5, |value| value.parse::<f64>().unwrap_or_default());
    let factor = 10f64.powf(precision);
    let shifted = value * factor;
    let rounded = if shifted.fract() >= round_on {
        shifted.ceil()
    } else {
        shifted.floor()
    } / factor;
    format_template_float(rounded)
}

pub(crate) fn format_template_integer_binary(
    args: &[String],
    op: impl FnOnce(i64, i64) -> Option<i64>,
) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let left = parse_template_integer(&args[0]);
    let right = parse_template_integer(&args[1]);
    op(left, right).map_or_else(String::new, |value| value.to_string())
}

pub(crate) fn format_template_ordering(
    args: &[String],
    predicate: impl FnOnce(Ordering) -> bool,
) -> String {
    if args.len() < 2 {
        return "false".to_string();
    }
    template_compare_values(&args[0], &args[1])
        .is_some_and(predicate)
        .to_string()
}

pub(crate) fn format_template_printf_string(
    value: &str,
    width: Option<usize>,
    precision: Option<usize>,
    left_align: bool,
) -> String {
    let mut rendered = precision.map_or_else(
        || value.to_string(),
        |precision| value.chars().take(precision).collect(),
    );
    let Some(width) = width else {
        return rendered;
    };

    let len = rendered.chars().count();
    if len >= width {
        return rendered;
    }
    let padding = " ".repeat(width - len);
    if left_align {
        rendered.push_str(&padding);
        rendered
    } else {
        format!("{padding}{rendered}")
    }
}

pub(crate) fn html_escape_template_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

pub(crate) fn js_escape_template_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            '"' => escaped.push_str("\\\""),
            '<' => escaped.push_str("\\u003C"),
            '>' => escaped.push_str("\\u003E"),
            '&' => escaped.push_str("\\u0026"),
            '=' => escaped.push_str("\\u003D"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            ch if ch.is_control() => push_template_unicode_escape(&mut escaped, u32::from(ch)),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
pub(crate) fn parse_fixed_template_digits(
    value: &str,
    pos: &mut usize,
    count: usize,
) -> Option<u32> {
    let start = *pos;
    let digits = value.get(start..)?.get(..count)?;
    digits
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then_some(())?;
    advance_template_pos(pos, count)?;
    digits.parse::<u32>().ok()
}

#[cfg(test)]
pub(crate) fn parse_go_time_layout_value(layout: &str, value: &str) -> Option<ParsedTemplateDate> {
    let parsed = TemplateTime::parse(layout, "UTC", value)?;
    let fields = parsed.parsed_fields();
    let year = fields.year;
    let month = fields.month;
    let day = fields.day;
    let hour = fields.hour;
    let minute = fields.minute;
    let second = fields.second;
    let nanosecond = fields.nanosecond;
    let offset = fields.offset;
    Some(ParsedTemplateDate {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        offset_seconds: Some(offset),
    })
}

/// Parses an `{{else}}` body that runs to its `{{end}}`, returning the body's
/// parts and the position just past `{{end}}`.
pub(crate) fn parse_template_else_body(
    template: &str,
    else_body_start: usize,
) -> Result<ParsedTemplateValue<Vec<TemplatePart>>, ParseError> {
    let Some(control) = find_template_control_action(template, else_body_start)? else {
        return Err(template_parse_error("expected template end action"));
    };
    if control.expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[else_body_start..control.body_end])?;
    Ok(ParsedTemplateValue {
        value: else_parts,
        next_position: control.next_position,
    })
}

#[cfg(test)]
pub(crate) fn parse_template_fractional_nanoseconds(
    value: &str,
    pos: &mut usize,
    max_digits: usize,
) -> Option<u32> {
    match_template_literal(value, pos, '.')?;
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_digits)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    let mut fraction = digits.parse::<u32>().ok()?;
    for _ in digits_len..9 {
        fraction = fraction.checked_mul(10)?;
    }
    Some(fraction)
}

#[cfg(test)]
pub(crate) fn parse_template_timezone_offset(value: &str, pos: &mut usize) -> Option<i32> {
    let rest = value.get(*pos..)?;
    if rest.starts_with('Z') {
        advance_template_pos(pos, 1)?;
        return Some(0);
    }
    let sign: i32 = if rest.starts_with('+') {
        advance_template_pos(pos, 1)?;
        1
    } else if rest.starts_with('-') {
        advance_template_pos(pos, 1)?;
        -1
    } else {
        return None;
    };
    let hours = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    match_template_literal(value, pos, ':')?;
    let minutes = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    let total_minutes = hours.checked_mul(60)?.checked_add(minutes)?;
    sign.checked_mul(total_minutes.checked_mul(60)?)
}

pub(crate) fn parse_template_variable_name(
    variable: &str,
    message: &'static str,
) -> Result<String, ParseError> {
    let Some(variable) = variable.strip_prefix('$') else {
        return Err(template_parse_error(message));
    };
    if variable.is_empty() {
        return Err(template_parse_error(message));
    }
    if variable.contains(is_template_variable_name_char_invalid) {
        return Err(template_parse_error(message));
    }
    Ok(variable.to_string())
}

#[cfg(test)]
pub(crate) fn parse_variable_template_digits(
    value: &str,
    pos: &mut usize,
    max_count: usize,
) -> Option<u32> {
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_count)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    digits.parse::<u32>().ok()
}

pub(crate) fn template_action_trim_right(
    template: &str,
    expression_start: usize,
    close: usize,
) -> bool {
    if close <= expression_start || !template[..close].ends_with('-') {
        return false;
    }
    template[expression_start..close - 1]
        .chars()
        .next_back()
        .is_some_and(char::is_whitespace)
}

pub(crate) fn template_json_value_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value != 0
            } else if let Some(value) = value.as_u64() {
                value != 0
            } else {
                value.as_f64().is_some_and(|value| value != 0.0)
            }
        }
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(value) => !value.is_empty(),
        serde_json::Value::Object(value) => !value.is_empty(),
    }
}

pub(crate) fn template_root_field_value(fields: &Labels, path: &[String]) -> TemplateRuntimeValue {
    let Some(first_and_rest) = path.split_first() else {
        return TemplateRuntimeValue::Labels(fields.clone());
    };
    let first = first_and_rest.0;
    let rest = first_and_rest.1;

    let Some(value) = fields.get(first) else {
        return TemplateRuntimeValue::String(String::new());
    };
    if rest.is_empty() {
        return TemplateRuntimeValue::String(value.clone());
    }

    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|json| template_variable_path_value(&TemplateRuntimeValue::Json(json), rest))
        .unwrap_or_else(|| TemplateRuntimeValue::String(String::new()))
}

pub(crate) fn template_slice_array(
    values: &[serde_json::Value],
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some(bounds) = template_slice_bounds(values.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Json(serde_json::Value::Array(
        values[bounds.start..bounds.end].to_vec(),
    ))
}

pub(crate) fn template_value_is_collection(value: &TemplateRuntimeValue) -> bool {
    matches!(
        value,
        TemplateRuntimeValue::FloatSlice(_)
            | TemplateRuntimeValue::HistogramSpans(_)
            | TemplateRuntimeValue::Labels(_)
            | TemplateRuntimeValue::Bytes(_)
            | TemplateRuntimeValue::ByteSlice(_)
            | TemplateRuntimeValue::ByteLabels(_)
            | TemplateRuntimeValue::Object(_)
            | TemplateRuntimeValue::Array(_)
            | TemplateRuntimeValue::QueryResult(_)
            | TemplateRuntimeValue::String(_)
            | TemplateRuntimeValue::Json(
                serde_json::Value::String(_)
                    | serde_json::Value::Array(_)
                    | serde_json::Value::Object(_)
            )
    )
}

pub(crate) fn title_template_string(value: &str) -> String {
    let mut titled = String::new();
    let mut capitalize_next = true;
    for ch in value.chars() {
        if ch.is_alphanumeric() {
            if capitalize_next {
                for upper in ch.to_uppercase() {
                    titled.push(upper);
                }
            } else {
                for lower in ch.to_lowercase() {
                    titled.push(lower);
                }
            }
            capitalize_next = false;
        } else {
            titled.push(ch);
            capitalize_next = true;
        }
    }
    titled
}

pub(crate) fn unix_to_template_timestamp(epoch: &str) -> String {
    let Ok(value) = epoch.parse::<i128>() else {
        return String::new();
    };
    let nanos = match epoch.len() {
        5 => value.checked_mul(86_400_000_000_000),
        10 => value.checked_mul(1_000_000_000),
        13 => value.checked_mul(1_000_000),
        16 => value.checked_mul(1_000),
        19 => Some(value),
        _ => None,
    };
    nanos.map_or_else(String::new, |value| value.to_string())
}

pub(crate) fn urldecode_template_string(value: &str) -> String {
    let mut bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    while let Some(first) = bytes.split_first() {
        let byte = *first.0;
        let rest = first.1;
        if byte == b'%'
            && let Some(hex_bytes) = rest.get(..2)
            && let Ok(hex) = std::str::from_utf8(hex_bytes)
            && let Ok(decoded_byte) = u8::from_str_radix(hex, 16)
        {
            decoded.push(decoded_byte);
            bytes = &rest[2..];
            continue;
        }
        decoded.push(if byte == b'+' { b' ' } else { byte });
        bytes = rest;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

pub(crate) fn urlquery_template_string(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(hex_digit(byte >> 4));
                encoded.push(hex_digit(byte & 0x0f));
            }
        }
    }
    encoded
}

pub(crate) fn is_template_function_name(name: &str) -> bool {
    prometheus_functions::is_name(name)
        || matches!(
            name,
            "query"
                | "humanize"
                | "humanizeDuration"
                | "humanizePercentage"
                | "humanize1024"
                | "reReplaceAll"
                | "first"
                | "label"
                | "value"
                | "alignLeft"
                | "alignRight"
                | "add"
                | "addf"
                | "and"
                | "b64dec"
                | "b64enc"
                | "lower"
                | "upper"
                | "replace"
                | "default"
                | "contains"
                | "call"
                | "bytes"
                | "date"
                | "eq"
                | "ge"
                | "gt"
                | "hasPrefix"
                | "hasSuffix"
                | "html"
                | "index"
                | "js"
                | "le"
                | "duration"
                | "duration_seconds"
                | "div"
                | "divf"
                | "ceil"
                | "float64"
                | "floor"
                | "fromJson"
                | "indent"
                | "int"
                | "len"
                | "lt"
                | "max"
                | "maxf"
                | "min"
                | "minf"
                | "mod"
                | "mul"
                | "mulf"
                | "ne"
                | "nindent"
                | "now"
                | "not"
                | "or"
                | "print"
                | "printf"
                | "println"
                | "repeat"
                | "count"
                | "regexReplaceAll"
                | "regexReplaceAllLiteral"
                | "substr"
                | "title"
                | "toDate"
                | "toDateInZone"
                | "trim"
                | "trimAll"
                | "trimPrefix"
                | "trimSuffix"
                | "trunc"
                | "sub"
                | "subf"
                | "round"
                | "slice"
                | "unixEpoch"
                | "unixEpochMillis"
                | "unixEpochNanos"
                | "unixToTime"
                | "urlquery"
                | "urlencode"
                | "urldecode"
        )
}
