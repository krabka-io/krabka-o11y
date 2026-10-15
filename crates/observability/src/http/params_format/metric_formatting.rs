use super::{QuotedTextScanner, split_vector_comparison_query};
use crate::{
    ComparisonOp, ComparisonResult, MetricQuery, MetricScalarArithmeticOp, RangeAggregation,
    VectorComparisonText, format_label_replace_arguments, format_loki_duration_ns,
    format_loki_offset_duration_ns, format_metric_vector_arithmetic_expression,
    format_metric_vector_binary_expression, format_quantile, format_range_aggregation_name,
    format_scalar_vector_expression, format_stream_query, format_vector_aggregation_query,
    format_vector_comparison_text, format_vector_function_text, format_vector_grouping,
    format_vector_label_replace_function, http::params_format::split_vector_binary_query,
    parse_logql_string_argument, parse_metric_label_replace_query, parse_metric_query,
    parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_scalar_sample, split_logql_function_arguments,
};

pub(crate) fn format_metric_scalar_arithmetic_operator(
    op: MetricScalarArithmeticOp,
) -> &'static str {
    krabka_logql::arithmetic_text(op)
}

/// Formats a binary operation between two vectors: arithmetic, comparison,
/// or a set operator.
pub(crate) fn format_metric_vector_operation(query: &str) -> Option<String> {
    format_metric_vector_arithmetic_expression(query)
        .or_else(|| format_metric_vector_comparison_expression(query))
        .or_else(|| format_metric_vector_set_expression(query))
}

pub(crate) fn format_mixed_metric_vector_expression(query: &str) -> Option<String> {
    format_metric_vector_operation(query).or_else(|| format_sort_vector_expression(query))
}

pub(crate) fn format_scalar_text(scalar: &str) -> Option<String> {
    Some(parse_scalar_sample(scalar)?.format())
}

pub(crate) fn format_simple_metric_query(query: &MetricQuery) -> Option<String> {
    if query.vector_aggregation.is_some() || query.range_grouping.is_some() {
        return None;
    }
    format_metric_range_aggregation_query(query)
}

pub(crate) fn has_word_boundary(query: &str, index: usize, len: usize) -> bool {
    query[..index]
        .chars()
        .next_back()
        .is_none_or(char::is_whitespace)
        && query[index + len..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}

pub(crate) fn indent_logql_lines(value: &str, prefix: &str) -> String {
    value
        .lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn split_top_level_comparison_query(query: &str) -> Option<TopLevelQuerySplit<'_>> {
    split_top_level_query(query, |index, ch| {
        if !matches!(ch, '>' | '<' | '=' | '!') {
            return None;
        }
        [">=", "<=", "==", "!=", ">", "<"]
            .into_iter()
            .find(|operator| query[index..].starts_with(operator))
    })
}

pub(crate) fn split_top_level_set_query(query: &str) -> Option<TopLevelQuerySplit<'_>> {
    split_top_level_query(query, |index, ch| {
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        ["unless", "and", "or"].into_iter().find(|operator| {
            query[index..].starts_with(operator) && has_word_boundary(query, index, operator.len())
        })
    })
}

pub(crate) fn format_label_replace_metric_scalar_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_metric_scalar_vector_expression(arguments[0].trim())?;
    format_label_replace_arguments(&vector, &arguments[1..])
}

pub(crate) fn format_label_replace_metric_vector_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_mixed_metric_vector_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace(\n  {vector},\n  {},\n  {},\n  {},\n  {}\n)",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

pub(crate) fn format_logql_quoted_string(value: &str) -> String {
    let mut formatted = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => formatted.push_str("\\\\"),
            '"' => formatted.push_str("\\\""),
            '\n' => formatted.push_str("\\n"),
            '\r' => formatted.push_str("\\r"),
            '\t' => formatted.push_str("\\t"),
            other => formatted.push(other),
        }
    }
    formatted.push('"');
    formatted
}

pub(crate) fn format_loki_vector_expression(query: &str) -> Option<String> {
    let formatters: [fn(&str) -> Option<String>; 7] = [
        format_metric_vector_operation,
        format_metric_scalar_arithmetic_expression,
        format_metric_scalar_comparison_expression,
        format_vector_label_replace_function,
        format_label_replace_metric_scalar_expression,
        format_label_replace_metric_vector_expression,
        format_scalar_vector_expression,
    ];
    formatters
        .into_iter()
        .find_map(|format| format(query))
        .or_else(|| {
            parse_metric_query(query)
                .ok()
                .and_then(|query| format_metric_query(&query))
        })
}

pub(crate) fn format_metric_label_replace_query(query: &str) -> Option<String> {
    let label_replace = parse_metric_label_replace_query(query).ok()?;
    let metric = format_metric_query(&label_replace.query)?;
    Some(format!(
        "label_replace({metric},{},{},{},{})",
        format_logql_quoted_string(&label_replace.destination_label),
        format_logql_quoted_string(&label_replace.replacement),
        format_logql_quoted_string(&label_replace.source_label),
        format_logql_quoted_string(&label_replace.pattern),
    ))
}

pub(crate) fn format_metric_query(query: &MetricQuery) -> Option<String> {
    let mut formatted = format_metric_range_aggregation_query(query)?;
    if let Some(grouping) = &query.range_grouping {
        formatted = format!("{formatted} {}", format_vector_grouping(grouping));
    }
    if let Some(vector_aggregation) = &query.vector_aggregation {
        formatted = format_vector_aggregation_query(vector_aggregation, &formatted)?;
    }
    Some(formatted)
}

pub(crate) fn format_metric_range_aggregation_query(query: &MetricQuery) -> Option<String> {
    let range = format_metric_range_selector(query)?;
    if let RangeAggregation::QuantileOverTime(quantile) = query.aggregation {
        return Some(format!(
            "quantile_over_time({},{range})",
            format_quantile(quantile),
        ));
    }
    Some(format!(
        "{}({range})",
        format_range_aggregation_name(&query.aggregation)?,
    ))
}

pub(crate) fn format_metric_range_selector(query: &MetricQuery) -> Option<String> {
    let range = format_loki_duration_ns(query.range_ns.0)?;
    let offset = if query.offset_ns.0 == 0 {
        String::new()
    } else {
        let sign = if query.offset_ns.0 < 0 { "-" } else { "" };
        let duration = format_loki_offset_duration_ns(query.offset_ns.0.checked_abs()?)?;
        format!(" offset {sign}{duration}")
    };
    Some(format!(
        "{}[{range}]{offset}",
        format_stream_query(&query.stream)
    ))
}

pub(crate) fn format_metric_scalar_arithmetic_expression(query: &str) -> Option<String> {
    let arithmetic = parse_metric_scalar_arithmetic_query(query).ok()?;
    let metric = format_simple_metric_query(&arithmetic.query)?;
    let scalar = format_scalar_text(&arithmetic.scalar)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(if arithmetic.scalar_on_left {
        format!("({scalar} {operator} {metric})")
    } else {
        format!("({metric} {operator} {scalar})")
    })
}

pub(crate) fn format_metric_scalar_comparison_expression(query: &str) -> Option<String> {
    let comparison = parse_metric_scalar_comparison_query(query).ok()?;
    let metric = format_simple_metric_query(&comparison.query)?;
    let scalar = format_scalar_text(&comparison.scalar)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    let bool_modifier = if comparison.bool_modifier {
        " bool"
    } else {
        ""
    };
    Some(if comparison.scalar_on_left {
        format!("({scalar} {operator}{bool_modifier} {metric})")
    } else {
        format!("({metric} {operator}{bool_modifier} {scalar})")
    })
}

pub(crate) fn format_metric_scalar_comparison_operator(op: ComparisonOp) -> Option<&'static str> {
    match op {
        ComparisonOp::Equal => Some("=="),
        ComparisonOp::NotEqual => Some("!="),
        ComparisonOp::Greater => Some(">"),
        ComparisonOp::GreaterEqual => Some(">="),
        ComparisonOp::Less => Some("<"),
        ComparisonOp::LessEqual => Some("<="),
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => None,
    }
}

pub(crate) fn format_metric_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    None
}

pub(crate) fn format_sort_vector_expression(query: &str) -> Option<String> {
    for function in ["sort", "sort_desc"] {
        let Some(arguments) = split_logql_function_arguments(query, function) else {
            continue;
        };
        if arguments.len() != 1 {
            return None;
        }
        let inner = format_loki_vector_expression(arguments[0].trim())?;
        if inner.contains('\n') {
            return Some(format!(
                "{function}(\n{}\n)",
                indent_logql_lines(&inner, "  ")
            ));
        }
        return Some(format!("{function}({inner})"));
    }
    None
}

pub(crate) fn split_top_level_arithmetic_query(query: &str) -> Option<TopLevelQuerySplit<'_>> {
    split_top_level_query(query, |_, ch| match ch {
        '+' => Some("+"),
        '-' => Some("-"),
        '*' => Some("*"),
        '/' => Some("/"),
        '%' => Some("%"),
        '^' => Some("^"),
        _ => None,
    })
}

/// The text on either side of a binary operator.
#[derive(Clone, Copy)]
pub(crate) struct OperandTexts<'a> {
    pub(crate) left: &'a str,
    pub(crate) right: &'a str,
}

pub(crate) struct FormattedOperands {
    pub(crate) left: String,
    pub(crate) right: String,
}

/// Formats the operands of a binary expression between a simple metric query
/// and a vector function, which may stand on either side.
pub(crate) fn format_metric_and_vector_operands(
    operands: OperandTexts<'_>,
) -> Option<FormattedOperands> {
    let OperandTexts {
        left: left_text,
        right: right_text,
    } = operands;
    let format_metric = |text: &str| {
        parse_metric_query(text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query))
    };
    if let Some(left) = format_metric(left_text)
        && let Some(right) = format_vector_function_text(right_text.trim())
    {
        return Some(FormattedOperands { left, right });
    }
    if let Some(left) = format_vector_function_text(left_text.trim())
        && let Some(right) = format_metric(right_text)
    {
        return Some(FormattedOperands { left, right });
    }
    None
}

pub(crate) fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let split = split_vector_comparison_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let right_text = split.right;
    let bool_modifier = split.bool_modifier;
    let modifiers = split.modifiers;
    let operands = format_metric_and_vector_operands(OperandTexts {
        left: left_text,
        right: right_text,
    })?;

    Some(format_vector_comparison_text(VectorComparisonText {
        left: &operands.left,
        operator,
        comparison_result: ComparisonResult::from_bool_modifier(bool_modifier),
        modifiers,
        right: &operands.right,
    }))
}

pub(crate) fn format_metric_vector_set_expression(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_set_query)?;
    let operands = format_metric_and_vector_operands(OperandTexts {
        left: split.left,
        right: split.right,
    })?;

    Some(format_metric_vector_binary_expression(
        &operands.left,
        split.operator,
        split.modifiers,
        &operands.right,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopLevelQuerySplit<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'static str,
    pub(crate) right: &'a str,
}

/// Splits `query` at the first operator that sits outside every quote,
/// parenthesis, bracket and brace, and returns the left side, the operator,
/// and the right side with its leading whitespace trimmed.
///
/// `operator_at` names the operator that starts at a top-level byte index,
/// if one does; it never sees a quote or a bracket character.
pub(crate) fn split_top_level_query(
    query: &str,
    operator_at: impl Fn(usize, char) -> Option<&'static str>,
) -> Option<TopLevelQuerySplit<'_>> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quotes = QuotedTextScanner::default();
    for character in query.char_indices() {
        let index = character.0;
        let ch = character.1;
        if quotes.in_quotes(ch) {
            continue;
        }
        match ch {
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            _ if parens == 0 && brackets == 0 && braces == 0 => {
                if let Some(operator) = operator_at(index, ch) {
                    return Some(TopLevelQuerySplit {
                        left: &query[..index],
                        operator,
                        right: query[index + operator.len()..].trim_start(),
                    });
                }
            }
            _ => {}
        }
    }
    None
}
