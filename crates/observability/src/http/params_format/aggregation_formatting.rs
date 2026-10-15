use std::fmt::Write as _;
use crate::http::params_format::ParsedValueAt;
use crate::http::params_format::QuotedTextScanner;

mod apply_approx_metric_selection;
pub(crate) use apply_approx_metric_selection::apply_approx_metric_selection;

use crate::{
    ComparisonResult, Quantile, RangeAggregation, ScalarSample, ScalarVectorExpressionResult,
    VectorAggregation, VectorAggregationOp, VectorGrouping, format_logql_quoted_string,
    parse_formatted_vector_function, parse_scalar_sample, parse_vector_arithmetic_operator,
    scalar_vector_expression_result,
};

pub(crate) struct ParsedVectorBinaryRight {
    pub(crate) modifiers: Option<FormattedVectorBinaryModifiers>,
    pub(crate) right: String,
}

fn format_vector_binary_expression(query: &str) -> Option<String> {
    format_vector_set_expression(query)
        .or_else(|| format_vector_arithmetic_expression(query))
        .or_else(|| format_vector_comparison_expression(query))
}

fn format_vector_binary_operands(
    query: &str,
    left: &str,
    operator: &str,
    right_position: usize,
) -> Option<String> {
    let parsed_right = parse_vector_binary_right(query, right_position)?;
    Some(match parsed_right.modifiers {
        Some(modifiers) => format!(
            "({left} {operator} {}{}{right})",
            modifiers.text,
            modifiers.right_separator,
            right = parsed_right.right
        ),
        None => format!("({left} {operator} {})", parsed_right.right),
    })
}

fn parse_vector_binary_right(
    query: &str,
    mut right_position: usize,
) -> Option<ParsedVectorBinaryRight> {
    let modifiers =
        if let Some(parsed_modifiers) = parse_vector_binary_modifiers(query, right_position) {
            right_position = parsed_modifiers.position;
            Some(parsed_modifiers.value)
        } else {
            None
        };
    let right = parse_formatted_vector_function(query, right_position)?;
    (right.position == query.len()).then_some(ParsedVectorBinaryRight {
        modifiers,
        right: right.value,
    })
}

pub(crate) fn format_loki_offset_seconds(duration_ns: i64) -> String {
    format_loki_decimal_unit(duration_ns, 1_000_000_000, 9, "s")
}

pub(crate) fn format_quantile(quantile: Quantile) -> String {
    ScalarSample::new(
        i128::from(quantile.numerator.0),
        u128::from(quantile.denominator.0),
    )
    .format()
}

pub(crate) fn format_vector_arithmetic_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    let operator = parse_vector_arithmetic_operator(query, left.position)?;
    format_vector_binary_operands(query, &left.value, operator.value, operator.position)
}

pub(crate) fn format_vector_function_text(query: &str) -> Option<String> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let formatted = parse_formatted_vector_function(&query, 0)?;
    (formatted.position == query.len()).then_some(formatted.value)
}

pub(crate) fn format_vector_grouping(grouping: &VectorGrouping) -> String {
    match grouping {
        VectorGrouping::By(labels) => format!("by ({})", labels.join(",")),
        VectorGrouping::Without(labels) => format!("without ({})", labels.join(",")),
    }
}

pub(crate) fn format_vector_label_replace_function(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_vector_only_expression(arguments[0].trim())?;
    format_label_replace_arguments(&vector, &arguments[1..])
}

/// Prints `label_replace` with an already formatted vector operand and the
/// four string arguments that follow it, or `None` when one of them is not a
/// `LogQL` string.
pub(crate) fn format_label_replace_arguments(
    vector: &str,
    string_arguments: &[&str],
) -> Option<String> {
    let mut formatted = format!("label_replace({vector}");
    for argument in string_arguments {
        let argument = parse_logql_string_argument(argument.trim())?;
        formatted.push(',');
        formatted.push_str(&format_logql_quoted_string(&argument));
    }
    formatted.push(')');
    Some(formatted)
}

pub(crate) fn format_loki_decimal_unit(
    duration_ns: i64,
    unit_ns: i64,
    width: usize,
    suffix: &str,
) -> String {
    let whole = duration_ns / unit_ns;
    let fractional_ns = duration_ns % unit_ns;
    if fractional_ns == 0 {
        return format!("{whole}{suffix}");
    }

    let mut fraction = format!("{fractional_ns:0width$}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    format!("{whole}.{fraction}{suffix}")
}

pub(crate) fn format_range_aggregation_name(
    aggregation: &RangeAggregation,
) -> Option<&'static str> {
    match aggregation {
        RangeAggregation::CountOverTime => Some("count_over_time"),
        RangeAggregation::Rate => Some("rate"),
        RangeAggregation::RateCounter => Some("rate_counter"),
        RangeAggregation::BytesRate => Some("bytes_rate"),
        RangeAggregation::BytesOverTime => Some("bytes_over_time"),
        RangeAggregation::AbsentOverTime => Some("absent_over_time"),
        RangeAggregation::PresentOverTime => Some("present_over_time"),
        RangeAggregation::SumOverTime => Some("sum_over_time"),
        RangeAggregation::AvgOverTime => Some("avg_over_time"),
        RangeAggregation::StdvarOverTime => Some("stdvar_over_time"),
        RangeAggregation::StddevOverTime => Some("stddev_over_time"),
        RangeAggregation::MinOverTime => Some("min_over_time"),
        RangeAggregation::MaxOverTime => Some("max_over_time"),
        RangeAggregation::FirstOverTime => Some("first_over_time"),
        RangeAggregation::LastOverTime => Some("last_over_time"),
        RangeAggregation::QuantileOverTime(_) => None,
    }
}

pub(crate) fn format_vector_comparison_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    let operator = parse_vector_comparison_operator(query, left.position)?;
    let mut right_position = operator.position;
    let bool_modifier = query[right_position..].starts_with("bool");
    if bool_modifier {
        right_position += "bool".len();
    }
    let right = parse_vector_binary_right(query, right_position)?;
    Some(format_vector_comparison_text(VectorComparisonText {
        left: &left.value,
        operator: operator.value,
        comparison_result: ComparisonResult::from_bool_modifier(bool_modifier),
        modifiers: right.modifiers,
        right: &right.right,
    }))
}

pub(crate) fn format_vector_only_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    format_vector_binary_expression(&query)
}

pub(crate) fn format_vector_set_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    for operator in ["unless", "and", "or"] {
        if let Some(rest) = query[left.position..].strip_prefix(operator)
            && let Some(formatted) = format_vector_binary_operands(
                query,
                &left.value,
                operator,
                query.len() - rest.len(),
            )
        {
            return Some(formatted);
        }
    }
    None
}

pub(crate) fn parse_logql_string_argument(argument: &str) -> Option<String> {
    if let Some(inner) = argument
        .strip_prefix('`')
        .and_then(|argument| argument.strip_suffix('`'))
    {
        return Some(inner.to_string());
    }

    let inner = argument
        .strip_prefix('"')
        .and_then(|argument| argument.strip_suffix('"'))?;
    let mut parsed = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            parsed.push(match chars.next()? {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                other => other,
            });
        } else {
            parsed.push(ch);
        }
    }
    Some(parsed)
}

struct DurationUnit {
    nanoseconds: i64,
    suffix: &'static str,
}

pub(crate) fn format_loki_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let mut formatted = String::new();
    for unit in [
        DurationUnit {
            nanoseconds: 3_600_000_000_000_i64,
            suffix: "h",
        },
        DurationUnit {
            nanoseconds: 60_000_000_000_i64,
            suffix: "m",
        },
        DurationUnit {
            nanoseconds: 1_000_000_000_i64,
            suffix: "s",
        },
        DurationUnit {
            nanoseconds: 1_000_000_i64,
            suffix: "ms",
        },
        DurationUnit {
            nanoseconds: 1_000_i64,
            suffix: "us",
        },
        DurationUnit {
            nanoseconds: 1_i64,
            suffix: "ns",
        },
    ] {
        if remaining >= unit.nanoseconds {
            let value = remaining / unit.nanoseconds;
            remaining %= unit.nanoseconds;
            write!(formatted, "{value}{}", unit.suffix).expect("writing to a String cannot fail");
        }
    }
    Some(formatted)
}

pub(crate) fn format_loki_offset_duration_ns(duration_ns: i64) -> Option<String> {
    const HOUR_NS: i64 = 3_600_000_000_000;
    const MINUTE_NS: i64 = 60_000_000_000;
    const SECOND_NS: i64 = 1_000_000_000;
    const MILLISECOND_NS: i64 = 1_000_000;
    const MICROSECOND_NS: i64 = 1_000;

    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let hours = remaining / HOUR_NS;
    remaining %= HOUR_NS;
    let minutes = remaining / MINUTE_NS;
    remaining %= MINUTE_NS;

    if hours > 0 {
        return Some(format!(
            "{hours}h{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if minutes > 0 {
        return Some(format!(
            "{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if remaining >= SECOND_NS {
        return Some(format_loki_offset_seconds(remaining));
    }
    if remaining >= MILLISECOND_NS {
        return Some(format_loki_decimal_unit(remaining, MILLISECOND_NS, 6, "ms"));
    }
    if remaining >= MICROSECOND_NS {
        return Some(format_loki_decimal_unit(
            remaining,
            MICROSECOND_NS,
            3,
            "\u{00b5}s",
        ));
    }
    Some(format!("{remaining}ns"))
}

pub(crate) fn format_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(scalar) = query
        .strip_prefix("vector(")
        .and_then(|query| query.strip_suffix(')'))
    {
        if scalar.starts_with(['+', '-']) {
            return None;
        }
        if let Some(sample) = parse_scalar_sample(scalar) {
            return Some(format!("vector({})", sample.format_fixed_six()));
        }
    }
    if let Some(formatted) = format_vector_binary_expression(&query) {
        return Some(formatted);
    }
    match scalar_vector_expression_result(&query)? {
        ScalarVectorExpressionResult::Scalar { sample } => Some(sample),
        ScalarVectorExpressionResult::Vector { .. } => None,
    }
}

pub(crate) fn format_vector_aggregation_query(
    aggregation: &VectorAggregation,
    inner: &str,
) -> Option<String> {
    let grouping = aggregation
        .grouping
        .as_ref()
        .map_or_else(Default::default, |grouping| {
            format!(" {}", format_vector_grouping(grouping))
        });
    match &aggregation.op {
        VectorAggregationOp::Sum => Some(format!("sum{grouping}({inner})")),
        VectorAggregationOp::Count => Some(format!("count{grouping}({inner})")),
        VectorAggregationOp::Min => Some(format!("min{grouping}({inner})")),
        VectorAggregationOp::Max => Some(format!("max{grouping}({inner})")),
        VectorAggregationOp::Avg => Some(format!("avg{grouping}({inner})")),
        VectorAggregationOp::Stddev => Some(format!("stddev{grouping}({inner})")),
        VectorAggregationOp::Stdvar => Some(format!("stdvar{grouping}({inner})")),
        VectorAggregationOp::TopK(limit) => Some(format!("topk{grouping}({limit},{inner})")),
        VectorAggregationOp::BottomK(limit) => Some(format!("bottomk{grouping}({limit},{inner})")),
        VectorAggregationOp::ApproxTopK(limit) if aggregation.grouping.is_none() => {
            Some(format!("approx_topk({limit},{inner})"))
        }
        VectorAggregationOp::Sort if aggregation.grouping.is_none() => {
            Some(format!("sort({inner})"))
        }
        VectorAggregationOp::SortDesc if aggregation.grouping.is_none() => {
            Some(format!("sort_desc({inner})"))
        }
        VectorAggregationOp::CountValues(_)
        | VectorAggregationOp::ApproxTopK(_)
        | VectorAggregationOp::Sort
        | VectorAggregationOp::SortDesc => None,
    }
}

/// The parsed parts of a vector comparison, ready to print.
pub(crate) struct VectorComparisonText<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    pub(crate) comparison_result: ComparisonResult,
    pub(crate) modifiers: Option<FormattedVectorBinaryModifiers>,
    pub(crate) right: &'a str,
}

/// Prints a vector comparison as Loki's `format_query` writes it: wrapped in
/// parentheses, with `bool` and any matching modifiers between the operator
/// and the right operand.
pub(crate) fn format_vector_comparison_text(comparison: VectorComparisonText<'_>) -> String {
    let VectorComparisonText {
        left,
        operator,
        comparison_result,
        modifiers,
        right,
    } = comparison;
    let bool_text = match comparison_result {
        ComparisonResult::Bool => "bool ",
        ComparisonResult::Filter => "",
    };
    let modifier_text = modifiers.map_or_else(String::new, |modifiers| {
        format!("{}{}", modifiers.text, modifiers.right_separator)
    });
    format!("({left} {operator} {bool_text}{modifier_text}{right})")
}

pub(crate) struct FormattedVectorBinaryModifiers {
    pub(crate) text: String,
    pub(crate) right_separator: &'static str,
}

pub(crate) fn parse_vector_binary_modifiers(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<FormattedVectorBinaryModifiers>> {
    let matching_modifier = parse_vector_matching_modifier(query, position)?;
    if let Some(group_modifier) = parse_vector_group_modifier(query, matching_modifier.position) {
        return Some(ParsedValueAt {
            value: FormattedVectorBinaryModifiers {
                text: format!("{} {}", matching_modifier.value, group_modifier.value),
                right_separator: " ",
            },
            position: group_modifier.position,
        });
    }
    Some(ParsedValueAt {
        value: FormattedVectorBinaryModifiers {
            text: matching_modifier.value,
            right_separator: "  ",
        },
        position: matching_modifier.position,
    })
}

pub(crate) fn parse_vector_comparison_operator(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<&'static str>> {
    for operator in [">=", "<=", "==", "!=", ">", "<"] {
        if query[position..].starts_with(operator) {
            return Some(ParsedValueAt {
                value: operator,
                position: position + operator.len(),
            });
        }
    }
    None
}

pub(crate) fn parse_vector_group_modifier(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<String>> {
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let Some(labels) = rest.strip_prefix('(') else {
                return Some(ParsedValueAt {
                    value: modifier.to_string(),
                    position: position + modifier.len(),
                });
            };
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            if labels.is_empty() {
                return Some(ParsedValueAt {
                    value: modifier.to_string(),
                    position: position + modifier.len() + 2,
                });
            }
            return Some(ParsedValueAt {
                value: format!("{modifier} ({labels})"),
                position: position + modifier.len() + 1 + labels_end + 1,
            });
        }
    }
    None
}

pub(crate) fn parse_vector_matching_modifier(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<String>> {
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let labels = rest.strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            return Some(ParsedValueAt {
                value: format!("{modifier} ({labels})"),
                position: position + modifier.len() + 1 + labels_end + 1,
            });
        }
    }
    None
}

pub(crate) fn split_logql_function_arguments<'a>(
    query: &'a str,
    name: &str,
) -> Option<Vec<&'a str>> {
    let query = query.trim();
    let rest = query.strip_prefix(name)?.trim_start();
    let rest = rest.strip_prefix('(')?;
    let mut arguments = Vec::new();
    let mut start = 0;
    let mut parens = 0_i32;
    let mut quotes = QuotedTextScanner::default();
    for character in rest.char_indices() {
        let index = character.0;
        let ch = character.1;
        if quotes.in_quotes(ch) {
            continue;
        }
        match ch {
            '(' => parens += 1,
            ')' if parens > 0 => parens -= 1,
            ',' if parens == 0 => {
                arguments.push(rest[start..index].trim());
                start = index + ch.len_utf8();
            }
            ')' => {
                arguments.push(rest[start..index].trim());
                if rest[index + ch.len_utf8()..].trim().is_empty() {
                    return Some(arguments);
                }
                return None;
            }
            _ => {}
        }
    }
    None
}
