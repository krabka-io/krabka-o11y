use krabka_logql::{LogqlExpr, parse_logql_expr};

use crate::http::params_format::split_vector_binary_query;

pub(crate) struct SplitLeadingVectorMatchingModifier<'a> {
    pub(crate) text: String,
    pub(crate) remainder: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SplitLeadingVectorGroupModifier<'a> {
    pub(crate) text: Option<String>,
    pub(crate) remainder: &'a str,
}

pub(crate) struct SplitLeadingVectorBinaryModifiers<'a> {
    pub(crate) modifiers: Option<FormattedVectorBinaryModifiers>,
    pub(crate) remainder: &'a str,
}

use crate::{
    Bytes, ComparisonResult, FormattedVectorBinaryModifiers, HttpQueryError, MetricBinarySetOp,
    MetricVectorGroupModifier, MetricVectorMatching, OperandTexts, SeriesParams, decode_query_pair,
    format_label_replace_metric_scalar_expression, format_label_replace_metric_vector_expression,
    format_metric_and_vector_operands, format_metric_label_replace_query, format_metric_query,
    format_metric_scalar_arithmetic_expression, format_metric_scalar_arithmetic_operator,
    format_metric_scalar_comparison_expression, format_metric_scalar_comparison_operator,
    format_metric_vector_comparison_expression, format_metric_vector_set_expression,
    format_scalar_vector_expression, format_simple_metric_query, format_sort_vector_expression,
    format_stream_query, format_vector_function_text, indent_logql_lines,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query, parse_series_params, scalar_vector_expression_result,
    scalar_vector_plain_parse_error, split_query_param_pairs, split_top_level_arithmetic_query,
    split_top_level_comparison_query, split_top_level_set_query,
};

pub(crate) fn validate_metric_binary_query_operands(
    split: crate::http::params_format::metric_formatting::TopLevelQuerySplit<'_>,
) -> Option<()> {
    let right_text = split_leading_vector_binary_modifiers(split.right).remainder;
    parse_metric_query(split.left.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    Some(())
}

pub(crate) fn execute_format_query(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let query = parse_format_query_param(raw_query)?;
    format_logql_query(&query)
}

pub(crate) fn form_body_query(body: &Bytes) -> Result<String, HttpQueryError> {
    String::from_utf8(body.to_vec()).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

pub(crate) fn format_metric_binary_set_operator(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}

pub(crate) fn format_metric_vector_group_modifier(group: &MetricVectorGroupModifier) -> String {
    match group {
        MetricVectorGroupModifier::Left(labels) => {
            format_metric_vector_group_modifier_text("group_left", labels)
        }
        MetricVectorGroupModifier::Right(labels) => {
            format_metric_vector_group_modifier_text("group_right", labels)
        }
    }
}

pub(crate) fn format_metric_vector_group_modifier_text(
    modifier: &str,
    labels: &[String],
) -> String {
    if labels.is_empty() {
        modifier.to_string()
    } else {
        format!("{modifier} ({})", labels.join(","))
    }
}

pub(crate) fn label_join_format_query_error(query: &str) -> Option<String> {
    query
        .trim_start()
        .starts_with("label_join")
        .then(|| "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string())
}

/// Merges a POST query's URL query string with its form body, URL first.
pub(crate) fn post_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    merge_posted_query_params(raw_query, body, PostedQueryParamsOrder::UrlFirst)
}

pub(crate) fn format_binary_operator_line(
    operator: &str,
    comparison_result: ComparisonResult,
    modifiers: Option<FormattedVectorBinaryModifiers>,
) -> String {
    let mut formatted = operator.to_string();
    if comparison_result == ComparisonResult::Bool {
        formatted.push_str(" bool");
    }
    if let Some(modifiers) = modifiers {
        formatted.push(' ');
        formatted.push_str(&modifiers.text);
    }
    formatted
}

pub(crate) fn format_label_replace_metric_binary_expression(
    left_text: &str,
    operator: &str,
    right_text: &str,
) -> Option<String> {
    let left = format_label_replace_metric_binary_operand(left_text)?;
    let right = format_label_replace_metric_binary_operand(right_text)?;
    if !left.is_label_replace && !right.is_label_replace {
        return None;
    }
    Some(format!(
        "{}\n{operator}\n{}",
        indent_logql_lines(&left.text, "  "),
        indent_logql_lines(&right.text, "  "),
    ))
}

pub(crate) fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
    validate_metric_binary_query_operands(split_top_level_arithmetic_query(query)?)?;
    let arithmetic = parse_metric_binary_arithmetic_query(query).ok()?;
    let left = format_metric_query(&arithmetic.left)?;
    let right = format_metric_query(&arithmetic.right)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(format_metric_binary_expression(
        &MetricBinaryExpressionText {
            left: &left,
            operator,
            comparison_result: ComparisonResult::Filter,
            matching: arithmetic.matching.as_ref(),
            right: &right,
        },
    ))
}

pub(crate) fn format_metric_binary_set_query(query: &str) -> Option<String> {
    validate_metric_binary_query_operands(split_top_level_set_query(query)?)?;
    let set = parse_metric_binary_set_query(query).ok()?;
    let left = format_metric_query(&set.left)?;
    let right = format_metric_query(&set.right)?;
    let operator = format_metric_binary_set_operator(set.op);
    Some(format_metric_binary_expression(
        &MetricBinaryExpressionText {
            left: &left,
            operator,
            comparison_result: ComparisonResult::Filter,
            matching: set.matching.as_ref(),
            right: &right,
        },
    ))
}

pub(crate) fn format_metric_vector_binary_expression(
    left: &str,
    operator: &str,
    modifiers: Option<FormattedVectorBinaryModifiers>,
    right: &str,
) -> String {
    match modifiers {
        Some(modifiers) => format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        ),
        None => format!("({left} {operator} {right})"),
    }
}

pub(crate) fn format_metric_vector_matching(
    matching: &MetricVectorMatching,
) -> FormattedMetricVectorMatching {
    match matching {
        MetricVectorMatching::On { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("on", labels, group.as_ref()),
            has_group: group.is_some(),
        },
        MetricVectorMatching::Ignoring { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("ignoring", labels, group.as_ref()),
            has_group: group.is_some(),
        },
    }
}

pub(crate) fn format_metric_vector_matching_text(
    modifier: &str,
    labels: &[String],
    group: Option<&MetricVectorGroupModifier>,
) -> String {
    let mut text = format!("{modifier} ({})", labels.join(","));
    if let Some(group) = group {
        text.push(' ');
        text.push_str(&format_metric_vector_group_modifier(group));
    }
    text
}

pub(crate) fn logql_expression_contains_label_join(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::LabelJoin { .. } => true,
        LogqlExpr::Variants { variants, .. } => {
            variants.iter().any(logql_expression_contains_label_join)
        }
        LogqlExpr::Aggregation {
            expr: expression, ..
        }
        | LogqlExpr::Vector(expression)
        | LogqlExpr::LabelReplace {
            expr: expression, ..
        }
        | LogqlExpr::Sort {
            expr: expression, ..
        }
        | LogqlExpr::Selection {
            expr: expression, ..
        } => logql_expression_contains_label_join(expression),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => {
            logql_expression_contains_label_join(left)
                || logql_expression_contains_label_join(right)
        }
        LogqlExpr::Stream { .. } | LogqlExpr::Metric { .. } | LogqlExpr::Scalar(_) => false,
    }
}

pub(crate) fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::LokiFormatMissingQuery);
    };
    for pair in split_query_param_pairs(raw_query, &["query"]) {
        let pair = decode_query_pair(pair)?;
        if pair.key == "query" {
            return Ok(pair.value);
        }
    }
    Err(HttpQueryError::LokiFormatMissingQuery)
}

pub(crate) fn split_leading_vector_binary_modifiers(
    query: &str,
) -> SplitLeadingVectorBinaryModifiers<'_> {
    let Some(matching_modifier) = split_leading_vector_matching_modifier(query) else {
        return SplitLeadingVectorBinaryModifiers {
            modifiers: None,
            remainder: query.trim_start(),
        };
    };
    let group_modifier = split_leading_vector_group_modifier(matching_modifier.remainder);
    SplitLeadingVectorBinaryModifiers {
        modifiers: Some(match group_modifier.text {
            Some(group_modifier) => FormattedVectorBinaryModifiers {
                text: format!("{} {group_modifier}", matching_modifier.text),
                right_separator: " ",
            },
            None => FormattedVectorBinaryModifiers {
                text: matching_modifier.text,
                right_separator: "  ",
            },
        }),
        remainder: group_modifier.remainder.trim_start(),
    }
}

pub(crate) fn split_leading_vector_matching_modifier(
    query: &str,
) -> Option<SplitLeadingVectorMatchingModifier<'_>> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let labels = rest.trim_start().strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels_text = &labels[..labels_end];
            return Some(SplitLeadingVectorMatchingModifier {
                text: format!("{modifier} ({labels_text})"),
                remainder: &labels[labels_end + 1..],
            });
        }
    }
    None
}

pub(crate) fn format_label_replace_metric_binary_arithmetic(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_arithmetic_query)?;
    let operator =
        format_binary_operator_line(split.operator, ComparisonResult::Filter, split.modifiers);
    format_label_replace_metric_binary_expression(split.left.trim(), &operator, split.right.trim())
}

pub(crate) fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let split = super::split_vector_comparison_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let comparison_result = if split.bool_modifier {
        ComparisonResult::Bool
    } else {
        ComparisonResult::Filter
    };
    let modifiers = split.modifiers;
    let operator = format_binary_operator_line(operator, comparison_result, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, split.right.trim())
}

pub(crate) struct FormattedBinaryOperand {
    pub(crate) text: String,
    pub(crate) is_label_replace: bool,
}

pub(crate) fn format_label_replace_metric_binary_operand(
    query: &str,
) -> Option<FormattedBinaryOperand> {
    if let Some(formatted) = format_metric_label_replace_query(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: false,
        });
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_simple_metric_query(&query))
        .map(|text| FormattedBinaryOperand {
            text,
            is_label_replace: false,
        })
}

pub(crate) fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_set_query)?;
    let operator =
        format_binary_operator_line(split.operator, ComparisonResult::Filter, split.modifiers);
    format_label_replace_metric_binary_expression(split.left.trim(), &operator, split.right.trim())
}

pub(crate) fn format_logql_query(query: &str) -> Result<String, HttpQueryError> {
    if let Some(error) = scalar_vector_plain_parse_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }
    if let Some(error) = label_join_format_query_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }

    match parse_query(query) {
        Ok(query) => Ok(format_stream_query(&query)),
        Err(stream_error) => {
            if let Some(formatted) = format_scalar_vector_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_arithmetic(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_comparison(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_set(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_arithmetic_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_comparison_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_set_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_comparison_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_set_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_label_replace_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_sort_vector_expression(query) {
                Ok(formatted)
            } else if let Ok(metric_query) = parse_metric_query(query) {
                Ok(format_metric_query(&metric_query).unwrap_or_else(|| query.trim().to_string()))
            } else if let Ok(expression) = parse_logql_expr(query) {
                if logql_expression_contains_label_join(&expression) {
                    return Err(HttpQueryError::LokiFormatPlainParse(
                        "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
                            .to_string(),
                    ));
                }
                Ok(expression.to_string())
            } else if parse_metric_label_join_query(query).is_ok()
                || parse_metric_label_replace_query(query).is_ok()
                || parse_metric_binary_arithmetic_query(query).is_ok()
                || parse_metric_binary_comparison_query(query).is_ok()
                || parse_metric_binary_set_query(query).is_ok()
                || parse_metric_scalar_arithmetic_query(query).is_ok()
                || parse_metric_scalar_comparison_query(query).is_ok()
                || scalar_vector_expression_result(query).is_some()
            {
                Ok(query.trim().to_string())
            } else {
                Err(HttpQueryError::LokiFormatParse {
                    query: query.to_string(),
                    source: stream_error,
                })
            }
        }
    }
}

pub(crate) fn format_metric_binary_comparison_query(query: &str) -> Option<String> {
    let split = split_top_level_comparison_query(query)?;
    let left_text = split.left;
    let right_text = split.right;
    let right_text = right_text.trim_start();
    let right_text = right_text
        .strip_prefix("bool")
        .map_or(right_text, str::trim_start);
    let right_text = split_leading_vector_binary_modifiers(right_text).remainder;
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let comparison = parse_metric_binary_comparison_query(query).ok()?;
    let left = format_metric_query(&comparison.left)?;
    let right = format_metric_query(&comparison.right)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    Some(format_metric_binary_expression(
        &MetricBinaryExpressionText {
            left: &left,
            operator,
            comparison_result: ComparisonResult::from_bool_modifier(comparison.bool_modifier),
            matching: comparison.matching.as_ref(),
            right: &right,
        },
    ))
}

/// The formatted parts of one vector-to-vector binary expression.
pub(crate) struct MetricBinaryExpressionText<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    /// [`ComparisonResult::Bool`] writes the `bool` modifier. Arithmetic and
    /// set operators take no modifier, so they use
    /// [`ComparisonResult::Filter`].
    pub(crate) comparison_result: ComparisonResult,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
    pub(crate) right: &'a str,
}

pub(crate) fn format_metric_binary_expression(
    expression: &MetricBinaryExpressionText<'_>,
) -> String {
    let MetricBinaryExpressionText {
        left,
        operator,
        comparison_result,
        matching,
        right,
    } = expression;
    let bool_text = match comparison_result {
        ComparisonResult::Bool => " bool",
        ComparisonResult::Filter => "",
    };
    let Some(matching) = matching else {
        return format!("({left} {operator}{bool_text} {right})");
    };
    let matching = format_metric_vector_matching(matching);
    if matching.has_group {
        return format!(
            "  {left}\n{operator}{bool_text} {}\n  {right}",
            matching.text
        );
    }
    format!("({left} {operator}{bool_text} {}  {right})", matching.text)
}

pub(crate) fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_arithmetic_query)?;
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

pub(crate) struct FormattedMetricVectorMatching {
    pub(crate) text: String,
    pub(crate) has_group: bool,
}

/// Which half of a POST query comes first once its URL query string and form
/// body are merged.
#[derive(Clone, Copy)]
pub(crate) enum PostedQueryParamsOrder {
    /// The URL query string, then the form body.
    UrlFirst,
    /// The form body, then the URL query string.
    BodyFirst,
}

/// Merges a POST query's URL query string with its form body in `order`.
///
/// The first arm's guard is a permanent mutation survivor against `true`:
/// dropping it lets an empty `raw_query` take that arm, and it is only reached
/// when the body is empty too, so the arm returns the same empty string the
/// fall-through would have.
pub(crate) fn merge_posted_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
    order: PostedQueryParamsOrder,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    if let Some(raw_query) = raw_query.filter(|query| !query.is_empty()) {
        if body_query.is_empty() {
            Ok(raw_query.to_owned())
        } else {
            Ok(match order {
                PostedQueryParamsOrder::UrlFirst => format!("{raw_query}&{body_query}"),
                PostedQueryParamsOrder::BodyFirst => format!("{body_query}&{raw_query}"),
            })
        }
    } else {
        Ok(body_query)
    }
}

/// Merges a POST query's URL query string with its form body, body first.
pub(crate) fn post_query_params_body_first(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    merge_posted_query_params(raw_query, body, PostedQueryParamsOrder::BodyFirst)
}

/// Parses the series parameters of a metadata POST from its URL query string
/// and its form body, body first.
pub(crate) fn parse_posted_series_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<SeriesParams, HttpQueryError> {
    parse_series_params(Some(&post_query_params_body_first(raw_query, body)?))
}

pub(crate) fn split_leading_vector_group_modifier(
    query: &str,
) -> SplitLeadingVectorGroupModifier<'_> {
    let query = query.trim_start();
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let rest = rest.trim_start();
            let Some(labels) = rest.strip_prefix('(') else {
                return SplitLeadingVectorGroupModifier {
                    text: Some(modifier.to_string()),
                    remainder: rest,
                };
            };
            let Some(labels_end) = labels.find(')') else {
                return SplitLeadingVectorGroupModifier {
                    text: None,
                    remainder: query,
                };
            };
            let labels_text = &labels[..labels_end];
            let modifier_text = if labels_text.is_empty() {
                modifier.to_string()
            } else {
                format!("{modifier} ({labels_text})")
            };
            return SplitLeadingVectorGroupModifier {
                text: Some(modifier_text),
                remainder: &labels[labels_end + 1..],
            };
        }
    }
    SplitLeadingVectorGroupModifier {
        text: None,
        remainder: query,
    }
}
