use crate::{
    Bytes, FormattedVectorBinaryModifiers, HttpQueryError, MetricBinarySetOp,
    MetricVectorGroupModifier, MetricVectorMatching, decode_form_component,
    format_label_replace_metric_scalar_expression, format_label_replace_metric_vector_expression,
    format_metric_label_replace_query, format_metric_query,
    format_metric_scalar_arithmetic_expression, format_metric_scalar_arithmetic_operator,
    format_metric_scalar_comparison_expression, format_metric_scalar_comparison_operator,
    format_metric_vector_comparison_expression, format_metric_vector_set_expression,
    format_scalar_vector_expression, format_simple_metric_query, format_sort_vector_expression,
    format_stream_query, format_vector_function_text, indent_logql_lines,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query, scalar_vector_expression_result, scalar_vector_plain_parse_error,
    split_query_param_pairs, split_top_level_arithmetic_query, split_top_level_comparison_query,
    split_top_level_set_query,
};
pub(crate) fn form_body_query(body: &Bytes) -> Result<String, HttpQueryError> {
    String::from_utf8(body.to_vec()).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

/// Merges a POST query's URL query string with its form body, URL first.
///
/// The first arm's guard is a permanent mutation survivor against `true`:
/// dropping it lets an empty `raw_query` take that arm, and it is only reached
/// when the body is empty too, so the arm returns the same empty string the
/// fall-through would have.
pub(crate) fn post_query_params(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{raw_query}&{body_query}"))
        }
        _ => Ok(body_query),
    }
}

/// Merges a POST query's URL query string with its form body, body first.
///
/// Its first arm's guard is a permanent survivor for the same reason as
/// [`post_query_params`].
pub(crate) fn post_query_params_body_first(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{body_query}&{raw_query}"))
        }
        _ => Ok(body_query),
    }
}

pub(crate) fn execute_format_query(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let query = parse_format_query_param(raw_query)?;
    format_logql_query(&query)
}

pub(crate) fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::LokiFormatMissingQuery);
    };
    for pair in split_query_param_pairs(raw_query, &["query"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "query" {
            return decode_form_component(value);
        }
    }
    Err(HttpQueryError::LokiFormatMissingQuery)
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
            // The label_replace and binary arms below are shadowed: for every
            // query that could be constructed, the dedicated `format_*` branch
            // above accepts exactly what the corresponding `parse_*` here
            // does, and returns a reprint rather than falling through. Those
            // arms therefore stay as permanent mutation survivors. They are
            // kept because the formatters can decline on their own -- each
            // gives up if a sub-expression will not format -- and this is the
            // arm that catches a query when they do.
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

pub(crate) fn label_join_format_query_error(query: &str) -> Option<String> {
    query
        .trim_start()
        .starts_with("label_join")
        .then(|| "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string())
}

pub(crate) fn format_label_replace_metric_binary_arithmetic(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

pub(crate) fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

pub(crate) fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, bool_modifier, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

pub(crate) fn format_binary_operator_line(
    operator: &str,
    bool_modifier: bool,
    modifiers: Option<FormattedVectorBinaryModifiers>,
) -> String {
    let mut formatted = operator.to_string();
    if bool_modifier {
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
    let (left, left_is_label_replace) = format_label_replace_metric_binary_operand(left_text)?;
    let (right, right_is_label_replace) = format_label_replace_metric_binary_operand(right_text)?;
    if !left_is_label_replace && !right_is_label_replace {
        return None;
    }
    Some(format!(
        "{}\n{operator}\n{}",
        indent_logql_lines(&left, "  "),
        indent_logql_lines(&right, "  "),
    ))
}

pub(crate) fn format_label_replace_metric_binary_operand(query: &str) -> Option<(String, bool)> {
    if let Some(formatted) = format_metric_label_replace_query(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_vector_function_text(query) {
        return Some((formatted, false));
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_simple_metric_query(&query))
        .map(|formatted| (formatted, false))
}

pub(crate) fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = if let (Some(left), Some(right)) = (
        parse_metric_query(left_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
        format_vector_function_text(right_text.trim()),
    ) {
        (left, right)
    } else if let (Some(left), Some(right)) = (
        format_vector_function_text(left_text.trim()),
        parse_metric_query(right_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
    ) {
        (left, right)
    } else {
        return None;
    };

    Some(format_metric_vector_binary_expression(
        &left, operator, modifiers, &right,
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

pub(crate) fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_arithmetic_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let arithmetic = parse_metric_binary_arithmetic_query(query).ok()?;
    let left = format_metric_query(&arithmetic.left)?;
    let right = format_metric_query(&arithmetic.right)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        arithmetic.matching.as_ref(),
        &right,
    ))
}

pub(crate) fn format_metric_binary_comparison_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let right_text = right_text
        .strip_prefix("bool")
        .map_or(right_text, str::trim_start);
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let comparison = parse_metric_binary_comparison_query(query).ok()?;
    let left = format_metric_query(&comparison.left)?;
    let right = format_metric_query(&comparison.right)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    Some(format_metric_binary_expression(
        &left,
        operator,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
        &right,
    ))
}

pub(crate) fn format_metric_binary_set_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_set_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let set = parse_metric_binary_set_query(query).ok()?;
    let left = format_metric_query(&set.left)?;
    let right = format_metric_query(&set.right)?;
    let operator = format_metric_binary_set_operator(set.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        set.matching.as_ref(),
        &right,
    ))
}

pub(crate) fn format_metric_binary_expression(
    left: &str,
    operator: &str,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    right: &str,
) -> String {
    let bool_text = if bool_modifier { " bool" } else { "" };
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

pub(crate) struct FormattedMetricVectorMatching {
    pub(crate) text: String,
    pub(crate) has_group: bool,
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

pub(crate) fn format_metric_binary_set_operator(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}

pub(crate) fn split_leading_vector_binary_modifiers(
    query: &str,
) -> (Option<FormattedVectorBinaryModifiers>, &str) {
    let Some((matching_modifier, rest)) = split_leading_vector_matching_modifier(query) else {
        return (None, query.trim_start());
    };
    let (group_modifier, rest) = split_leading_vector_group_modifier(rest);
    (
        Some(match group_modifier {
            Some(group_modifier) => FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            None => FormattedVectorBinaryModifiers {
                text: matching_modifier,
                right_separator: "  ",
            },
        }),
        rest.trim_start(),
    )
}

pub(crate) fn split_leading_vector_matching_modifier(query: &str) -> Option<(String, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let labels = rest.trim_start().strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels_text = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels_text})"),
                &labels[labels_end + 1..],
            ));
        }
    }
    None
}

pub(crate) fn split_leading_vector_group_modifier(query: &str) -> (Option<String>, &str) {
    let query = query.trim_start();
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let rest = rest.trim_start();
            let Some(labels) = rest.strip_prefix('(') else {
                return (Some(modifier.to_string()), rest);
            };
            let Some(labels_end) = labels.find(')') else {
                return (None, query);
            };
            let labels_text = &labels[..labels_end];
            let modifier_text = if labels_text.is_empty() {
                modifier.to_string()
            } else {
                format!("{modifier} ({labels_text})")
            };
            return (Some(modifier_text), &labels[labels_end + 1..]);
        }
    }
    (None, query)
}
