fn form_body_query(body: &Bytes) -> Result<String, HttpQueryError> {
    String::from_utf8(body.to_vec()).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

/// Merges a POST query's URL query string with its form body, URL first.
///
/// The first arm's guard is a permanent mutation survivor against `true`:
/// dropping it lets an empty `raw_query` take that arm, and it is only reached
/// when the body is empty too, so the arm returns the same empty string the
/// fall-through would have.
fn post_query_params(raw_query: Option<&str>, body: &Bytes) -> Result<String, HttpQueryError> {
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
fn post_query_params_body_first(
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

fn execute_format_query(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let query = parse_format_query_param(raw_query)?;
    format_logql_query(&query)
}

fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
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

fn format_logql_query(query: &str) -> Result<String, HttpQueryError> {
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

fn label_join_format_query_error(query: &str) -> Option<String> {
    query
        .trim_start()
        .starts_with("label_join")
        .then(|| "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string())
}

fn format_label_replace_metric_binary_arithmetic(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
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

fn format_binary_operator_line(
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

fn format_label_replace_metric_binary_expression(
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

fn format_label_replace_metric_binary_operand(query: &str) -> Option<(String, bool)> {
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

fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
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

fn format_metric_vector_binary_expression(
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

fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
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

fn format_metric_binary_comparison_query(query: &str) -> Option<String> {
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

fn format_metric_binary_set_query(query: &str) -> Option<String> {
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

fn format_metric_binary_expression(
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

struct FormattedMetricVectorMatching {
    text: String,
    has_group: bool,
}

fn format_metric_vector_matching(matching: &MetricVectorMatching) -> FormattedMetricVectorMatching {
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

fn format_metric_vector_matching_text(
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

fn format_metric_vector_group_modifier(group: &MetricVectorGroupModifier) -> String {
    match group {
        MetricVectorGroupModifier::Left(labels) => {
            format_metric_vector_group_modifier_text("group_left", labels)
        }
        MetricVectorGroupModifier::Right(labels) => {
            format_metric_vector_group_modifier_text("group_right", labels)
        }
    }
}

fn format_metric_vector_group_modifier_text(modifier: &str, labels: &[String]) -> String {
    if labels.is_empty() {
        modifier.to_string()
    } else {
        format!("{modifier} ({})", labels.join(","))
    }
}

fn format_metric_binary_set_operator(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}

fn split_leading_vector_binary_modifiers(
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

fn split_leading_vector_matching_modifier(query: &str) -> Option<(String, &str)> {
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

fn split_leading_vector_group_modifier(query: &str) -> (Option<String>, &str) {
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

fn format_metric_vector_set_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
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

fn format_metric_scalar_arithmetic_expression(query: &str) -> Option<String> {
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

fn format_metric_scalar_comparison_expression(query: &str) -> Option<String> {
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

fn format_scalar_text(scalar: &str) -> Option<String> {
    Some(parse_scalar_sample(scalar)?.format())
}

fn format_metric_scalar_arithmetic_operator(op: MetricScalarArithmeticOp) -> &'static str {
    match op {
        MetricScalarArithmeticOp::Add => "+",
        MetricScalarArithmeticOp::Subtract => "-",
        MetricScalarArithmeticOp::Multiply => "*",
        MetricScalarArithmeticOp::Divide => "/",
        MetricScalarArithmeticOp::Modulo => "%",
        MetricScalarArithmeticOp::Power => "^",
    }
}

fn format_metric_scalar_comparison_operator(op: ComparisonOp) -> Option<&'static str> {
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

fn format_metric_label_replace_query(query: &str) -> Option<String> {
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

fn format_label_replace_metric_scalar_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_metric_scalar_vector_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace({vector},{},{},{},{})",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

fn format_metric_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    None
}

fn format_label_replace_metric_vector_expression(query: &str) -> Option<String> {
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

fn format_mixed_metric_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_sort_vector_expression(query) {
        return Some(formatted);
    }
    None
}

fn format_sort_vector_expression(query: &str) -> Option<String> {
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

fn format_loki_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_scalar_vector_expression(query) {
        return Some(formatted);
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_metric_query(&query))
}

fn indent_logql_lines(value: &str, prefix: &str) -> String {
    value
        .lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_logql_quoted_string(value: &str) -> String {
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

fn split_top_level_set_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            ch if parens == 0 && brackets == 0 && braces == 0 && ch.is_ascii_alphabetic() => {
                for operator in ["unless", "and", "or"] {
                    if query[index..].starts_with(operator)
                        && has_word_boundary(query, index, operator.len())
                    {
                        return Some((
                            &query[..index],
                            operator,
                            query[index + operator.len()..].trim_start(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn has_word_boundary(query: &str, index: usize, len: usize) -> bool {
    query[..index]
        .chars()
        .next_back()
        .is_none_or(char::is_whitespace)
        && query[index + len..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}

fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
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

    match (bool_modifier, modifiers) {
        (true, Some(modifiers)) => Some(format!(
            "({left} {operator} bool {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (true, None) => Some(format!("({left} {operator} bool {right})")),
        (false, Some(modifiers)) => Some(format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (false, None) => Some(format!("({left} {operator} {right})")),
    }
}

fn split_top_level_comparison_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            '>' | '<' | '=' | '!' if parens == 0 && brackets == 0 && braces == 0 => {
                for operator in [">=", "<=", "==", "!=", ">", "<"] {
                    if query[index..].starts_with(operator) {
                        return Some((
                            &query[..index],
                            operator,
                            query[index + operator.len()..].trim_start(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_arithmetic_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            '+' | '-' | '*' | '/' | '%' | '^' if parens == 0 && brackets == 0 && braces == 0 => {
                let right = query[index + ch.len_utf8()..].trim_start();
                return Some((
                    &query[..index],
                    match ch {
                        '+' => "+",
                        '-' => "-",
                        '*' => "*",
                        '/' => "/",
                        '%' => "%",
                        '^' => "^",
                        _ => unreachable!(),
                    },
                    right,
                ));
            }
            _ => {}
        }
    }
    None
}

fn format_simple_metric_query(query: &MetricQuery) -> Option<String> {
    if query.vector_aggregation.is_some() || query.range_grouping.is_some() {
        return None;
    }
    format_metric_range_aggregation_query(query)
}

fn format_metric_query(query: &MetricQuery) -> Option<String> {
    let mut formatted = format_metric_range_aggregation_query(query)?;
    if let Some(grouping) = &query.range_grouping {
        formatted = format!("{formatted} {}", format_vector_grouping(grouping));
    }
    if let Some(vector_aggregation) = &query.vector_aggregation {
        formatted = format_vector_aggregation_query(vector_aggregation, &formatted)?;
    }
    Some(formatted)
}

fn format_metric_range_aggregation_query(query: &MetricQuery) -> Option<String> {
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

fn format_metric_range_selector(query: &MetricQuery) -> Option<String> {
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

fn format_vector_aggregation_query(aggregation: &VectorAggregation, inner: &str) -> Option<String> {
    let grouping = aggregation
        .grouping
        .as_ref()
        .map(|grouping| format!(" {}", format_vector_grouping(grouping)))
        .unwrap_or_default();
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

fn format_vector_grouping(grouping: &VectorGrouping) -> String {
    match grouping {
        VectorGrouping::By(labels) => format!("by ({})", labels.join(",")),
        VectorGrouping::Without(labels) => format!("without ({})", labels.join(",")),
    }
}

fn format_loki_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let mut formatted = String::new();
    for (unit_ns, suffix) in [
        (3_600_000_000_000_i64, "h"),
        (60_000_000_000_i64, "m"),
        (1_000_000_000_i64, "s"),
        (1_000_000_i64, "ms"),
        (1_000_i64, "us"),
        (1_i64, "ns"),
    ] {
        if remaining >= unit_ns {
            let value = remaining / unit_ns;
            remaining %= unit_ns;
            write!(formatted, "{value}{suffix}").expect("writing to a String cannot fail");
        }
    }
    Some(formatted)
}

fn format_loki_offset_duration_ns(duration_ns: i64) -> Option<String> {
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

fn format_loki_offset_seconds(duration_ns: i64) -> String {
    format_loki_decimal_unit(duration_ns, 1_000_000_000, 9, "s")
}

fn format_loki_decimal_unit(duration_ns: i64, unit_ns: i64, width: usize, suffix: &str) -> String {
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

fn format_quantile(quantile: Quantile) -> String {
    ScalarSample::new(
        i128::from(quantile.numerator.0),
        u128::from(quantile.denominator.0),
    )
    .format()
}

fn format_range_aggregation_name(aggregation: &RangeAggregation) -> Option<&'static str> {
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

fn format_vector_function_text(query: &str) -> Option<String> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let (formatted, end) = parse_formatted_vector_function(&query, 0)?;
    (end == query.len()).then_some(formatted)
}

fn format_scalar_vector_expression(query: &str) -> Option<String> {
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
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    match scalar_vector_expression_result(&query)? {
        ScalarVectorExpressionResult::Scalar { sample } => Some(sample),
        ScalarVectorExpressionResult::Vector { .. } => None,
    }
}

fn format_vector_label_replace_function(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_vector_only_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace({vector},{},{},{},{})",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

fn format_vector_only_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    None
}

fn split_logql_function_arguments<'a>(query: &'a str, name: &str) -> Option<Vec<&'a str>> {
    let query = query.trim();
    let rest = query.strip_prefix(name)?.trim_start();
    let rest = rest.strip_prefix('(')?;
    let mut arguments = Vec::new();
    let mut start = 0;
    let mut parens = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in rest.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
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

fn parse_logql_string_argument(argument: &str) -> Option<String> {
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

fn format_vector_set_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    for operator in ["unless", "and", "or"] {
        if let Some(rest) = query[position..].strip_prefix(operator) {
            let mut right_position = query.len() - rest.len();
            let modifiers = if let Some((modifiers, next_position)) =
                parse_vector_binary_modifiers(query, right_position)
            {
                right_position = next_position;
                Some(modifiers)
            } else {
                None
            };
            let (right, end) = parse_formatted_vector_function(query, right_position)?;
            if end == query.len() {
                return Some(match modifiers {
                    Some(modifiers) => format!(
                        "({left} {operator} {}{}{right})",
                        modifiers.text, modifiers.right_separator
                    ),
                    None => format!("({left} {operator} {right})"),
                });
            }
        }
    }
    None
}

fn format_vector_comparison_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_comparison_operator(query, position)?;
    let bool_modifier = query[right_position..].starts_with("bool");
    if bool_modifier {
        right_position += "bool".len();
    }
    let modifiers = if let Some((modifiers, next_position)) =
        parse_vector_binary_modifiers(query, right_position)
    {
        right_position = next_position;
        Some(modifiers)
    } else {
        None
    };
    let (right, end) = parse_formatted_vector_function(query, right_position)?;
    if end != query.len() {
        return None;
    }
    match (bool_modifier, modifiers) {
        (true, Some(modifiers)) => Some(format!(
            "({left} {operator} bool {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (true, None) => Some(format!("({left} {operator} bool {right})")),
        (false, Some(modifiers)) => Some(format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (false, None) => Some(format!("({left} {operator} {right})")),
    }
}

fn parse_vector_comparison_operator(query: &str, position: usize) -> Option<(&'static str, usize)> {
    for operator in [">=", "<=", "==", "!=", ">", "<"] {
        if query[position..].starts_with(operator) {
            return Some((operator, position + operator.len()));
        }
    }
    None
}

fn format_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_arithmetic_operator(query, position)?;
    let modifiers = if let Some((modifiers, next_position)) =
        parse_vector_binary_modifiers(query, right_position)
    {
        right_position = next_position;
        Some(modifiers)
    } else {
        None
    };
    let (right, end) = parse_formatted_vector_function(query, right_position)?;
    if end == query.len() {
        Some(match modifiers {
            Some(modifiers) => format!(
                "({left} {operator} {}{}{right})",
                modifiers.text, modifiers.right_separator
            ),
            None => format!("({left} {operator} {right})"),
        })
    } else {
        None
    }
}

struct FormattedVectorBinaryModifiers {
    text: String,
    right_separator: &'static str,
}

fn parse_vector_binary_modifiers(
    query: &str,
    position: usize,
) -> Option<(FormattedVectorBinaryModifiers, usize)> {
    let (matching_modifier, position) = parse_vector_matching_modifier(query, position)?;
    if let Some((group_modifier, position)) = parse_vector_group_modifier(query, position) {
        return Some((
            FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            position,
        ));
    }
    Some((
        FormattedVectorBinaryModifiers {
            text: matching_modifier,
            right_separator: "  ",
        },
        position,
    ))
}

fn parse_vector_matching_modifier(query: &str, position: usize) -> Option<(String, usize)> {
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let labels = rest.strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}

fn parse_vector_group_modifier(query: &str, position: usize) -> Option<(String, usize)> {
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let Some(labels) = rest.strip_prefix('(') else {
                return Some((modifier.to_string(), position + modifier.len()));
            };
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            if labels.is_empty() {
                return Some((modifier.to_string(), position + modifier.len() + 2));
            }
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}

fn parse_vector_arithmetic_operator(query: &str, position: usize) -> Option<(&'static str, usize)> {
    for (raw, formatted) in [
        ("+", "+"),
        ("-", "-"),
        ("*", "*"),
        ("/", "/"),
        ("%", "%"),
        ("^", "^"),
    ] {
        if query[position..].starts_with(raw) {
            return Some((formatted, position + raw.len()));
        }
    }
    None
}

fn parse_formatted_vector_function(query: &str, position: usize) -> Option<(String, usize)> {
    if let Some(scalar) = query[position..].strip_prefix("vector(") {
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        let sample = parse_scalar_sample(scalar_text)?.format_fixed_six();
        return Some((
            format!("vector({sample})"),
            position + "vector(".len() + scalar_end + 1,
        ));
    }

    let call_end = find_logql_function_call_end(query, position, "label_replace")?;
    let formatted = format_vector_label_replace_function(&query[position..call_end])?;
    Some((formatted, call_end))
}

fn find_logql_function_call_end(query: &str, position: usize, name: &str) -> Option<usize> {
    let rest = &query[position..];
    let rest = rest.strip_prefix(name)?;
    let mut chars = rest.char_indices();
    let (_, first) = chars.next()?;
    if first != '(' {
        return None;
    }

    let mut parens = 1_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in chars {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => {
                parens -= 1;
                if parens == 0 {
                    return Some(position + name.len() + index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

fn format_stream_query(query: &StreamQuery) -> String {
    let mut formatted = format!(
        "{{{}}}",
        query
            .matchers
            .iter()
            .map(format_label_matcher)
            .collect::<Vec<_>>()
            .join(",")
    );
    for stage in &query.pipeline {
        if matches!(stage, PipelineStage::LineFilter(_)) {
            formatted.push(' ');
        } else {
            formatted.push_str(" | ");
        }
        formatted.push_str(&format_pipeline_stage(stage));
    }
    formatted
}

fn format_label_matcher(matcher: &krabka_logql::LabelMatcher) -> String {
    format!(
        "{}{}{}",
        matcher.name,
        match matcher.op {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::RegexEqual => "=~",
            MatchOp::RegexNotEqual => "!~",
        },
        quote_logql_string(&matcher.value)
    )
}

fn format_pipeline_stage(stage: &PipelineStage) -> String {
    match stage {
        PipelineStage::LineFilter(filter) => {
            let value = if filter.is_ip_matcher() {
                format!("ip({})", quote_logql_string(&filter.pattern))
            } else {
                quote_logql_string(&filter.pattern)
            };
            format!(
                "{} {value}",
                match filter.op {
                    LineFilterOp::Contains => "|=",
                    LineFilterOp::NotContains => "!=",
                    LineFilterOp::Regex => "|~",
                    LineFilterOp::NotRegex => "!~",
                    LineFilterOp::Pattern => "|>",
                    LineFilterOp::NotPattern => "!>",
                }
            )
        }
        PipelineStage::Decolorize => "decolorize".to_string(),
        PipelineStage::Parser(ParserStage::Json) => "json".to_string(),
        PipelineStage::Parser(ParserStage::JsonSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    format!(
                        "{}={}",
                        extraction.destination(),
                        quote_logql_string(extraction.expression())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("json {extractions}")
        }
        PipelineStage::Parser(ParserStage::Logfmt) => "logfmt".to_string(),
        PipelineStage::Parser(ParserStage::LogfmtConfigured(config)) => {
            format!("logfmt{}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::LogfmtSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    if extraction.destination() == extraction.source() {
                        extraction.destination().to_string()
                    } else {
                        format!(
                            "{}={}",
                            extraction.destination(),
                            quote_logql_string(extraction.source())
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("logfmt{} {extractions}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::Unpack) => "unpack".to_string(),
        PipelineStage::Parser(ParserStage::Pattern(pattern)) => {
            format!("pattern {}", quote_logql_string(pattern.pattern()))
        }
        PipelineStage::Parser(ParserStage::Regexp(parser)) => {
            format!("regexp {}", quote_logql_string(parser.pattern()))
        }
        PipelineStage::LineFormat(format) => {
            format!("line_format {}", quote_logql_string(format.template()))
        }
        PipelineStage::LabelFormat(format) => {
            let assignments = format
                .assignments()
                .iter()
                .map(|assignment| {
                    let value = match assignment.value() {
                        LabelFormatValue::Rename(source) => source.clone(),
                        LabelFormatValue::Template(template) => {
                            quote_logql_string(template.template())
                        }
                    };
                    format!("{}={value}", assignment.destination())
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("label_format {assignments}")
        }
        PipelineStage::DropLabels(selections) => {
            format!("drop {}", format_label_selection_set(selections))
        }
        PipelineStage::KeepLabels(selections) => {
            format!("keep {}", format_label_selection_set(selections))
        }
        PipelineStage::Unwrap(unwrap) => match unwrap.conversion() {
            UnwrapConversion::Raw => format!("unwrap {}", unwrap.label()),
            UnwrapConversion::Bytes => format!("unwrap bytes({})", unwrap.label()),
            UnwrapConversion::Duration => format!("unwrap duration({})", unwrap.label()),
        },
        PipelineStage::FieldFilter(filter) => format_field_filter(filter),
        PipelineStage::FieldFilterChain(chain) => {
            let mut formatted = format_field_filter(chain.first());
            for (op, filter) in chain.rest() {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter(filter));
            }
            formatted
        }
        PipelineStage::FieldFilterExpression(expression) => {
            format_field_filter_expression(expression)
        }
    }
}

fn format_logfmt_parser_flags(config: &LogfmtParserConfig) -> String {
    let mut flags = Vec::new();
    if config.keep_empty() {
        flags.push("--keep-empty");
    }
    if config.strict() {
        flags.push("--strict");
    }
    if flags.is_empty() {
        String::new()
    } else {
        format!(" {}", flags.join(" "))
    }
}

fn format_label_selection_set(selections: &LabelSelectionSet) -> String {
    selections
        .selections()
        .iter()
        .map(|selection| {
            let Some(matcher) = selection.matcher() else {
                return selection.name_str().to_string();
            };
            match matcher {
                LabelSelectionMatcher::Equal(value) => {
                    format!("{}={}", selection.name_str(), quote_logql_string(value))
                }
                LabelSelectionMatcher::Regex(pattern) => {
                    format!("{}=~{}", selection.name_str(), quote_logql_string(pattern))
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_field_filter(filter: &FieldFilter) -> String {
    format!(
        "{}{}{}",
        filter.name,
        match filter.op {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::RegexEqual => "=~",
            ComparisonOp::RegexNotEqual => "!~",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterEqual => ">=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessEqual => "<=",
        },
        match &filter.value {
            FieldValue::Number(value) => value.to_string(),
            FieldValue::Duration(value) => format!("{value}ns"),
            FieldValue::Bytes(value) => format!("{}B", value.bytes_f64()),
            FieldValue::String(value) => quote_logql_string(value),
            FieldValue::Ip(value) => format!("ip({})", quote_logql_string(value.pattern())),
        }
    )
}

fn format_field_filter_expression(expression: &FieldFilterExpression) -> String {
    match expression {
        FieldFilterExpression::Filter(filter) => format_field_filter(filter),
        FieldFilterExpression::Group(expression) => {
            format!("({})", format_field_filter_expression(expression))
        }
        FieldFilterExpression::Chain { first, rest } => {
            let mut formatted = format_field_filter_expression(first);
            for (op, expression) in rest {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter_expression(expression));
            }
            formatted
        }
    }
}

fn quote_logql_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization cannot fail")
}

fn validate_query_series_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_series) = state.max_query_series else {
        return Ok(());
    };
    let series = plan.fingerprints.len();
    if series > max_query_series {
        return Err(HttpQueryError::QuerySeriesTooLarge {
            series,
            max_series: max_query_series,
        });
    }
    Ok(())
}

fn validate_query_bytes_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_read) = state.max_query_read else {
        return Ok(());
    };
    let planned = planned_block_bytes(plan);
    if planned > max_query_read {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(HttpQueryError::QueryBytesTooLarge {
            planned_bytes: planned.bytes_u64(),
            max_bytes: max_query_read.bytes_u64(),
        });
    }
    Ok(())
}

