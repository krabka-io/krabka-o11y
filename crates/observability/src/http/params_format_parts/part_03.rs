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

