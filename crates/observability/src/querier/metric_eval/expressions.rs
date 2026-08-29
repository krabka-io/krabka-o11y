use super::*;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LabelReplaceExpression {
    pub(crate) query: String,
    pub(crate) destination_label: String,
    pub(crate) replacement: String,
    pub(crate) source_label: String,
    pub(crate) pattern: String,
}

pub(crate) struct SortVectorExpression {
    pub(crate) query: String,
    pub(crate) descending: bool,
}

pub(crate) enum LabelReplaceMetricBinaryExpression {
    Arithmetic {
        left: String,
        op: MetricScalarArithmeticOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Comparison {
        left: String,
        op: ComparisonOp,
        bool_modifier: bool,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Set {
        left: String,
        op: MetricBinarySetOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
}

pub(crate) struct MetricVectorArithmeticExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: MetricScalarArithmeticOp,
    pub(crate) matching: Option<MetricVectorMatching>,
}

pub(crate) struct MetricVectorComparisonExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: ComparisonOp,
    pub(crate) bool_modifier: bool,
    pub(crate) matching: Option<MetricVectorMatching>,
}

pub(crate) struct MetricVectorSetExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: MetricBinarySetOp,
    pub(crate) matching: Option<MetricVectorMatching>,
}

pub(crate) fn parse_label_replace_expression(query: &str) -> Option<LabelReplaceExpression> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    let [
        inner_query,
        destination_label,
        replacement,
        source_label,
        pattern,
    ] = arguments.as_slice()
    else {
        return None;
    };

    Some(LabelReplaceExpression {
        query: inner_query.to_string(),
        destination_label: parse_logql_string_argument(destination_label)?,
        replacement: parse_logql_string_argument(replacement)?,
        source_label: parse_logql_string_argument(source_label)?,
        pattern: parse_logql_string_argument(pattern)?,
    })
}

pub(crate) fn parse_sort_vector_expression(query: &str) -> Option<SortVectorExpression> {
    for (function_name, descending) in [("sort", false), ("sort_desc", true)] {
        let Some(arguments) = split_logql_function_arguments(query, function_name) else {
            continue;
        };
        let [inner_query] = arguments.as_slice() else {
            return None;
        };
        return Some(SortVectorExpression {
            query: inner_query.to_string(),
            descending,
        });
    }

    None
}

pub(crate) fn strip_outer_parenthesized_expression(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter = None;
    let mut escaped = false;
    for (index, ch) in trimmed.char_indices() {
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote_delimiter = None;
            }
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && index != trimmed.len() - ch.len_utf8() {
                    return None;
                }
            }
            _ => {}
        }
    }

    if depth == 0 {
        Some(trimmed[1..trimmed.len() - 1].trim())
    } else {
        None
    }
}

pub(crate) fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some((left, operator, right)) = split_top_level_arithmetic_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
                left: left.to_string(),
                op: parse_metric_arithmetic_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_comparison_query(query) {
        let right = right.trim_start();
        let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
            (true, rest.trim_start())
        } else {
            (false, right)
        };
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Comparison {
                left: left.to_string(),
                op: parse_metric_comparison_operator(operator)?,
                bool_modifier,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_set_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, false)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Set {
                left: left.to_string(),
                op: parse_metric_set_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    None
}

pub(crate) fn parse_metric_vector_arithmetic_expression(
    query: &str,
) -> Option<MetricVectorArithmeticExpression> {
    let (left, operator, right) = split_top_level_arithmetic_query(query)?;
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorArithmeticExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        (true, false) => Some(MetricVectorArithmeticExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        _ => None,
    }
}

pub(crate) fn parse_metric_vector_comparison_expression(
    query: &str,
) -> Option<MetricVectorComparisonExpression> {
    let (left, operator, right) = split_top_level_comparison_query(query)?;
    let right = right.trim_start();
    let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right)
    };
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorComparisonExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        (true, false) => Some(MetricVectorComparisonExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        _ => None,
    }
}

pub(crate) fn parse_metric_vector_set_expression(query: &str) -> Option<MetricVectorSetExpression> {
    let (left, operator, right) = split_top_level_set_query(query)?;
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, false)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorSetExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_set_operator(operator)?,
            matching,
        }),
        (true, false) => Some(MetricVectorSetExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_set_operator(operator)?,
            matching,
        }),
        _ => None,
    }
}

pub(crate) fn parse_leading_metric_vector_matching_modifier(
    query: &str,
    allow_group_modifier: bool,
) -> Option<(Option<MetricVectorMatching>, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let (labels, rest) = parse_leading_label_list(rest.trim_start())?;
        let (group, rest) = parse_leading_metric_vector_group_modifier(rest.trim_start())?;
        if group.is_some() && !allow_group_modifier {
            return None;
        }
        let matching = match modifier {
            "on" => MetricVectorMatching::On { labels, group },
            "ignoring" => MetricVectorMatching::Ignoring { labels, group },
            _ => unreachable!("modifier loop only produces known modifiers"),
        };
        return Some((Some(matching), rest));
    }

    Some((None, query))
}

pub(crate) fn parse_leading_label_list(query: &str) -> Option<(Vec<String>, &str)> {
    let inner = query.strip_prefix('(')?;
    let labels_end = inner.find(')')?;
    let labels_text = &inner[..labels_end];
    let labels = if labels_text.trim().is_empty() {
        Vec::new()
    } else {
        labels_text
            .split(',')
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
            .collect()
    };
    Some((labels, &inner[labels_end + 1..]))
}

pub(crate) fn parse_leading_metric_vector_group_modifier(
    query: &str,
) -> Option<(Option<MetricVectorGroupModifier>, &str)> {
    for modifier in ["group_left", "group_right"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let rest = rest.trim_start();
        let (labels, rest) = if rest.starts_with('(') {
            parse_leading_label_list(rest)?
        } else {
            (Vec::new(), rest)
        };
        let group = match modifier {
            "group_left" => MetricVectorGroupModifier::Left(labels),
            "group_right" => MetricVectorGroupModifier::Right(labels),
            _ => unreachable!("modifier loop only produces known group modifiers"),
        };
        return Some((Some(group), rest));
    }

    Some((None, query))
}

pub(crate) fn parse_metric_arithmetic_operator(operator: &str) -> Option<MetricScalarArithmeticOp> {
    match operator {
        "+" => Some(MetricScalarArithmeticOp::Add),
        "-" => Some(MetricScalarArithmeticOp::Subtract),
        "*" => Some(MetricScalarArithmeticOp::Multiply),
        "/" => Some(MetricScalarArithmeticOp::Divide),
        "%" => Some(MetricScalarArithmeticOp::Modulo),
        "^" => Some(MetricScalarArithmeticOp::Power),
        _ => None,
    }
}

pub(crate) fn parse_metric_comparison_operator(operator: &str) -> Option<ComparisonOp> {
    match operator {
        "==" => Some(ComparisonOp::Equal),
        "!=" => Some(ComparisonOp::NotEqual),
        ">" => Some(ComparisonOp::Greater),
        ">=" => Some(ComparisonOp::GreaterEqual),
        "<" => Some(ComparisonOp::Less),
        "<=" => Some(ComparisonOp::LessEqual),
        _ => None,
    }
}

pub(crate) fn parse_metric_set_operator(operator: &str) -> Option<MetricBinarySetOp> {
    match operator {
        "and" => Some(MetricBinarySetOp::And),
        "or" => Some(MetricBinarySetOp::Or),
        "unless" => Some(MetricBinarySetOp::Unless),
        _ => None,
    }
}

pub(crate) fn loki_instant_scalar_or_vector_response(
    timestamp_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let timestamp = unix_ns_string_to_loki_seconds(&timestamp_ns.to_string());
    match result {
        ScalarVectorExpressionResult::Scalar { sample } => loki_success_value(json!({
            "resultType": "scalar",
            "result": [timestamp, sample]
        })),
        ScalarVectorExpressionResult::Vector { sample, metric } => {
            let timestamp = json!(timestamp_ns);
            let result = sample.map_or_else(Vec::new, |sample| {
                vec![json!({
                    "metric": metric,
                    "value": [
                        timestamp,
                        sample
                    ]
                })]
            });
            loki_success_value(json!({
                "resultType": "vector",
                "result": result
            }))
        }
    }
}

pub(crate) fn loki_range_vector_response(
    time_range: TimeRange,
    step_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let (sample, metric) = match result {
        ScalarVectorExpressionResult::Scalar { sample } => (Some(sample), BTreeMap::new()),
        ScalarVectorExpressionResult::Vector { sample, metric } => (sample, metric),
    };
    let result = sample.map_or_else(Vec::new, |sample| {
        vec![json!({
            "metric": metric,
            "values": eval_times(time_range, step_ns)
                .into_iter()
                .map(|timestamp_ns| {
                    json!([
                        unix_ns_string_to_loki_seconds(&timestamp_ns.to_string()),
                        sample
                    ])
                })
                .collect::<Vec<_>>()
        })]
    });
    loki_success_value(json!({
        "resultType": "matrix",
        "result": result
    }))
}

#[derive(Clone)]
pub(crate) enum ScalarVectorExpressionResult {
    Scalar {
        sample: String,
    },
    Vector {
        sample: Option<String>,
        metric: BTreeMap<String, String>,
    },
}

pub(crate) fn scalar_vector_expression_result(query: &str) -> Option<ScalarVectorExpressionResult> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let mut parser = VectorScalarExpressionParser::new(&query);
    let result = parser.parse_result()?;
    if parser.is_finished() {
        Some(result)
    } else {
        None
    }
}
