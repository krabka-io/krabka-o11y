#![cfg_attr(test, allow(dead_code, unused_imports))]

use crate::{
    BTreeMap, ComparisonOp, MetricBinarySetOp, MetricScalarArithmeticOp, MetricVectorGroupModifier,
    MetricVectorMatching, TimeRange, Value, eval_times, json, loki_success_value,
    parse_logql_string_argument,
    querier::metric_eval::validation::{
        VectorScalarExpressionParser, scalar_vector_query_is_vector,
    },
    split_logql_function_arguments, split_top_level_arithmetic_query,
    split_top_level_comparison_query, split_top_level_set_query, unix_ns_string_to_loki_seconds,
};

struct ParsedBoolModifier<'a> {
    bool_modifier: bool,
    remaining: &'a str,
}

fn parse_leading_bool_modifier(query: &str) -> ParsedBoolModifier<'_> {
    let query = query.trim_start();
    if let Some(rest) = query.strip_prefix("bool") {
        ParsedBoolModifier {
            bool_modifier: true,
            remaining: rest.trim_start(),
        }
    } else {
        ParsedBoolModifier {
            bool_modifier: false,
            remaining: query,
        }
    }
}

struct OrderedMetricVectors<'a> {
    left: &'a str,
    right: &'a str,
    scalar_on_left: bool,
}

fn metric_vector_order<'a>(left: &'a str, right: &'a str) -> Option<OrderedMetricVectors<'a>> {
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    if !left_is_vector && right_is_vector {
        Some(OrderedMetricVectors {
            left,
            right,
            scalar_on_left: false,
        })
    } else if left_is_vector && !right_is_vector {
        Some(OrderedMetricVectors {
            left: right,
            right: left,
            scalar_on_left: true,
        })
    } else {
        None
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LabelReplaceExpression {
    pub(crate) query: String,
    pub(crate) destination_label: String,
    pub(crate) replacement: String,
    pub(crate) source_label: String,
    pub(crate) pattern: String,
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

struct ParsedLabelReplaceOperands<'a> {
    left: &'a str,
    right: &'a str,
    matching: Option<MetricVectorMatching>,
    bool_modifier: bool,
}

fn parse_label_replace_operands<'a>(
    left: &'a str,
    right: &'a str,
    allow_group_modifier: bool,
    has_bool_modifier: bool,
) -> Option<ParsedLabelReplaceOperands<'a>> {
    let bool_modifier = if has_bool_modifier {
        parse_leading_bool_modifier(right)
    } else {
        ParsedBoolModifier {
            bool_modifier: false,
            remaining: right,
        }
    };
    let parsed_matching = parse_leading_metric_vector_matching_modifier(
        bool_modifier.remaining,
        allow_group_modifier,
    )?;
    let left = left.trim();
    let right = parsed_matching.remaining.trim();
    (parse_label_replace_expression(left).is_some()
        || parse_label_replace_expression(right).is_some())
    .then_some(ParsedLabelReplaceOperands {
        left,
        right,
        matching: parsed_matching.matching,
        bool_modifier: bool_modifier.bool_modifier,
    })
}

pub(crate) fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some(split) = split_top_level_arithmetic_query(query)
        && let Some(operands) = parse_label_replace_operands(split.left, split.right, true, false)
    {
        return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
            left: operands.left.to_string(),
            op: parse_metric_arithmetic_operator(split.operator)?,
            matching: operands.matching,
            right: operands.right.to_string(),
        });
    }

    if let Some(split) = split_top_level_comparison_query(query)
        && let Some(operands) = parse_label_replace_operands(split.left, split.right, true, true)
    {
        return Some(LabelReplaceMetricBinaryExpression::Comparison {
            left: operands.left.to_string(),
            op: parse_metric_comparison_operator(split.operator)?,
            bool_modifier: operands.bool_modifier,
            matching: operands.matching,
            right: operands.right.to_string(),
        });
    }

    if let Some(split) = split_top_level_set_query(query)
        && let Some(operands) = parse_label_replace_operands(split.left, split.right, false, false)
    {
        return Some(LabelReplaceMetricBinaryExpression::Set {
            left: operands.left.to_string(),
            op: parse_metric_set_operator(split.operator)?,
            matching: operands.matching,
            right: operands.right.to_string(),
        });
    }

    None
}

pub(crate) struct ParsedLabelList<'a> {
    pub(crate) labels: Vec<String>,
    pub(crate) remaining: &'a str,
}

pub(crate) fn parse_leading_label_list(query: &str) -> Option<ParsedLabelList<'_>> {
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
    Some(ParsedLabelList {
        labels,
        remaining: &inner[labels_end + 1..],
    })
}

pub(crate) struct ParsedMetricVectorGroupModifier<'a> {
    pub(crate) group: Option<MetricVectorGroupModifier>,
    pub(crate) remaining: &'a str,
}

pub(crate) fn parse_leading_metric_vector_group_modifier(
    query: &str,
) -> Option<ParsedMetricVectorGroupModifier<'_>> {
    for modifier in ["group_left", "group_right"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let rest = rest.trim_start();
        let (labels, remaining) = if rest.starts_with('(') {
            let parsed = parse_leading_label_list(rest)?;
            (parsed.labels, parsed.remaining)
        } else {
            (Vec::new(), rest)
        };
        let group = match modifier {
            "group_left" => MetricVectorGroupModifier::Left(labels),
            "group_right" => MetricVectorGroupModifier::Right(labels),
            _ => unreachable!("modifier loop only produces known group modifiers"),
        };
        return Some(ParsedMetricVectorGroupModifier {
            group: Some(group),
            remaining,
        });
    }

    Some(ParsedMetricVectorGroupModifier {
        group: None,
        remaining: query,
    })
}

pub(crate) struct ParsedMetricVectorMatchingModifier<'a> {
    pub(crate) matching: Option<MetricVectorMatching>,
    pub(crate) remaining: &'a str,
}

pub(crate) fn parse_leading_metric_vector_matching_modifier(
    query: &str,
    allow_group_modifier: bool,
) -> Option<ParsedMetricVectorMatchingModifier<'_>> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let parsed_labels = parse_leading_label_list(rest.trim_start())?;
        let parsed_group =
            parse_leading_metric_vector_group_modifier(parsed_labels.remaining.trim_start())?;
        let labels = parsed_labels.labels;
        let rest = parsed_group.remaining;
        let group = parsed_group.group;
        if group.is_some() && !allow_group_modifier {
            return None;
        }
        let matching = match modifier {
            "on" => MetricVectorMatching::On { labels, group },
            "ignoring" => MetricVectorMatching::Ignoring { labels, group },
            _ => unreachable!("modifier loop only produces known modifiers"),
        };
        return Some(ParsedMetricVectorMatchingModifier {
            matching: Some(matching),
            remaining: rest,
        });
    }

    Some(ParsedMetricVectorMatchingModifier {
        matching: None,
        remaining: query,
    })
}

pub(crate) fn parse_metric_vector_set_expression(query: &str) -> Option<MetricVectorSetExpression> {
    let split = split_top_level_set_query(query)?;
    let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, false)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let left = split.left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    if !left_is_vector && right_is_vector {
        Some(MetricVectorSetExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_set_operator(split.operator)?,
            matching,
        })
    } else if left_is_vector && !right_is_vector {
        Some(MetricVectorSetExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_set_operator(split.operator)?,
            matching,
        })
    } else {
        None
    }
}

pub(crate) struct SortVectorExpression {
    pub(crate) query: String,
    pub(crate) descending: bool,
}
pub(crate) fn parse_metric_set_operator(operator: &str) -> Option<MetricBinarySetOp> {
    match operator {
        "and" => Some(MetricBinarySetOp::And),
        "or" => Some(MetricBinarySetOp::Or),
        "unless" => Some(MetricBinarySetOp::Unless),
        _ => None,
    }
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

pub(crate) fn parse_metric_vector_arithmetic_expression(
    query: &str,
) -> Option<MetricVectorArithmeticExpression> {
    let split = split_top_level_arithmetic_query(query)?;
    let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, true)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let ordered = metric_vector_order(split.left.trim(), right.trim())?;
    Some(MetricVectorArithmeticExpression {
        metric_query: ordered.left.to_string(),
        vector_query: ordered.right.to_string(),
        vector_on_left: ordered.scalar_on_left,
        op: parse_metric_arithmetic_operator(split.operator)?,
        matching,
    })
}

pub(crate) fn parse_metric_vector_comparison_expression(
    query: &str,
) -> Option<MetricVectorComparisonExpression> {
    let split = split_top_level_comparison_query(query)?;
    let bool_modifier = parse_leading_bool_modifier(split.right);
    let parsed_matching =
        parse_leading_metric_vector_matching_modifier(bool_modifier.remaining, true)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let ordered = metric_vector_order(split.left.trim(), right.trim())?;
    Some(MetricVectorComparisonExpression {
        metric_query: ordered.left.to_string(),
        vector_query: ordered.right.to_string(),
        vector_on_left: ordered.scalar_on_left,
        op: parse_metric_comparison_operator(split.operator)?,
        bool_modifier: bool_modifier.bool_modifier,
        matching,
    })
}

pub(crate) fn parse_sort_vector_expression(query: &str) -> Option<SortVectorExpression> {
    for function_name in ["sort", "sort_desc"] {
        let descending = function_name == "sort_desc";
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
        ScalarVectorExpressionResult::Scalar {
            sample: scalar_sample,
        } => (Some(scalar_sample), BTreeMap::new()),
        ScalarVectorExpressionResult::Vector {
            sample: vector_sample,
            metric: vector_metric,
        } => (vector_sample, vector_metric),
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

pub(crate) fn strip_outer_parenthesized_expression(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter = None;
    let mut escaped = false;
    for character in trimmed.char_indices() {
        let index = character.0;
        let ch = character.1;
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
