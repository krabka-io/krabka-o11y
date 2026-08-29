#[derive(Clone, Debug, PartialEq)]
struct LabelReplaceExpression {
    query: String,
    destination_label: String,
    replacement: String,
    source_label: String,
    pattern: String,
}

struct SortVectorExpression {
    query: String,
    descending: bool,
}

enum LabelReplaceMetricBinaryExpression {
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

struct MetricVectorArithmeticExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: MetricScalarArithmeticOp,
    matching: Option<MetricVectorMatching>,
}

struct MetricVectorComparisonExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<MetricVectorMatching>,
}

struct MetricVectorSetExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: MetricBinarySetOp,
    matching: Option<MetricVectorMatching>,
}

fn parse_label_replace_expression(query: &str) -> Option<LabelReplaceExpression> {
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

fn parse_sort_vector_expression(query: &str) -> Option<SortVectorExpression> {
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

fn strip_outer_parenthesized_expression(query: &str) -> Option<&str> {
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

fn parse_label_replace_metric_binary_expression(
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

fn parse_metric_vector_arithmetic_expression(
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

fn parse_metric_vector_comparison_expression(
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

fn parse_metric_vector_set_expression(query: &str) -> Option<MetricVectorSetExpression> {
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

fn parse_leading_metric_vector_matching_modifier(
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

fn parse_leading_label_list(query: &str) -> Option<(Vec<String>, &str)> {
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

fn parse_leading_metric_vector_group_modifier(
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

fn parse_metric_arithmetic_operator(operator: &str) -> Option<MetricScalarArithmeticOp> {
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

fn parse_metric_comparison_operator(operator: &str) -> Option<ComparisonOp> {
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

fn parse_metric_set_operator(operator: &str) -> Option<MetricBinarySetOp> {
    match operator {
        "and" => Some(MetricBinarySetOp::And),
        "or" => Some(MetricBinarySetOp::Or),
        "unless" => Some(MetricBinarySetOp::Unless),
        _ => None,
    }
}

fn loki_instant_scalar_or_vector_response(
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

fn loki_range_vector_response(
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
enum ScalarVectorExpressionResult {
    Scalar {
        sample: String,
    },
    Vector {
        sample: Option<String>,
        metric: BTreeMap<String, String>,
    },
}

fn scalar_vector_expression_result(query: &str) -> Option<ScalarVectorExpressionResult> {
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

fn scalar_vector_query_is_vector(query: &str) -> bool {
    matches!(
        scalar_vector_expression_result(query),
        Some(ScalarVectorExpressionResult::Vector { .. })
    )
}

fn reject_signed_vector_function_literal(query: &str) -> Result<(), HttpQueryError> {
    scalar_vector_plain_parse_error(query)
        .map(HttpQueryError::LokiPlainParse)
        .map_or(Ok(()), Err)
}

fn scalar_vector_plain_parse_error(query: &str) -> Option<String> {
    signed_vector_function_literal_error(query)
        .or_else(|| unspaced_vector_set_operator_error(query))
}

fn signed_vector_function_literal_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is always on a char boundary");
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += ch.len_utf8();
            continue;
        }
        if query[index..].starts_with("vector(") {
            let mut sign_index = index + "vector(".len();
            while let Some(next) = query[sign_index..].chars().next() {
                if !next.is_whitespace() {
                    break;
                }
                sign_index += next.len_utf8();
            }
            if let Some(sign @ ('+' | '-')) = query[sign_index..].chars().next() {
                let column = query[..sign_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected {sign}, expecting NUMBER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}

fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is always on a char boundary");
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += ch.len_utf8();
            continue;
        }
        if ch == ')' {
            let next_index = index + ch.len_utf8();
            if ["and", "or", "unless"]
                .iter()
                .any(|operator| query[next_index..].starts_with(operator))
            {
                let column = query[..next_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected IDENTIFIER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}

fn could_be_scalar_vector_expression(query: &str) -> bool {
    let trimmed = query.trim_start();
    let Some(first) = trimmed.chars().next() else {
        return false;
    };
    if first.is_ascii_digit() || matches!(first, '+' | '-' | '.' | '(') {
        return true;
    }
    // `== '_'` against `!= '_'` is a permanent survivor. The branch it guards
    // returns true only for three literal identifiers: a leading `_` cannot
    // begin any of them, and every other character the mutation newly admits
    // takes an empty identifier, which matches none of them.
    if first.is_ascii_alphabetic() || first == '_' {
        let ident_len = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(char::len_utf8)
            .sum::<usize>();
        return matches!(
            &trimmed[..ident_len],
            "vector" | "label_replace" | "label_join"
        );
    }
    false
}

fn apply_label_replace_to_loki_result(
    value: &mut Value,
    destination_label: &str,
    replacement: &str,
    source_label: &str,
    pattern: &str,
    query: &str,
) -> Result<(), HttpQueryError> {
    let regex = Regex::new(pattern).map_err(|error| HttpQueryError::LokiParse {
        query: query.to_string(),
        source: ParseError::Syntax {
            message: error.to_string(),
            position: 0,
        },
    })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let source_value = metric
            .get(source_label)
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(replacement, &mut destination_value);
            metric.insert(destination_label.to_string(), json!(destination_value));
        }
    }
    Ok(())
}

fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let joined = label_join
            .source_labels
            .iter()
            .map(|label| metric.get(label).and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(&label_join.separator);
        metric.insert(label_join.destination_label.clone(), json!(joined));
    }
}

struct VectorScalarExpressionParser<'a> {
    input: &'a str,
    position: usize,
    vector_terms: usize,
}

impl<'a> VectorScalarExpressionParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            position: 0,
            vector_terms: 0,
        }
    }

    fn parse_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        if self.input[self.position..].starts_with("label_replace(") {
            return self.parse_label_replace_result();
        }
        if self.input[self.position..].starts_with("label_join(") {
            return self.parse_label_join_result();
        }

        let left_vector_terms = self.vector_terms;
        let left = self.parse_expression()?;
        let left_contains_vector = self.vector_terms > left_vector_terms;
        if let Some(operator) = self.parse_set_operator() {
            let right_vector_terms = self.vector_terms;
            let _right = self.parse_expression()?;
            let right_contains_vector = self.vector_terms > right_vector_terms;
            if !left_contains_vector || !right_contains_vector {
                return None;
            }
            let sample = match operator {
                ScalarSetOp::And | ScalarSetOp::Or => Some(left.format()),
                ScalarSetOp::Unless => None,
            };
            return Some(ScalarVectorExpressionResult::Vector {
                sample,
                metric: BTreeMap::new(),
            });
        }

        let Some(operator) = self.parse_comparison_operator() else {
            return Some(if self.vector_terms > 0 {
                ScalarVectorExpressionResult::Vector {
                    sample: Some(left.format()),
                    metric: BTreeMap::new(),
                }
            } else {
                ScalarVectorExpressionResult::Scalar {
                    sample: left.format(),
                }
            });
        };

        let bool_modifier = self.consume_keyword("bool");
        let left_vector_terms = self.vector_terms;
        let has_matching_modifier = self.consume_vector_matching_modifier()?;
        let right_vector_terms = self.vector_terms;
        let right = self.parse_expression()?;
        self.validate_vector_matching_modifier(
            has_matching_modifier,
            left_vector_terms,
            right_vector_terms,
        )?;
        let comparison_matches = left.compare(operator, right)?;
        if self.vector_terms == 0 {
            if bool_modifier {
                return None;
            }
            return Some(ScalarVectorExpressionResult::Scalar {
                sample: if comparison_matches { "1" } else { "0" }.to_string(),
            });
        }
        let sample = if bool_modifier {
            Some(if comparison_matches { "1" } else { "0" }.to_string())
        } else if comparison_matches {
            Some(left.format())
        } else {
            None
        };
        Some(ScalarVectorExpressionResult::Vector {
            sample,
            metric: BTreeMap::new(),
        })
    }

    fn parse_label_replace_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_replace");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let replacement = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let source_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let pattern = self.parse_string_literal()?;
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let regex = Regex::new(&pattern).ok()?;
        let source_value = metric.get(&source_label).map_or("", String::as_str);
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(&replacement, &mut destination_value);
            metric.insert(destination_label, destination_value);
        }

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    fn parse_label_join_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_join");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let separator = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let mut source_labels = vec![self.parse_string_literal()?];
        while self.consume(',') {
            source_labels.push(self.parse_string_literal()?);
        }
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let joined = source_labels
            .iter()
            .map(|label| metric.get(label).map_or("", String::as_str))
            .collect::<Vec<_>>()
            .join(&separator);
        metric.insert(destination_label, joined);

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    fn parse_expression(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_product()?;
        loop {
            if self.consume('+') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.add(right)?;
            } else if self.consume('-') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.subtract(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    fn parse_product(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_power()?;
        loop {
            if self.consume('*') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.multiply(right)?;
            } else if self.consume('/') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.divide(right)?;
            } else if self.consume('%') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.modulo(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    fn parse_power(&mut self) -> Option<ScalarSample> {
        let sample = self.parse_primary()?;
        if self.consume('^') {
            let left_vector_terms = self.vector_terms;
            let has_matching_modifier = self.consume_vector_matching_modifier()?;
            let right_vector_terms = self.vector_terms;
            let right = self.parse_power()?;
            self.validate_vector_matching_modifier(
                has_matching_modifier,
                left_vector_terms,
                right_vector_terms,
            )?;
            sample.power(right)
        } else {
            Some(sample)
        }
    }

    fn parse_primary(&mut self) -> Option<ScalarSample> {
        if self.consume('(') {
            let sample = self.parse_expression()?;
            return self.consume(')').then_some(sample);
        }

        self.parse_vector_scalar()
            .or_else(|| self.parse_scalar_literal())
    }

    fn parse_comparison_operator(&mut self) -> Option<ScalarComparisonOp> {
        for (operator, op) in [
            (">=", ScalarComparisonOp::GreaterOrEqual),
            ("<=", ScalarComparisonOp::LessOrEqual),
            ("==", ScalarComparisonOp::Equal),
            ("!=", ScalarComparisonOp::NotEqual),
            (">", ScalarComparisonOp::Greater),
            ("<", ScalarComparisonOp::Less),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    fn parse_set_operator(&mut self) -> Option<ScalarSetOp> {
        for (operator, op) in [
            ("unless", ScalarSetOp::Unless),
            ("and", ScalarSetOp::And),
            ("or", ScalarSetOp::Or),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    fn consume_vector_matching_modifier(&mut self) -> Option<bool> {
        if self.consume_keyword("on") || self.consume_keyword("ignoring") {
            self.consume_label_list()?;
            self.consume_group_modifier()?;
            Some(true)
        } else {
            Some(false)
        }
    }

    fn consume_group_modifier(&mut self) -> Option<()> {
        if !(self.consume_keyword("group_left") || self.consume_keyword("group_right")) {
            return Some(());
        }
        if self.input[self.position..].starts_with('(') {
            self.consume_label_list()?;
        }
        Some(())
    }

    fn consume_label_list(&mut self) -> Option<()> {
        self.consume('(').then_some(())?;
        if self.consume(')') {
            return Some(());
        }

        loop {
            self.consume_label_name()?;
            if self.consume(')') {
                return Some(());
            }
            self.consume(',').then_some(())?;
        }
    }

    fn consume_label_name(&mut self) -> Option<()> {
        let bytes = self.input.as_bytes();
        let first = *bytes.get(self.position)?;
        if !matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_') {
            return None;
        }

        // `+= 1` against `*= 1` is a permanent survivor here. The first byte
        // has just been checked to be a letter or an underscore, and the loop
        // below accepts exactly those plus digits -- so leaving the position
        // on it lets the loop step over it instead, and the name ends in the
        // same place either way.
        self.position += 1;
        while matches!(
            bytes.get(self.position),
            Some(b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
        ) {
            self.position += 1;
        }
        Some(())
    }

    fn validate_vector_matching_modifier(
        &self,
        has_matching_modifier: bool,
        left_vector_terms: usize,
        right_vector_terms: usize,
    ) -> Option<()> {
        if !has_matching_modifier {
            return Some(());
        }

        let left_contains_vector = left_vector_terms > 0;
        let right_contains_vector = self.vector_terms > right_vector_terms;
        (left_contains_vector && right_contains_vector).then_some(())
    }

    fn parse_vector_scalar(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let scalar = rest.strip_prefix("vector(")?;
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        self.position += "vector(".len() + scalar_end + 1;
        let sample = parse_scalar_sample(scalar_text)?;
        self.vector_terms += 1;
        Some(sample)
    }

    fn parse_scalar_literal(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let literal_len = scalar_literal_len(rest)?;
        let sample = parse_scalar_sample(&rest[..literal_len])?;
        self.position += literal_len;
        Some(sample)
    }

    fn parse_string_literal(&mut self) -> Option<String> {
        self.consume('"').then_some(())?;
        let mut value = String::new();
        // `<` is a permanent mutation survivor against `<=`: the extra pass it
        // allows slices an empty remainder, whose first character is `None`,
        // and the `?` returns the same `None` the loop would have fallen to.
        while self.position < self.input.len() {
            let ch = self.input[self.position..].chars().next()?;
            self.position += ch.len_utf8();
            match ch {
                '"' => return Some(value),
                '\\' => {
                    let escaped = self.input[self.position..].chars().next()?;
                    self.position += escaped.len_utf8();
                    value.push(match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        other => other,
                    });
                }
                other => value.push(other),
            }
        }
        None
    }

    fn consume(&mut self, operator: char) -> bool {
        if self.input[self.position..].starts_with(operator) {
            self.position += operator.len_utf8();
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if self.input[self.position..].starts_with(keyword) {
            self.position += keyword.len();
            true
        } else {
            false
        }
    }

    fn is_finished(&self) -> bool {
        self.position == self.input.len()
    }
}

#[derive(Clone, Copy)]
enum ScalarSetOp {
    And,
    Or,
    Unless,
}

fn scalar_literal_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut position = 0;
    if matches!(bytes.get(position), Some(b'+' | b'-')) {
        position += 1;
    }

    let whole_start = position;
    while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
        position += 1;
    }
    let whole_digits = position > whole_start;

    let mut fractional_digits = false;
    if matches!(bytes.get(position), Some(b'.')) {
        position += 1;
        let fractional_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        fractional_digits = position > fractional_start;
    }

    if !whole_digits && !fractional_digits {
        return None;
    }

    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;
        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let exponent_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        if position == exponent_start {
            return None;
        }
    }

    Some(position)
}

#[derive(Clone, Copy)]
enum ScalarComparisonOp {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Clone, Copy)]
struct ScalarSample {
    numerator: i128,
    denominator: u128,
}

impl ScalarSample {
    fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self {
                numerator: 0,
                denominator: 1,
            };
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).unwrap_or(i128::MAX),
            denominator: denominator / divisor,
        }
    }

    fn add(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_add(right?)?, denominator))
    }

    fn subtract(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_sub(right?)?, denominator))
    }

    fn multiply(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator.checked_mul(other.numerator)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }

    fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        // `< 0` against `<= 0` is a permanent survivor: `ScalarSample::new`
        // normalises a zero denominator to one, and the divisor's numerator was
        // rejected above, so this product is never zero.
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
    }

    fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    fn compare(self, operator: ScalarComparisonOp, other: Self) -> Option<bool> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?)?;
        Some(match operator {
            ScalarComparisonOp::Equal => left == right,
            ScalarComparisonOp::NotEqual => left != right,
            ScalarComparisonOp::Greater => left > right,
            ScalarComparisonOp::GreaterOrEqual => left >= right,
            ScalarComparisonOp::Less => left < right,
            ScalarComparisonOp::LessOrEqual => left <= right,
        })
    }

    fn to_f64(self) -> Option<f64> {
        let value = self.numerator.to_f64()? / self.denominator.to_f64()?;
        value.is_finite().then_some(value)
    }

    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64()?).round();
        Some(Self::new(i128::from_f64(scaled)?, METRIC_DECIMAL_SCALE))
    }

    fn format(self) -> String {
        let negative = self.numerator < 0;
        let numerator = self.numerator.unsigned_abs();
        let whole = numerator / self.denominator;
        let mut remainder = numerator % self.denominator;
        let sign = if negative { "-" } else { "" };
        if remainder == 0 {
            return format!("{sign}{whole}");
        }

        let mut decimals = String::new();
        while remainder != 0 && decimals.len() < 9 {
            remainder *= 10;
            let digit =
                u8::try_from(remainder / self.denominator).expect("decimal digit is less than 10");
            decimals.push(char::from(b'0' + digit));
            remainder %= self.denominator;
        }
        while decimals.ends_with('0') {
            decimals.pop();
        }
        format!("{sign}{whole}.{decimals}")
    }

    fn format_fixed_six(self) -> String {
        format!("{:.6}", self.to_f64().unwrap_or_default())
    }
}

fn parse_scalar_sample(value: &str) -> Option<ScalarSample> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(ScalarSample::new(numerator, denominator))
}

fn gcd_signed(left: i128, right: u128) -> u128 {
    let mut left = left.unsigned_abs();
    let mut right = right;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn validate_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let Some(max_query_range) = state.max_query_range else {
        return Ok(());
    };
    // `start_ns` and `end_ns` are instants; only their difference is an extent.
    // The error carries plain nanoseconds so its rendered message is fixed by
    // the `#[error]` format string alone.
    let max_range_ns = max_query_range.nanos_i64();
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryRangeTooLarge {
            range_ns: i64::MAX,
            max_range_ns,
        })?;
    if query_range > max_query_range {
        return Err(HttpQueryError::QueryRangeTooLarge {
            range_ns: query_range.nanos_i64(),
            max_range_ns,
        });
    }
    Ok(())
}

fn validate_loki_volume_query_range_limit(time_range: TimeRange) -> Result<(), HttpQueryError> {
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(Time::from_nanos(i64::MAX)),
        })?;
    if query_range > LOKI_VOLUME_MAX_QUERY_RANGE {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range),
        });
    }
    Ok(())
}

fn validate_loki_range_query_range_limit(
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(time_range)?;
    }
    Ok(())
}

/// Resolves a range query's step in nanoseconds, defaulting it from the range.
///
/// `Loki` refuses a non-positive step outright rather than dividing by it, and
/// every range-vector response resolves its step through here.
fn resolved_range_step(step: Option<i64>, time_range: TimeRange) -> Result<i64, HttpQueryError> {
    let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    Ok(step_ns)
}

fn validate_loki_query_range_resolution(
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if !matches!(kind, QueryKind::Range) {
        return Ok(());
    }
    let step_ns = resolved_range_step(params.step, time_range)?;
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryResolutionTooHigh)?;
    // Loki truncates the point count, so the division stays over whole
    // nanoseconds rather than fractional seconds.
    if query_range.nanos_i64() / step_ns > LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS {
        return Err(HttpQueryError::QueryResolutionTooHigh);
    }
    Ok(())
}

/// Renders an extent the way `Loki` spells a query length in its own error text.
///
/// The whole seconds come from the nanosecond count by integer division, not
/// from [`TimeExt::secs_i64`]. That method rounds to nearest and would report a
/// second more than `Loki` does for the same window.
fn format_loki_query_length(range: Time) -> String {
    let total_seconds = range.nanos_i64().max(0) / 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;

    format!("{hours}h{minutes}m{seconds}s")
}

fn validate_query_length_limit(state: &QuerierState, query: &str) -> Result<(), HttpQueryError> {
    let Some(max_query_length) = state.max_query_length.map(ByteSizeExt::bytes_usize) else {
        return Ok(());
    };
    let query_length = query.len();
    if query_length > max_query_length {
        return Err(HttpQueryError::QueryLengthTooLarge {
            query_length,
            max_query_length,
        });
    }
    Ok(())
}

async fn execute_http_metric_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: MetricQuery,
) -> Result<Value, HttpQueryError> {
    if metric_query_uses_approx_topk(&query) {
        return Err(HttpQueryError::ApproxTopKDisabled);
    }
    if metric_query_uses_count_values(&query) {
        return Err(HttpQueryError::CountValuesQuery);
    }
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                (time_range, step_ns),
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }
    let response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            (eval_range, 1),
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

async fn execute_http_metric_expression_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: &str,
    full_query: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(sort) = parse_sort_vector_expression(query) {
        return Box::pin(execute_http_sort_vector_expression(
            state, tenant, time_range, step, kind, sort, full_query,
        ))
        .await;
    }
    if split_logql_function_arguments(query, "label_join").is_none()
        && let Some(result) = scalar_vector_expression_result(query)
    {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            label_replace.query.clone(),
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(label_replace) = parse_label_replace_expression(query) {
        let mut value = Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            full_query,
        ))
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(inner_query) = strip_outer_parenthesized_expression(query) {
        return Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            inner_query,
            full_query,
        ))
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(query) {
        return execute_http_metric_vector_arithmetic_expression(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(query) {
        return execute_http_metric_vector_comparison_expression(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(query) {
        return execute_http_metric_vector_set_expression(
            state, tenant, time_range, step, kind, set, full_query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(query) {
        return execute_http_metric_binary_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(query) {
        return execute_http_metric_binary_comparison_query(
            state, tenant, time_range, step, kind, comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(query) {
        return execute_http_metric_binary_set_query(state, tenant, time_range, step, kind, set)
            .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(query) {
        return execute_http_metric_scalar_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(query) {
        return execute_http_metric_scalar_comparison_query(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    let query = parse_metric_query(query).map_err(|source| HttpQueryError::LokiParse {
        query: full_query.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}

async fn execute_http_label_replace_metric_binary_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    binary: LabelReplaceMetricBinaryExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    match binary {
        LabelReplaceMetricBinaryExpression::Arithmetic {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_arithmetic_to_loki_result(&mut left, &right, op, matching.as_ref());
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Comparison {
            left,
            op,
            bool_modifier,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_comparison_to_loki_result(
                &mut left,
                &right,
                op,
                bool_modifier,
                matching.as_ref(),
            );
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Set {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_set_to_loki_result(&mut left, &right, op, matching.as_ref());
            Ok(left)
        }
    }
}

async fn execute_http_metric_binary_operand(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    operand: &str,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(label_replace) = parse_label_replace_expression(operand) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            query_text,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            query_text,
        )?;
        return Ok(value);
    }
    if scalar_vector_query_is_vector(operand) {
        return execute_http_scalar_vector_expression_result(
            operand, time_range, step, kind, query_text,
        );
    }

    let query = parse_metric_query(operand).map_err(|source| HttpQueryError::LokiParse {
        query: query_text.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}

async fn execute_http_sort_vector_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    sort: SortVectorExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let mut value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &sort.query,
        query_text,
    ))
    .await?;
    sort_loki_vector_result(&mut value, sort.descending);
    Ok(value)
}

async fn execute_http_metric_vector_arithmetic_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricVectorArithmeticExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &arithmetic.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &arithmetic.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if arithmetic.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &metric_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &vector_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        Ok(value)
    }
}

async fn execute_http_metric_vector_comparison_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricVectorComparisonExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &comparison.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &comparison.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if comparison.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &metric_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &vector_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        Ok(value)
    }
}

async fn execute_http_metric_vector_set_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricVectorSetExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &set.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &set.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if set.vector_on_left {
        let mut value = vector_value;
        if matches!(kind, QueryKind::Instant) {
            normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        }
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &metric_value,
            set.op,
            set.matching.as_ref(),
        );
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &vector_value,
            set.op,
            set.matching.as_ref(),
        );
        Ok(value)
    }
}

fn normalize_loki_vector_sample_timestamps_to_seconds(value: &mut Value) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(sample) = series.get_mut("value").and_then(Value::as_array_mut) else {
            continue;
        };
        let Some(timestamp) = sample.get_mut(0) else {
            continue;
        };
        *timestamp = match timestamp {
            Value::Number(number) => {
                let seconds = unix_ns_string_to_loki_seconds(&number.to_string());
                json!(seconds)
            }
            Value::String(text) => json!(unix_ns_string_to_loki_seconds(text)),
            _ => continue,
        };
    }
}

fn execute_http_scalar_vector_expression_result(
    query: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let vector_result =
        scalar_vector_expression_result(query).ok_or_else(|| HttpQueryError::LokiParse {
            query: query_text.to_string(),
            source: ParseError::Syntax {
                message: "expected vector expression".to_string(),
                position: 0,
            },
        })?;
    let value = match kind {
        QueryKind::Instant => {
            loki_instant_scalar_or_vector_response(time_range.end_ns, vector_result)
        }
        QueryKind::Range => loki_range_vector_response(
            time_range,
            resolved_range_step(step, time_range)?,
            vector_result,
        ),
    };
    Ok(add_loki_query_stats(value))
}

fn retain_metric_binary_on_labels(value: &mut Value, matching: Option<&MetricVectorMatching>) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        metric.retain(|label, _| labels.contains(label));
    }
}

fn sort_loki_vector_result(value: &mut Value, descending: bool) {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector") {
        return;
    }
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    results.sort_by(|left, right| {
        let ordering = match (
            loki_vector_sample_value(left),
            loki_vector_sample_value(right),
        ) {
            (Some(left), Some(right)) => left.cmp_value(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

fn loki_vector_sample_value(sample: &Value) -> Option<MetricValue> {
    sample
        .pointer("/value/1")
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
}

fn metric_query_uses_approx_topk(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
}

fn metric_query_uses_count_values(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::CountValues(_)))
}

async fn execute_http_metric_binary_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricBinaryArithmetic,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, arithmetic.right).await?;
    apply_metric_binary_arithmetic_to_loki_result(
        &mut left,
        &right,
        arithmetic.op,
        arithmetic.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricBinaryComparison,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        comparison.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, comparison.right).await?;
    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        comparison.op,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_set_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricBinarySet,
) -> Result<Value, HttpQueryError> {
    let mut left =
        execute_http_metric_query(state, tenant, time_range, step, kind, set.left.clone()).await?;
    let right = execute_http_metric_query(state, tenant, time_range, step, kind, set.right).await?;
    apply_metric_binary_set_to_loki_result(&mut left, &right, set.op, set.matching.as_ref());
    Ok(left)
}

async fn execute_http_metric_scalar_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricScalarComparison,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = comparison.query.clone();
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let mut response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                (time_range, step_ns),
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }

    let mut response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            (eval_range, 1),
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

async fn execute_http_metric_scalar_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricScalarArithmetic,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = arithmetic.query.clone();
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let mut response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                (time_range, step_ns),
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }

    let mut response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            (eval_range, 1),
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_arithmetic_group_right_to_results(
            left_results,
            right_results,
            op,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_arithmetic_to_series(&mut left_results[index], right_series, op) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn apply_metric_binary_arithmetic_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

fn apply_metric_binary_arithmetic_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample(&mut left_values[index], right_sample, op) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample(left_sample, right_sample, op)
}

fn apply_metric_binary_arithmetic_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample_operands(output_sample, left_sample, &right_sample, op)
}

fn matching_metric_binary_sample<'a>(
    left_sample: &Value,
    right_values: &'a [Value],
) -> Option<&'a Value> {
    right_values
        .iter()
        .find(|right_sample| metric_binary_sample_timestamps_match(left_sample, right_sample))
}

fn metric_binary_sample_timestamps_match(left_sample: &Value, right_sample: &Value) -> bool {
    match (
        metric_binary_sample_timestamp_ns_candidates(left_sample),
        metric_binary_sample_timestamp_ns_candidates(right_sample),
    ) {
        (Some(left), Some(right)) => left
            .iter()
            .any(|left_timestamp| right.contains(left_timestamp)),
        (None, None) => {
            left_sample.as_array().and_then(|sample| sample.first())
                == right_sample.as_array().and_then(|sample| sample.first())
        }
        _ => false,
    }
}

fn metric_binary_sample_timestamp_ns_candidates(sample: &Value) -> Option<Vec<i64>> {
    let timestamp = sample.as_array()?.first()?;
    if let Some(timestamp) = timestamp.as_i64() {
        return Some(metric_binary_integer_timestamp_ns_candidates(timestamp));
    }
    if let Some(timestamp) = timestamp.as_u64() {
        return i64::try_from(timestamp)
            .ok()
            .map(metric_binary_integer_timestamp_ns_candidates);
    }
    if let Some(timestamp) = timestamp.as_f64() {
        let timestamp = timestamp * 1_000_000_000.0;
        return i64::from_f64(timestamp.round()).map(|timestamp| vec![timestamp]);
    }
    if let Some(timestamp) = timestamp.as_str() {
        let mut candidates = Vec::new();
        if let Some(timestamp) = parse_decimal_seconds_timestamp(timestamp) {
            candidates.push(timestamp);
        }
        if let Ok(timestamp) = timestamp.parse::<i64>() {
            candidates.extend(metric_binary_integer_timestamp_ns_candidates(timestamp));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if !candidates.is_empty() {
            return Some(candidates);
        }
    }
    None
}

fn metric_binary_integer_timestamp_ns_candidates(timestamp: i64) -> Vec<i64> {
    let mut candidates = vec![timestamp];
    if let Some(seconds_timestamp) = timestamp.checked_mul(1_000_000_000) {
        candidates.push(seconds_timestamp);
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn apply_metric_binary_arithmetic_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_arithmetic_to_sample_operands(left_sample, &original_left, right_sample, op)
}

fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(left_value, op, right_value, false) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_comparison_group_right_to_results(
            left_results,
            right_results,
            op,
            bool_modifier,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_comparison_to_series(
            &mut left_results[index],
            right_series,
            op,
            bool_modifier,
        ) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn apply_metric_binary_comparison_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
            bool_modifier,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

fn apply_metric_binary_comparison_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample(
                &mut left_values[index],
                right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample(left_sample, right_sample, op, bool_modifier)
}

fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample_operands(
        output_sample,
        left_sample,
        &right_sample,
        op,
        bool_modifier,
    )
}

fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        &original_left,
        right_sample,
        op,
        bool_modifier,
    )
}

fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(left_value, op, right_value, false);
    if bool_modifier {
        if let Some(value) = output_values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        if matches
            && let (Some(output), Some(left)) = (output_values.get_mut(1), left_values.get(1))
        {
            *output = left.clone();
        }
        matches
    }
}

fn apply_metric_binary_set_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricBinarySetOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        if matches!(op, MetricBinarySetOp::And) {
            left_results.clear();
        }
        return;
    };

    if matches!(op, MetricBinarySetOp::Or) {
        let left_label_sets = left_results
            .iter()
            .filter_map(metric_series_labels)
            .map(|labels| metric_vector_matching_key(&labels, matching))
            .collect::<BTreeSet<_>>();
        for right_series in right_results {
            let Some(right_labels) = metric_series_labels(right_series) else {
                continue;
            };
            let right_key = metric_vector_matching_key(&right_labels, matching);
            if !left_label_sets.contains(&right_key) {
                left_results.push(right_series.clone());
            }
        }
        sort_loki_metric_results_by_labels(left_results);
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let right_series = right_results.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == left_key)
        });
        let keep = match (op, right_series) {
            (MetricBinarySetOp::And | MetricBinarySetOp::Unless, Some(right_series)) => {
                apply_metric_binary_set_to_series(&mut left_results[index], right_series, op)
            }
            (MetricBinarySetOp::And, None) => false,
            (MetricBinarySetOp::Unless, None) | (MetricBinarySetOp::Or, _) => true,
        };
        if keep {
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn metric_series_labels(series: &Value) -> Option<Labels> {
    series.get("metric").and_then(json_object_to_labels)
}

fn sort_loki_metric_results_by_labels(results: &mut [Value]) {
    results.sort_by_key(metric_series_labels);
}

fn metric_vector_matching_key(labels: &Labels, matching: Option<&MetricVectorMatching>) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

fn metric_vector_group_modifier(
    matching: Option<&MetricVectorMatching>,
) -> Option<&MetricVectorGroupModifier> {
    match matching {
        Some(
            MetricVectorMatching::On { group, .. } | MetricVectorMatching::Ignoring { group, .. },
        ) => group.as_ref(),
        None => None,
    }
}

fn include_metric_group_labels(
    output_series: &mut Value,
    source_series: &Value,
    labels: &[String],
) {
    if labels.is_empty() {
        return;
    }
    let Some(source_metric) = source_series.get("metric").and_then(Value::as_object) else {
        return;
    };
    let Some(output_metric) = output_series
        .get_mut("metric")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for label in labels {
        output_metric.remove(label);
        if let Some(value) = source_metric.get(label).and_then(Value::as_str) {
            output_metric.insert(label.clone(), json!(value));
        }
    }
}

fn apply_metric_binary_set_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricBinarySetOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let right_values = right_series.get("values").and_then(Value::as_array);
        let mut index = 0;
        while index < left_values.len() {
            let matched = right_values
                .and_then(|right_values| {
                    matching_metric_binary_sample(&left_values[index], right_values)
                })
                .is_some();
            if metric_binary_set_keeps_sample(op, matched) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    let matched = right_series
        .get("value")
        .is_some_and(|right_sample| metric_samples_share_timestamp(left_sample, right_sample));
    metric_binary_set_keeps_sample(op, matched)
}

fn metric_binary_set_keeps_sample(op: MetricBinarySetOp, matched: bool) -> bool {
    match op {
        MetricBinarySetOp::And => matched,
        MetricBinarySetOp::Or => true,
        MetricBinarySetOp::Unless => !matched,
    }
}

fn metric_samples_share_timestamp(left_sample: &Value, right_sample: &Value) -> bool {
    metric_binary_sample_timestamps_match(left_sample, right_sample)
}

fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&arithmetic.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_arithmetic_to_series(
            &mut results[index],
            arithmetic.op,
            scalar,
            arithmetic.scalar_on_left,
        ) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_arithmetic_to_sample(
                &mut values[index],
                op,
                scalar,
                scalar_on_left,
            ) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_arithmetic_to_sample(sample, op, scalar, scalar_on_left)
}

fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(sample_value, op, scalar, scalar_on_left)
    else {
        return false;
    };
    if let Some(value) = values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

fn metric_scalar_arithmetic_value(
    sample: MetricValue,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> Option<MetricValue> {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}

fn apply_metric_scalar_comparison_to_loki_result(
    value: &mut Value,
    comparison: &MetricScalarComparison,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&comparison.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_comparison_to_series(&mut results[index], comparison, scalar) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

fn apply_metric_scalar_comparison_to_series(
    series: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_comparison_to_sample(&mut values[index], comparison, scalar) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_comparison_to_sample(sample, comparison, scalar)
}

fn apply_metric_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(
        sample_value,
        comparison.op,
        scalar,
        comparison.scalar_on_left,
    );
    if comparison.bool_modifier {
        if let Some(value) = values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        matches
    }
}

fn metric_scalar_comparison_matches(
    sample: MetricValue,
    op: ComparisonOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    let ordering = left.cmp_value(right);
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}

fn default_metric_range_step(time_range: TimeRange) -> i64 {
    time_range.end_ns.saturating_sub(time_range.start_ns).max(1)
}

async fn execute_http_metric_range_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    time_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        return execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            (time_range, step_ns),
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from);
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        return execute_metric_query_range_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            (time_range, step_ns),
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from);
    }
    execute_metric_query_range_with_deletes(
        &state.root,
        plan,
        query,
        &state.label_index,
        time_range,
        step_ns,
        delete_filters,
    )
    .await
    .map_err(HttpQueryError::from)
}

async fn execute_http_metric_instant_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    let response = if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from)?
    } else if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        execute_metric_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    } else {
        execute_metric_query_with_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    };

    Ok(loki_vector_response_from_matrix(response))
}

async fn execute_http_stream_query(
    state: &QuerierState,
    query: &str,
    tenant: &str,
    time_range: TimeRange,
    options: (LokiDirection, Option<usize>, Option<i64>, Option<i64>),
) -> Result<Value, HttpQueryError> {
    let (direction, limit, interval, end_exclusive) = options;
    validate_loki_interval(interval)?;
    let query = parse_query(query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            &plan,
            &state.label_index,
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters: &delete_filters,
            },
            StreamScanOptions::from_stream_options(direction, limit, interval, end_exclusive)
                .with_block_fetch_concurrency(state.cold_block_fetch_concurrency),
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response =
            apply_loki_stream_options(scan.value, direction, limit, interval, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_blocks_with_hot_tail(
            response,
            &scan.scanned_blocks,
            &plan,
            &records,
            &frontier,
        ));
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        let response = execute_stream_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            &plan,
            &state.label_index,
            &records,
            &frontier,
            &delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response =
            apply_loki_stream_options(response, direction, limit, interval, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_plan_with_hot_tail(
            response, &plan, &records, &frontier,
        ));
    }
    let response =
        execute_stream_query_with_deletes(&state.root, &plan, &state.label_index, &delete_filters)
            .await
            .map_err(HttpQueryError::from)?;
    let response = apply_loki_stream_options(response, direction, limit, interval, end_exclusive);
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
}

fn validate_loki_interval(interval: Option<i64>) -> Result<(), HttpQueryError> {
    if let Some(interval_ns) = interval
        && interval_ns < 0
    {
        return Err(HttpQueryError::InvalidInterval);
    }
    Ok(())
}

