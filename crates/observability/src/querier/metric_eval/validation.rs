#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn scalar_vector_query_is_vector(query: &str) -> bool {
    matches!(
        scalar_vector_expression_result(query),
        Some(ScalarVectorExpressionResult::Vector { .. })
    )
}

pub(crate) fn reject_signed_vector_function_literal(query: &str) -> Result<(), HttpQueryError> {
    scalar_vector_plain_parse_error(query)
        .map(HttpQueryError::LokiPlainParse)
        .map_or(Ok(()), Err)
}

pub(crate) fn scalar_vector_plain_parse_error(query: &str) -> Option<String> {
    signed_vector_function_literal_error(query)
        .or_else(|| unspaced_vector_set_operator_error(query))
}

pub(crate) fn signed_vector_function_literal_error(query: &str) -> Option<String> {
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

pub(crate) fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
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

pub(crate) fn could_be_scalar_vector_expression(query: &str) -> bool {
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

pub(crate) fn apply_label_replace_to_loki_result(
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

pub(crate) fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
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

pub(crate) struct VectorScalarExpressionParser<'a> {
    pub(crate) input: &'a str,
    pub(crate) position: usize,
    pub(crate) vector_terms: usize,
}
