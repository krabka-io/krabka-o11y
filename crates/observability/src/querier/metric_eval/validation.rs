use crate::querier::metric_eval::for_each_loki_result_metric;

use crate::{HttpQueryError, ParseError, Regex, Value, json};
use krabka_logql::MetricLabelJoin;
use crate::{ScalarVectorExpressionResult, scalar_vector_expression_result};

mod vector_scalar_expression_parser;

pub(crate) use vector_scalar_expression_parser::VectorScalarExpressionParser;

pub(crate) fn reject_signed_vector_function_literal(query: &str) -> Result<(), HttpQueryError> {
    scalar_vector_plain_parse_error(query)
        .map(HttpQueryError::LokiPlainParse)
        .map_or(Ok(()), Err)
}

pub(crate) fn scalar_vector_plain_parse_error(query: &str) -> Option<String> {
    signed_vector_function_literal_error(query)
        .or_else(|| unspaced_vector_set_operator_error(query))
}

pub(crate) fn scalar_vector_query_is_vector(query: &str) -> bool {
    matches!(
        scalar_vector_expression_result(query),
        Some(ScalarVectorExpressionResult::Vector { .. })
    )
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
            "vector" | "label_replace" | "label_join" | "sort" | "sort_desc"
        );
    }
    false
}

pub(crate) fn signed_vector_function_literal_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    first_unquoted_match(query, |index, _| {
        if !query[index..].starts_with("vector(") {
            return None;
        }
        let mut sign_index = index + "vector(".len();
        while let Some(next) = query[sign_index..].chars().next() {
            if !next.is_whitespace() {
                break;
            }
            sign_index += next.len_utf8();
        }
        let sign @ ('+' | '-') = query[sign_index..].chars().next()? else {
            return None;
        };
        let column = query[..sign_index].chars().count() + 1;
        Some(format!(
            "parse error at line 1, col {column}: syntax error: unexpected {sign}, expecting NUMBER"
        ))
    })
}

pub(crate) fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    first_unquoted_match(query, |index, ch| {
        if ch != ')' {
            return None;
        }
        let next_index = index + ch.len_utf8();
        if !["and", "or", "unless"]
            .iter()
            .any(|operator| query[next_index..].starts_with(operator))
        {
            return None;
        }
        let column = query[..next_index].chars().count() + 1;
        Some(format!(
            "parse error at line 1, col {column}: syntax error: unexpected IDENTIFIER"
        ))
    })
}

#[cfg(test)]

#[cfg(test)]
pub(crate) fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
    apply_label_join_fields(
        value,
        &label_join.destination_label,
        &label_join.separator,
        &label_join.source_labels,
    );
}

pub(crate) fn apply_label_join_fields(
    value: &mut Value,
    destination_label: &str,
    separator: &str,
    source_labels: &[String],
) {
    for_each_loki_result_metric(value, |metric| {
        let joined = source_labels
            .iter()
            .map(|label| metric.get(label).and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(separator);
        metric.insert(destination_label.to_string(), json!(joined));
    });
}

/// The arguments of `label_replace` after its vector operand.
#[derive(Clone, Copy)]
pub(crate) struct LabelReplaceArguments<'a> {
    pub(crate) destination_label: &'a str,
    pub(crate) replacement: &'a str,
    pub(crate) source_label: &'a str,
    pub(crate) pattern: &'a str,
}

/// Applies `label_replace` to every series of `value`. `query` is the full
/// query text that an invalid `pattern` is reported against.
pub(crate) fn apply_label_replace_to_loki_result(
    value: &mut Value,
    arguments: LabelReplaceArguments<'_>,
    query: &str,
) -> Result<(), HttpQueryError> {
    let LabelReplaceArguments {
        destination_label,
        replacement,
        source_label,
        pattern,
    } = arguments;
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

/// The first `Some` that `matcher` returns for a character of `query` outside
/// every double-quoted string, given the character's byte index.
pub(crate) fn first_unquoted_match<T>(
    query: &str,
    mut matcher: impl FnMut(usize, char) -> Option<T>,
) -> Option<T> {
    let mut in_string = false;
    let mut escaped = false;
    for character in query.char_indices() {
        let index = character.0;
        let ch = character.1;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            continue;
        }
        if let Some(found) = matcher(index, ch) {
            return Some(found);
        }
    }
    None
}
