use super::{
    HttpQueryError, format_label_replace_metric_binary_arithmetic,
    format_label_replace_metric_binary_comparison, format_label_replace_metric_binary_set,
    format_label_replace_metric_scalar_expression, format_label_replace_metric_vector_expression,
    format_metric_binary_arithmetic_query, format_metric_binary_comparison_query,
    format_metric_binary_set_query, format_metric_label_replace_query, format_metric_query,
    format_metric_scalar_arithmetic_expression, format_metric_scalar_comparison_expression,
    format_metric_vector_arithmetic_expression, format_metric_vector_comparison_expression,
    format_metric_vector_set_expression, format_scalar_vector_expression,
    format_sort_vector_expression, format_stream_query, label_join_format_query_error,
    logql_expression_contains_label_join, parse_logql_expr, parse_metric_binary_arithmetic_query,
    parse_metric_binary_comparison_query, parse_metric_binary_set_query,
    parse_metric_label_join_query, parse_metric_label_replace_query, parse_metric_query,
    parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query, parse_query,
    scalar_vector_expression_result, scalar_vector_plain_parse_error,
};

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
