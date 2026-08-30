use super::*;

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
                // The legacy formatters above preserve Loki's established
                // canonical spelling for the expression shapes they support.
                // The central recursive AST is the typed fallback for nested
                // expressions those shallow parsers cannot represent.
                Ok(expression.to_string())
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
