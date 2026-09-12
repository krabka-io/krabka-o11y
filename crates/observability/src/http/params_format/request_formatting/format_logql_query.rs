use super::{
    HttpQueryError, LogqlExpr, format_metric_query, format_stream_query,
    label_join_format_query_error, logql_expression_contains_label_join, parse_logql_expr,
    scalar_vector_plain_parse_error,
};

pub(crate) fn format_logql_query(query: &str) -> Result<String, HttpQueryError> {
    if query.trim() == "{" {
        return Err(HttpQueryError::LokiFormatPlainParse(
            "parse error at line 1: expected label name".to_string(),
        ));
    }
    if let Some(error) = scalar_vector_plain_parse_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }
    if let Some(error) = label_join_format_query_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }
    let expression = parse_logql_expr(query).map_err(|source| HttpQueryError::LokiFormatParse {
        query: query.to_string(),
        source,
    })?;
    if logql_expression_contains_label_join(&expression) {
        return Err(HttpQueryError::LokiFormatPlainParse(
            "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string(),
        ));
    }
    Ok(match &expression {
        LogqlExpr::Stream { query, .. } => format_stream_query(query),
        LogqlExpr::Metric { query, .. } => {
            format_metric_query(query).unwrap_or_else(|| expression.to_string())
        }
        LogqlExpr::Vector(inner) => eval_scalar(inner)
            .map(|value| format!("vector({value:.6})"))
            .unwrap_or_else(|| expression.to_string()),
        LogqlExpr::Comparison { left, right, .. }
            if direct_range_metric(left) && eval_scalar(right).is_some() =>
        {
            format!("({expression})")
        }
        _ if eval_scalar(&expression).is_some() => eval_scalar(&expression)
            .expect("guard established a scalar")
            .to_string(),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. }
            if metric_expression(left) && metric_expression(right) =>
        {
            format!("({expression})")
        }
        _ => expression.to_string(),
    })
}

fn direct_range_metric(expression: &LogqlExpr) -> bool {
    matches!(expression, LogqlExpr::Metric { query, .. } if query.vector_aggregation.is_none())
}

fn metric_expression(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::Metric { .. } => true,
        LogqlExpr::Vector(inner)
        | LogqlExpr::Sort { expr: inner, .. }
        | LogqlExpr::Selection { expr: inner, .. }
        | LogqlExpr::LabelReplace { expr: inner, .. }
        | LogqlExpr::LabelJoin { expr: inner, .. } => metric_expression(inner),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => metric_expression(left) || metric_expression(right),
        LogqlExpr::Stream { .. } | LogqlExpr::Scalar(_) => false,
    }
}

fn eval_scalar(expression: &LogqlExpr) -> Option<f64> {
    use krabka_logql::MetricScalarArithmeticOp;
    match expression {
        LogqlExpr::Scalar(value) => value.parse().ok(),
        LogqlExpr::Arithmetic {
            left,
            op,
            matching: None,
            right,
        } => {
            let left = eval_scalar(left)?;
            let right = eval_scalar(right)?;
            Some(match op {
                MetricScalarArithmeticOp::Add => left + right,
                MetricScalarArithmeticOp::Subtract => left - right,
                MetricScalarArithmeticOp::Multiply => left * right,
                MetricScalarArithmeticOp::Divide => left / right,
                MetricScalarArithmeticOp::Modulo => left % right,
                MetricScalarArithmeticOp::Power => left.powf(right),
            })
            .filter(|value| value.is_finite())
        }
        _ => None,
    }
}
