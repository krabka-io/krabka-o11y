use super::{
    format_metric_scalar_arithmetic_operator, format_scalar_text, format_simple_metric_query,
    parse_metric_scalar_arithmetic_query,
};

pub(crate) fn format_metric_scalar_arithmetic_expression(query: &str) -> Option<String> {
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
