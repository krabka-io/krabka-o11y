use super::*;

pub(crate) fn format_metric_scalar_comparison_expression(query: &str) -> Option<String> {
    let comparison = parse_metric_scalar_comparison_query(query).ok()?;
    let metric = format_simple_metric_query(&comparison.query)?;
    let scalar = format_scalar_text(&comparison.scalar)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    let bool_modifier = if comparison.bool_modifier {
        " bool"
    } else {
        ""
    };
    Some(if comparison.scalar_on_left {
        format!("({scalar} {operator}{bool_modifier} {metric})")
    } else {
        format!("({metric} {operator}{bool_modifier} {scalar})")
    })
}
