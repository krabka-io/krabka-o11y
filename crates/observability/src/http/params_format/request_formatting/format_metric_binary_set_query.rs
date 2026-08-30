use super::{
    format_metric_binary_expression, format_metric_binary_set_operator, format_metric_query,
    parse_metric_binary_set_query, parse_metric_query, split_leading_vector_binary_modifiers,
    split_top_level_set_query,
};

pub(crate) fn format_metric_binary_set_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_set_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let set = parse_metric_binary_set_query(query).ok()?;
    let left = format_metric_query(&set.left)?;
    let right = format_metric_query(&set.right)?;
    let operator = format_metric_binary_set_operator(set.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        set.matching.as_ref(),
        &right,
    ))
}
