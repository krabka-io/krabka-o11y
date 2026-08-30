use super::*;

pub(crate) fn format_metric_binary_expression(
    left: &str,
    operator: &str,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    right: &str,
) -> String {
    let bool_text = if bool_modifier { " bool" } else { "" };
    let Some(matching) = matching else {
        return format!("({left} {operator}{bool_text} {right})");
    };
    let matching = format_metric_vector_matching(matching);
    if matching.has_group {
        return format!(
            "  {left}\n{operator}{bool_text} {}\n  {right}",
            matching.text
        );
    }
    format!("({left} {operator}{bool_text} {}  {right})", matching.text)
}
