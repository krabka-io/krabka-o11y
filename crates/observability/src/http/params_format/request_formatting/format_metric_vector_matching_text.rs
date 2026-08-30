use super::*;

pub(crate) fn format_metric_vector_matching_text(
    modifier: &str,
    labels: &[String],
    group: Option<&MetricVectorGroupModifier>,
) -> String {
    let mut text = format!("{modifier} ({})", labels.join(","));
    if let Some(group) = group {
        text.push(' ');
        text.push_str(&format_metric_vector_group_modifier(group));
    }
    text
}
