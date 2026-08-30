pub(crate) fn format_metric_vector_group_modifier_text(
    modifier: &str,
    labels: &[String],
) -> String {
    if labels.is_empty() {
        modifier.to_string()
    } else {
        format!("{modifier} ({})", labels.join(","))
    }
}
