pub(crate) fn format_labels(labels: &[String]) -> String {
    if labels.is_empty() {
        String::new()
    } else {
        format!("({})", labels.join(", "))
    }
}
