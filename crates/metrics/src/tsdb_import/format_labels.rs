/// Formats label pairs as a Prometheus series selector for error messages.
pub fn format_labels(labels: &[(String, String)]) -> String {
    let pairs = labels
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>();
    format!("{{{}}}", pairs.join(", "))
}
