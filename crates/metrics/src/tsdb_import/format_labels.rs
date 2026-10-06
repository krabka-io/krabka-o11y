/// Formats label pairs as a Prometheus series selector for error messages.
pub fn format_labels(labels: &[(String, super::MetricString)]) -> String {
    let pairs = labels
        .iter()
        .map(|(name, value)| {
            let quoted = value
                .utf8()
                .map_or_else(|| value.quoted(), |value| format!("{value:?}"));
            format!("{name}={quoted}")
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", pairs.join(", "))
}
