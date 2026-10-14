/// Formats label pairs as a Prometheus series selector for error messages.
pub fn format_labels(labels: &[super::MetricLabelPair<super::MetricString>]) -> String {
    let pairs = labels
        .iter()
        .map(|pair| {
            let quoted = pair
                .value
                .utf8()
                .map_or_else(|| pair.value.quoted(), |value| format!("{value:?}"));
            format!("{}={quoted}", pair.name)
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", pairs.join(", "))
}
