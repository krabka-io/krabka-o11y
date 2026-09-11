use super::Labels;

/// Renders a series' labels as `name="value"`, comma separated, for an error
/// that has to say which series it is talking about.
///
/// A fingerprint alone does not identify a series to anyone reading a log:
/// it is a hash, and the writer that produced the series is the one who needs
/// to recognise it. The label set is what they wrote.
pub(crate) fn render_series_labels(labels: &Labels) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}
