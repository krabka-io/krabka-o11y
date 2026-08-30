use super::*;

/// The label set for one projected series: the clock identity, the metric
/// name, and any state label the family adds.
pub(crate) fn projected_labels(
    reading: &DecodedClockReading,
    name: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut labels = vec![
        ("__name__".to_string(), name.to_string()),
        ("node".to_string(), reading.node.clone()),
        ("clock".to_string(), reading.clock.clone()),
        (
            "source".to_string(),
            reading.source_kind.as_label().to_string(),
        ),
    ];
    labels.extend(
        extra
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
    );
    labels
}
