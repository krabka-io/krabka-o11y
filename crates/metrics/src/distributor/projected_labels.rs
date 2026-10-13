use super::DecodedClockReading;

/// The label set for one projected series: the clock identity, the metric
/// name, and any state label the family adds.
pub(crate) fn projected_labels(
    reading: &DecodedClockReading,
    name: &str,
    extra: &[krabka_blockstore::LabelPair],
) -> Vec<krabka_blockstore::LabelPair> {
    let mut labels = vec![
        krabka_blockstore::LabelPair::new("__name__", name),
        krabka_blockstore::LabelPair::new("node", reading.node.clone()),
        krabka_blockstore::LabelPair::new("clock", reading.clock.clone()),
        krabka_blockstore::LabelPair::new("source", reading.source_kind.as_label()),
    ];
    labels.extend(extra.iter().cloned());
    labels
}
