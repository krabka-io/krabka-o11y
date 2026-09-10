use super::Labels;

/// Returns `labels` without `drop`, keeping `__name__`.
///
/// This is `Labels.BytesWithoutLabels(le)`, the signature `resetHistograms`
/// groups classic buckets by.
pub(crate) fn labels_without_label(labels: &Labels, drop: &str) -> Labels {
    labels
        .iter()
        .filter(|(name, _)| name.as_str() != drop)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
