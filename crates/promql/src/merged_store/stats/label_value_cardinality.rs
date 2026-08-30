use super::*;

pub(crate) fn label_value_cardinality(
    by_value: BTreeMap<(String, String), BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelValueCardinality> {
    let mut out = by_value
        .into_iter()
        .map(
            |((label_name, label_value), fingerprints)| LabelValueCardinality {
                label_name,
                label_value,
                series_count: fingerprints.len(),
            },
        )
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.label_name.cmp(&right.label_name))
            .then_with(|| left.label_value.cmp(&right.label_value))
    });
    out
}
