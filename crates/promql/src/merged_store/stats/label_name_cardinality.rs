use super::{BTreeMap, BTreeSet, LabelNameCardinality, SeriesFingerprint};

pub(crate) fn label_name_cardinality(
    by_name: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelNameCardinality> {
    let mut out = by_name
        .into_iter()
        .map(|(name, fingerprints)| LabelNameCardinality {
            name,
            series_count: fingerprints.len(),
        })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}
