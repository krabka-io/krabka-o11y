use super::{
    BTreeMap, Labels, MetricQuery, MetricSamples, MetricValue, VectorGrouping, VectorSelection,
    range_sample_value, vector_group_labels,
};

struct VectorSampleCandidate {
    labels: Labels,
    value: MetricValue,
}

pub(crate) fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<VectorSampleCandidate>>> = BTreeMap::new();

    for sample_entry in samples {
        let labels = sample_entry.0;
        let values = sample_entry.1;
        let grouped_labels = vector_group_labels(&labels, grouping);
        for value_entry in values {
            let time = value_entry.0;
            let value = value_entry.1;
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push(VectorSampleCandidate {
                    labels: labels.clone(),
                    value: range_sample_value(&value, query),
                });
        }
    }

    let mut selected = BTreeMap::new();
    for group_entry in groups {
        for candidate_entry in group_entry.1 {
            let time = candidate_entry.0;
            let mut candidates = candidate_entry.1;
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.value.cmp_value(left.value),
                    VectorSelection::Smallest => left.value.cmp_value(right.value),
                };
                value_order.then_with(|| left.labels.cmp(&right.labels))
            });
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            for candidate in candidates.into_iter().take(limit) {
                selected
                    .entry(candidate.labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, candidate.value);
            }
        }
    }

    selected
}
