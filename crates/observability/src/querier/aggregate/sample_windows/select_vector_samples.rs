use super::*;

pub(crate) fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<(Labels, MetricValue)>>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push((labels.clone(), range_sample_value(value, query)));
        }
    }

    let mut selected = BTreeMap::new();
    for (_grouped_labels, values) in groups {
        for (time, mut candidates) in values {
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.1.cmp_value(left.1),
                    VectorSelection::Smallest => left.1.cmp_value(right.1),
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            for (labels, value) in candidates.into_iter().take(limit) {
                selected
                    .entry(labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, value);
            }
        }
    }

    selected
}
