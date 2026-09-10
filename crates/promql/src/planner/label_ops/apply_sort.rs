use super::{InstantSample, SampleValue, SortOrder, labels_key, sort_value};

/// Sorts an already-assembled instant vector by sample value in `order`.
///
/// Histogram samples are dropped, not sorted: `funcSort` and `funcSortDesc`
/// pass the vector through `filterFloats` first, so a histogram series never
/// reaches the output. Ties break by canonical label key.
#[must_use]
pub fn apply_sort(samples: Vec<InstantSample>, order: SortOrder) -> Vec<InstantSample> {
    let mut samples: Vec<InstantSample> = samples
        .into_iter()
        .filter(|sample| matches!(sample.value, SampleValue::Float(_)))
        .collect();
    samples.sort_by(|left, right| {
        order
            .compare(sort_value(left), sort_value(right))
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}
