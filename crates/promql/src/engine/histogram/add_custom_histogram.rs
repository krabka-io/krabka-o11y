use super::{NativeHistogram, add_bucket_maps, remap_custom_counts};

pub(crate) fn add_custom_histogram(left: &mut NativeHistogram, right: &NativeHistogram) {
    let left_values = left.custom_values.as_deref().unwrap_or_default();
    let right_values = right.custom_values.as_deref().unwrap_or_default();
    let target_values = left_values
        .iter()
        .copied()
        .filter(|value| right_values.contains(value))
        .collect::<Vec<_>>();
    let left_counts = remap_custom_counts(
        &left.positive_spans,
        &left.positive_counts,
        left_values,
        &target_values,
    );
    let right_counts = remap_custom_counts(
        &right.positive_spans,
        &right.positive_counts,
        right_values,
        &target_values,
    );
    (left.positive_spans, left.positive_counts) = add_bucket_maps(left_counts, right_counts);
    left.custom_values = Some(target_values);
    left.zero_threshold = 0.0;
    left.zero_count = 0.0;
    left.negative_spans.clear();
    left.negative_counts.clear();
}
