use super::{NativeHistogram, add_bucket_maps, reduced_counts_outside_zero, zero_count_at_threshold};

pub(crate) fn add_exponential_histogram(left: &mut NativeHistogram, right: &NativeHistogram) {
    let mut threshold = left.zero_threshold.max(right.zero_threshold);
    let (left_zero_count, right_zero_count) = loop {
        let (left_count, left_threshold) = zero_count_at_threshold(left, threshold);
        let (right_count, right_threshold) = zero_count_at_threshold(right, threshold);
        let reconciled = left_threshold.max(right_threshold);
        if reconciled.to_bits() == threshold.to_bits() {
            break (left_count, right_count);
        }
        threshold = reconciled;
    };

    let target_schema = left.schema.min(right.schema);
    let left_positive = reduced_counts_outside_zero(
        left,
        &left.positive_spans,
        &left.positive_counts,
        threshold,
        target_schema,
    );
    let right_positive = reduced_counts_outside_zero(
        right,
        &right.positive_spans,
        &right.positive_counts,
        threshold,
        target_schema,
    );
    let left_negative = reduced_counts_outside_zero(
        left,
        &left.negative_spans,
        &left.negative_counts,
        threshold,
        target_schema,
    );
    let right_negative = reduced_counts_outside_zero(
        right,
        &right.negative_spans,
        &right.negative_counts,
        threshold,
        target_schema,
    );

    left.schema = target_schema;
    left.zero_threshold = threshold;
    left.zero_count = left_zero_count + right_zero_count;
    (left.positive_spans, left.positive_counts) = add_bucket_maps(left_positive, right_positive);
    (left.negative_spans, left.negative_counts) = add_bucket_maps(left_negative, right_negative);
    left.custom_values = None;
}
