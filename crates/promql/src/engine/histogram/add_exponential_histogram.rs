use super::{
    NativeHistogram, add_bucket_maps, reduced_counts_outside_zero, zero_count_at_threshold,
};

struct CombinedZeroCounts {
    left: f64,
    right: f64,
}

pub(crate) fn add_exponential_histogram(left: &mut NativeHistogram, right: &NativeHistogram) {
    let mut threshold = left.zero_threshold.max(right.zero_threshold);
    let zero_counts = loop {
        let left_count = zero_count_at_threshold(left, threshold);
        let right_count = zero_count_at_threshold(right, threshold);
        let reconciled = left_count.threshold.max(right_count.threshold);
        if reconciled.to_bits() == threshold.to_bits() {
            break CombinedZeroCounts {
                left: left_count.count,
                right: right_count.count,
            };
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
    left.zero_count = zero_counts.left + zero_counts.right;
    let positive = add_bucket_maps(left_positive, right_positive);
    left.positive_spans = positive.spans;
    left.positive_counts = positive.counts;
    let negative = add_bucket_maps(left_negative, right_negative);
    left.negative_spans = negative.spans;
    left.negative_counts = negative.counts;
    left.custom_values = None;
}
