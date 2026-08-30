use super::{HistogramBucketJson, NativeHistogram, append_spanned_buckets, BOUNDARY_OPEN_RIGHT, standard_histogram_bound, BOUNDARY_CLOSED_BOTH, BOUNDARY_OPEN_LEFT};

pub(crate) fn append_standard_histogram_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    hist: &NativeHistogram,
) {
    append_spanned_buckets(
        buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_RIGHT,
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(HistogramBucketJson {
            boundary_rule: BOUNDARY_CLOSED_BOTH,
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
}
