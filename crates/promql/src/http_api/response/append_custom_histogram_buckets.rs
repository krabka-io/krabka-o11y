use super::{
    BOUNDARY_OPEN_LEFT, HistogramBucketJson, NativeHistogram, append_spanned_buckets,
    custom_histogram_bound,
};

pub(crate) fn append_custom_histogram_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    hist: &NativeHistogram,
) {
    let custom_values = hist.custom_values.as_deref().unwrap_or_default();
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: custom_histogram_bound(index - 1, custom_values),
            upper: custom_histogram_bound(index, custom_values),
            count: 0.0,
        },
    );
}
