use super::*;

pub(crate) fn native_histogram_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    let mut buckets = Vec::new();
    if hist.is_nhcb() {
        let custom_values = hist.custom_values.as_deref().unwrap_or_default();
        append_native_spanned_buckets(
            &mut buckets,
            &hist.positive_spans,
            &hist.positive_counts,
            |index| NativeQuantileBucket {
                lower: custom_histogram_bound(index - 1, custom_values),
                upper: custom_histogram_bound(index, custom_values),
                count: 0.0,
            },
        );
        return buckets;
    }

    append_native_spanned_buckets(
        &mut buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| NativeQuantileBucket {
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(NativeQuantileBucket {
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_native_spanned_buckets(
        &mut buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| NativeQuantileBucket {
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
    buckets
}
