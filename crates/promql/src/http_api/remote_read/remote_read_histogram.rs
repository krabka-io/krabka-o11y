use super::*;

pub(crate) fn remote_read_histogram(timestamp: i64, hist: &NativeHistogram) -> pb::v1::Histogram {
    pb::v1::Histogram {
        count: Some(remote_read_histogram_count(hist)),
        sum: hist.sum,
        schema: i32::from(hist.schema),
        zero_threshold: hist.zero_threshold,
        zero_count: Some(remote_read_histogram_zero_count(hist)),
        negative_spans: remote_read_bucket_spans(&hist.negative_spans),
        negative_deltas: remote_read_histogram_deltas(hist.is_float, &hist.negative_counts),
        negative_counts: if hist.is_float {
            hist.negative_counts.clone()
        } else {
            Vec::new()
        },
        positive_spans: remote_read_bucket_spans(&hist.positive_spans),
        positive_deltas: remote_read_histogram_deltas(hist.is_float, &hist.positive_counts),
        positive_counts: if hist.is_float {
            hist.positive_counts.clone()
        } else {
            Vec::new()
        },
        reset_hint: remote_read_reset_hint(hist.reset_hint),
        timestamp,
        custom_values: hist.custom_values.clone().unwrap_or_default(),
    }
}
