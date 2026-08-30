use super::{WireTimeSeries, Histogram, bucket_spans_to_proto, ResetHint};

pub(crate) fn histograms_to_proto(row: &WireTimeSeries) -> Vec<Histogram> {
    row.native_histogram
        .iter()
        .map(|histogram| Histogram {
            count_float: histogram.count,
            sum: histogram.sum,
            schema: i32::from(histogram.schema),
            zero_threshold: histogram.zero_threshold,
            zero_count_float: histogram.zero_count,
            positive_spans: bucket_spans_to_proto(&histogram.positive_spans),
            positive_counts: histogram.positive_counts.clone(),
            reset_hint: ResetHint::No as i32,
            timestamp: row.timestamp_ms,
        })
        .collect()
}
