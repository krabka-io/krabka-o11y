use super::{Histogram, WireTimeSeries, bucket_spans_to_proto};
use krabka_metrics::wire::pb::v1::histogram::{Count, ResetHint, ZeroCount};

pub(crate) fn histograms_to_proto(row: &WireTimeSeries) -> Vec<Histogram> {
    row.native_histogram
        .iter()
        .map(|histogram| Histogram {
            count: Some(Count::CountFloat(histogram.count)),
            sum: histogram.sum,
            schema: i32::from(histogram.schema),
            zero_threshold: histogram.zero_threshold,
            zero_count: Some(ZeroCount::ZeroCountFloat(histogram.zero_count)),
            positive_spans: bucket_spans_to_proto(&histogram.positive_spans),
            positive_counts: histogram.positive_counts.clone(),
            reset_hint: ResetHint::No as i32,
            timestamp: row.timestamp_ms,
            ..Default::default()
        })
        .collect()
}
