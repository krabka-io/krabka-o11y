use super::{NativeHistogram, RangeSeries, SampleValue, Time, range_samples};

pub(crate) fn histogram_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Vec<NativeHistogram> {
    range_samples(series, range_end_ms, range)
        .filter_map(|sample| match sample.value {
            SampleValue::Histogram(histogram) => Some(histogram.clone()),
            SampleValue::Float(_) => None,
        })
        .collect()
}
