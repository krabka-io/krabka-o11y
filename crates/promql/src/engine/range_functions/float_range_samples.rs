use super::{FloatSample, RangeSeries, SampleValue, Time, TimeExt};

pub(crate) fn float_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Vec<FloatSample> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        .filter_map(|sample| {
            if sample.timestamp_ms <= range_start_ms || sample.timestamp_ms > range_end_ms {
                return None;
            }
            let SampleValue::Float(value) = &sample.value else {
                return None;
            };
            Some(FloatSample {
                timestamp_ms: sample.timestamp_ms,
                value: *value,
            })
        })
        .collect()
}
