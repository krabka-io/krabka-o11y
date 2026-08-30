use super::{RangeSeries, Time, TimeExt, SampleValue};

pub(crate) fn float_range_samples(series: &RangeSeries, range_end_ms: i64, range: Time) -> Vec<(i64, f64)> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        .filter_map(|(timestamp, value)| {
            if *timestamp <= range_start_ms || *timestamp > range_end_ms {
                return None;
            }
            let SampleValue::Float(value) = value else {
                return None;
            };
            Some((*timestamp, *value))
        })
        .collect()
}
