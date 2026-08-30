use super::{RangeSeries, Time, IrateFn, TimeExt, SampleValue, instant_delta};

pub(crate) fn instant_delta_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: IrateFn,
) -> Option<f64> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (timestamp, value) in &series.samples {
        if *timestamp <= range_start_ms || *timestamp > range_end_ms {
            continue;
        }
        let SampleValue::Float(value) = value else {
            return None;
        };
        timestamps.push(*timestamp);
        values.push(*value);
    }
    instant_delta(&timestamps, &values, kind)
}
