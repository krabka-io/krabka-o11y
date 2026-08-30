use super::{RangeSeries, Time, float_range_samples, quantile_value};

pub(crate) fn quantile_over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    quantile: f64,
) -> Option<f64> {
    let mut values = float_range_samples(series, range_end_ms, range)
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    quantile_value(quantile, &mut values)
}
