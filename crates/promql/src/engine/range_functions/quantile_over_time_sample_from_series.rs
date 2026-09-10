use super::{
    RangeSeries, Time, float_range_samples, note_histograms_ignored_in_range, quantile_value,
};

pub(crate) fn quantile_over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    quantile: f64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let mut values = float_range_samples(series, range_end_ms, range)
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    quantile_value(quantile, &mut values)
}
