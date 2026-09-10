use super::{
    RangeSeries, Time, emit_info, float_range_samples, histogram_ignored_in_mixed_range_info,
    histogram_range_samples,
};

/// Records the info Prometheus attaches when a float-only range fold meets a
/// window that also holds histogram samples.
///
/// `min_over_time`, `max_over_time`, `ts_of_min_over_time`,
/// `ts_of_max_over_time`, `mad_over_time`, the variance family,
/// `quantile_over_time`, `deriv`, `predict_linear`, and
/// `double_exponential_smoothing` all ignore the histogram samples and say so.
/// A window with no float sample at all yields no value and no annotation.
pub(crate) fn note_histograms_ignored_in_range(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) {
    if float_range_samples(series, range_end_ms, range).is_empty()
        || histogram_range_samples(series, range_end_ms, range).is_empty()
    {
        return;
    }
    emit_info(histogram_ignored_in_mixed_range_info(
        series.labels.get("__name__").unwrap_or(""),
    ));
}
