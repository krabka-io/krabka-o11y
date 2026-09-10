use super::{
    IrateFn, RangeSeries, SampleValue, Time, TimeExt, ToPrimitive, emit_warning, instant_delta,
    instant_delta_histogram, mixed_floats_histograms_warning, scale_native_histogram_values,
};

/// Folds one series' window into its `irate` or `idelta` value.
///
/// `instantValue` works on the last TWO samples of the window whatever their
/// type, so a window is refused only when those two disagree on type. A float
/// pair takes the arithmetic difference; a histogram pair is subtracted as a
/// histogram; a mixed pair warns and drops the series.
pub(crate) fn instant_delta_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: IrateFn,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    let window = series
        .samples
        .iter()
        .filter(|(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
        .collect::<Vec<_>>();
    let [(previous_ms, previous), (last_ms, last)] = window.get(window.len().checked_sub(2)?..)?
    else {
        return None;
    };
    let interval_secs = (last_ms - previous_ms).to_f64()? / 1000.0;
    if interval_secs <= 0.0 {
        return None;
    }

    match (previous, last) {
        (SampleValue::Float(previous), SampleValue::Float(last)) => Some(SampleValue::Float(
            instant_delta(*previous, *last, interval_secs, kind),
        )),
        (SampleValue::Histogram(previous), SampleValue::Histogram(last)) => {
            let metric = series.labels.get("__name__").unwrap_or("");
            let mut out = instant_delta_histogram(previous, last, metric, kind)?;
            if matches!(kind, IrateFn::Irate) {
                scale_native_histogram_values(&mut out, 1.0 / interval_secs);
            }
            Some(SampleValue::Histogram(out))
        }
        _ => {
            emit_warning(mixed_floats_histograms_warning(
                series.labels.get("__name__").unwrap_or(""),
            ));
            None
        }
    }
}
