use super::{
    ExtremumKind, OverTimeFn, RangeSeries, SampleValue, Time, float_range_samples,
    fold_over_time_extremum, histogram_range_samples, over_time_histogram_sample, over_time_mad,
    over_time_mean, over_time_variance, range_sample_count, range_samples, timestamp_seconds,
};

pub(crate) fn over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: OverTimeFn,
) -> Option<SampleValue> {
    if matches!(
        kind,
        OverTimeFn::Count | OverTimeFn::First | OverTimeFn::Last | OverTimeFn::Present
    ) {
        let sample_count = range_sample_count(series, range_end_ms, range);
        if sample_count == 0 {
            return None;
        }
        return match kind {
            OverTimeFn::Count => Some(SampleValue::Float((0..sample_count).map(|_| 1.0).sum())),
            OverTimeFn::First => range_samples(series, range_end_ms, range)
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Last => range_samples(series, range_end_ms, range)
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Present => Some(SampleValue::Float(1.0)),
            _ => unreachable!("over_time histogram-safe kind checked above"),
        };
    }

    if matches!(kind, OverTimeFn::Sum | OverTimeFn::Avg) {
        let histograms = histogram_range_samples(series, range_end_ms, range);
        if !histograms.is_empty() {
            return over_time_histogram_sample(&histograms, kind).map(SampleValue::Histogram);
        }
    }

    let samples = float_range_samples(series, range_end_ms, range);
    if samples.is_empty() {
        return None;
    }

    let value = match kind {
        OverTimeFn::Sum => samples.iter().map(|(_, value)| value).sum(),
        OverTimeFn::Avg => over_time_mean(samples.iter().map(|(_, value)| *value)),
        OverTimeFn::Count => unreachable!("count_over_time handled before float extraction"),
        OverTimeFn::Min => fold_over_time_extremum(&samples, ExtremumKind::Min),
        OverTimeFn::Max => fold_over_time_extremum(&samples, ExtremumKind::Max),
        OverTimeFn::Stddev => over_time_variance(&samples).sqrt(),
        OverTimeFn::Stdvar => over_time_variance(&samples),
        OverTimeFn::Mad => over_time_mad(&samples).expect("non-empty samples"),
        OverTimeFn::First => samples
            .into_iter()
            .min_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::Last => samples
            .into_iter()
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::TsOfFirst => timestamp_seconds(
            samples
                .into_iter()
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfLast => timestamp_seconds(
            samples
                .into_iter()
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMin => timestamp_seconds(
            samples
                .into_iter()
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| right.0.cmp(&left.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMax => timestamp_seconds(
            samples
                .into_iter()
                .max_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.0.cmp(&right.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::Present => unreachable!("present_over_time handled before float extraction"),
    };
    Some(SampleValue::Float(value))
}
