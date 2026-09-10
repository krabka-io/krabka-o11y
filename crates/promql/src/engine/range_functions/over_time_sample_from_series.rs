use super::{
    ExtremumKind, OverTimeFn, RangeSeries, SampleValue, Time, emit_warning, float_range_samples,
    fold_over_time_extremum, histogram_range_samples, mixed_floats_histograms_warning,
    note_histograms_ignored_in_range, over_time_histogram_sample, over_time_mad, over_time_mean,
    over_time_sum, over_time_variance, range_sample_count, range_samples, timestamp_seconds,
};

pub(crate) fn over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: OverTimeFn,
) -> Option<SampleValue> {
    // `ts_of_first_over_time` and `ts_of_last_over_time` time the earliest and
    // latest sample of ANY type, so a window of nothing but histograms still
    // has an answer.
    if matches!(
        kind,
        OverTimeFn::Count
            | OverTimeFn::First
            | OverTimeFn::Last
            | OverTimeFn::Present
            | OverTimeFn::TsOfFirst
            | OverTimeFn::TsOfLast
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
            OverTimeFn::TsOfFirst => range_samples(series, range_end_ms, range)
                .map(|(timestamp, _)| timestamp)
                .min()
                .map(|timestamp| SampleValue::Float(timestamp_seconds(timestamp))),
            OverTimeFn::TsOfLast => range_samples(series, range_end_ms, range)
                .map(|(timestamp, _)| timestamp)
                .max()
                .map(|timestamp| SampleValue::Float(timestamp_seconds(timestamp))),
            _ => unreachable!("over_time histogram-safe kind checked above"),
        };
    }

    if matches!(kind, OverTimeFn::Sum | OverTimeFn::Avg) {
        let histograms = histogram_range_samples(series, range_end_ms, range);
        if !histograms.is_empty() {
            // A window that mixes the two types has neither a float sum nor a
            // histogram sum: Prometheus warns and drops the series.
            if !float_range_samples(series, range_end_ms, range).is_empty() {
                emit_warning(mixed_floats_histograms_warning(
                    series.labels.get("__name__").unwrap_or(""),
                ));
                return None;
            }
            return over_time_histogram_sample(&histograms, kind).map(SampleValue::Histogram);
        }
    }

    if matches!(
        kind,
        OverTimeFn::Min
            | OverTimeFn::Max
            | OverTimeFn::Stddev
            | OverTimeFn::Stdvar
            | OverTimeFn::Mad
            | OverTimeFn::TsOfMin
            | OverTimeFn::TsOfMax
    ) {
        note_histograms_ignored_in_range(series, range_end_ms, range);
    }

    let samples = float_range_samples(series, range_end_ms, range);
    if samples.is_empty() {
        return None;
    }

    let value = match kind {
        OverTimeFn::Sum => over_time_sum(samples.iter().map(|(_, value)| *value)),
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
        OverTimeFn::TsOfFirst | OverTimeFn::TsOfLast => {
            unreachable!("ts_of_first/ts_of_last handled before float extraction")
        }
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
