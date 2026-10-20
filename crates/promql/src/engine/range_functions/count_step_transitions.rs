use std::cmp::Ordering;

use super::{
    NativeHistogram, RangeFn, RangeSeries, SampleValue, histogram_reset_between,
    native_histograms_equal, start_timestamp_reset,
};

/// Counts the `changes` or `resets` steps across one series' window.
///
/// `funcChanges` and `funcResets` walk the float and the histogram samples of a
/// window as ONE ordered sequence, so a window that mixes the two types is
/// counted rather than refused. A step from a float to a histogram, or back,
/// counts for both functions: the sample type changed, and the counter it
/// belongs to must have restarted.
///
/// Returns `None` for an empty window, where Prometheus drops the series.
pub(crate) fn count_step_transitions(
    series: &RangeSeries,
    range_start_ms: i64,
    range_end_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    let mut window = series.samples.iter().filter(|sample| {
        sample.timestamp_ms > range_start_ms && sample.timestamp_ms <= range_end_ms
    });
    let first = window.next()?;
    let mut previous_time = first.timestamp_ms;
    let mut previous = &first.value;
    let mut count = 0.0_f64;
    for current_sample in window {
        let current_time = current_sample.timestamp_ms;
        let current = &current_sample.value;
        let previous_start = series
            .start_timestamps_ms
            .get(&previous_time)
            .copied()
            .or(match previous {
                SampleValue::Histogram(h) => h.start_timestamp_ms,
                SampleValue::Float(_) => None,
            })
            .unwrap_or(0);
        let start = series
            .start_timestamps_ms
            .get(&current_time)
            .copied()
            .or(match current {
                SampleValue::Histogram(h) => h.start_timestamp_ms,
                SampleValue::Float(_) => None,
            })
            .unwrap_or(0);
        if step_counts(previous, current, kind)
            || matches!(kind, RangeFn::Resets)
                && start_timestamp_reset(previous_start, previous_time, start, current_time)
        {
            count += 1.0;
        }
        previous = current;
        previous_time = current_time;
    }
    Some(count)
}

fn step_counts(previous: &SampleValue, current: &SampleValue, kind: RangeFn) -> bool {
    match previous {
        SampleValue::Float(previous_float) => match current {
            SampleValue::Float(current_float) => {
                float_step_counts(*previous_float, *current_float, kind)
            }
            SampleValue::Histogram(_) => true,
        },
        SampleValue::Histogram(previous_histogram) => match current {
            SampleValue::Float(_) => true,
            SampleValue::Histogram(current_histogram) => {
                histogram_step_counts(previous_histogram, current_histogram, kind)
            }
        },
    }
}

fn float_step_counts(previous: f64, current: f64, kind: RangeFn) -> bool {
    if matches!(kind, RangeFn::Resets) {
        return current < previous;
    }
    // `partial_cmp` asks the same question as `!=` without spelling a float
    // comparison, and it answers "not equal" for a NaN, which the second test
    // then takes back: two NaNs in a row are not a change.
    current.partial_cmp(&previous) != Some(Ordering::Equal)
        && !(current.is_nan() && previous.is_nan())
}

fn histogram_step_counts(
    previous: &NativeHistogram,
    current: &NativeHistogram,
    kind: RangeFn,
) -> bool {
    if matches!(kind, RangeFn::Resets) {
        return histogram_reset_between(previous, current);
    }
    !native_histograms_equal(previous, current)
}
