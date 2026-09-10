use std::cmp::Ordering;

use super::{
    NativeHistogram, RangeFn, RangeSeries, SampleValue, histogram_reset_between,
    native_histograms_equal,
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
    let mut window = series
        .samples
        .iter()
        .filter(|(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
        .map(|(_, value)| value);
    let mut previous = window.next()?;
    let mut count = 0.0_f64;
    for current in window {
        if step_counts(previous, current, kind) {
            count += 1.0;
        }
        previous = current;
    }
    Some(count)
}

fn step_counts(previous: &SampleValue, current: &SampleValue, kind: RangeFn) -> bool {
    match (previous, current) {
        (SampleValue::Float(previous), SampleValue::Float(current)) => {
            float_step_counts(*previous, *current, kind)
        }
        (SampleValue::Histogram(previous), SampleValue::Histogram(current)) => {
            histogram_step_counts(previous, current, kind)
        }
        // The sample type changed, which is both a change and a reset.
        _ => true,
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
