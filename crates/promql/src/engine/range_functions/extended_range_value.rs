use super::{
    NativeHistogram, RangeFn, ResetHint, Time, TimeExt, ToPrimitive,
    add_compatible_native_histogram, emit_info, emit_warning, mismatched_custom_buckets_info,
    mixed_exponential_custom_warning, native_histogram_detect_reset,
    native_histogram_not_counter_warning, native_histogram_not_gauge_warning,
    scale_native_histogram_values,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExtendedIndices {
    pub first: usize,
    pub last: usize,
}

pub(crate) struct HistogramSample {
    pub timestamp_ms: i64,
    pub histogram: NativeHistogram,
}

pub(crate) fn extended_indices(
    timestamps: &[i64],
    start: i64,
    end: i64,
    smoothed: bool,
) -> Option<ExtendedIndices> {
    let mut last = timestamps.len().checked_sub(1)?;
    let first = timestamps[..last]
        .partition_point(|timestamp| *timestamp <= start)
        .saturating_sub(1);
    if smoothed {
        last = timestamps[..last].partition_point(|timestamp| *timestamp < end);
    }
    if timestamps[last] <= start || smoothed && timestamps[first] > end {
        return None;
    }
    Some(ExtendedIndices { first, last })
}

pub(crate) fn extended_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    start: i64,
    end: i64,
    range: Time,
    kind: RangeFn,
    smoothed: bool,
) -> Option<f64> {
    let ExtendedIndices { first, last } = extended_indices(timestamps, start, end, smoothed)?;
    let counter = matches!(kind, RangeFn::Rate | RangeFn::Increase);
    let interpolate = |left: usize, right: usize, target: i64| -> Option<f64> {
        let y1 = if counter && values[right] < values[left] {
            0.0
        } else {
            values[left]
        };
        Some(
            y1 + (values[right] - y1) * (target - timestamps[left]).to_f64()?
                / (timestamps[right] - timestamps[left]).to_f64()?,
        )
    };
    let left = if smoothed && timestamps[first] < start {
        interpolate(first, first + 1, start)?
    } else {
        values[first]
    };
    let right = if smoothed && last > 0 && timestamps[last] > end {
        interpolate(last - 1, last, end)?
    } else {
        values[last]
    };
    let mut result = right - left;
    if counter {
        let mut previous = left;
        let begin = first + usize::from(timestamps[first] <= start);
        let finish = last + usize::from(timestamps[last] < end);
        if begin < finish {
            for &value in &values[begin..finish] {
                if value < previous {
                    result += previous;
                }
                previous = value;
            }
        }
        if right < previous {
            result += previous;
        }
    }
    if matches!(kind, RangeFn::Rate) {
        result /= range.secs_f64();
    }
    Some(result)
}

fn combine(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
    factor: f64,
    metric: &str,
) -> Option<()> {
    if left.is_nhcb() != right.is_nhcb() {
        emit_warning(mixed_exponential_custom_warning(metric));
        return None;
    }
    if left.is_nhcb() && left.custom_values != right.custom_values {
        emit_info(mismatched_custom_buckets_info(if factor < 0.0 {
            "subtraction"
        } else {
            "addition"
        }));
    }
    let mut right = right.clone();
    scale_native_histogram_values(&mut right, factor);
    add_compatible_native_histogram(left, &right).ok()
}

pub(crate) fn interpolate_histogram(
    left: &HistogramSample,
    right: &HistogramSample,
    target: i64,
    counter: bool,
    metric: &str,
) -> Option<NativeHistogram> {
    if target == left.timestamp_ms {
        return Some(left.histogram.clone());
    }
    if target == right.timestamp_ms {
        return Some(right.histogram.clone());
    }
    if left.histogram.is_nhcb() != right.histogram.is_nhcb() {
        emit_warning(mixed_exponential_custom_warning(metric));
        return None;
    }
    let fraction = (target - left.timestamp_ms).to_f64()?
        / (right.timestamp_ms - left.timestamp_ms).to_f64()?;
    let mut result = right.histogram.clone();
    if !counter || !native_histogram_detect_reset(&left.histogram, &right.histogram) {
        combine(&mut result, &left.histogram, -1.0, metric)?;
        scale_native_histogram_values(&mut result, fraction);
        combine(&mut result, &left.histogram, 1.0, metric)?;
    } else {
        scale_native_histogram_values(&mut result, fraction);
    }
    Some(result)
}

pub(crate) fn extended_histogram_range_value(
    points: &[HistogramSample],
    start: i64,
    end: i64,
    range: Time,
    kind: RangeFn,
    smoothed: bool,
    metric: &str,
) -> Option<NativeHistogram> {
    let timestamps = points
        .iter()
        .map(|point| point.timestamp_ms)
        .collect::<Vec<_>>();
    let ExtendedIndices { first, last } = extended_indices(&timestamps, start, end, smoothed)?;
    let counter = matches!(kind, RangeFn::Rate | RangeFn::Increase);
    let custom = points[first].histogram.is_nhcb();
    for point in &points[first..=last] {
        let histogram = &point.histogram;
        if histogram.is_nhcb() != custom {
            emit_warning(mixed_exponential_custom_warning(metric));
            return None;
        }
        if counter && histogram.reset_hint == ResetHint::Gauge {
            emit_warning(native_histogram_not_counter_warning(metric));
        }
    }
    let left = if smoothed && timestamps[first] < start {
        interpolate_histogram(&points[first], &points[first + 1], start, counter, metric)?
    } else {
        points[first].histogram.clone()
    };
    let right = if smoothed && last > 0 && timestamps[last] > end {
        interpolate_histogram(&points[last - 1], &points[last], end, counter, metric)?
    } else {
        points[last].histogram.clone()
    };
    if !counter && (left.reset_hint != ResetHint::Gauge || right.reset_hint != ResetHint::Gauge) {
        emit_warning(native_histogram_not_gauge_warning(metric));
    }
    let mut result = right.clone();
    combine(&mut result, &left, -1.0, metric)?;
    if counter {
        let mut previous = &left;
        let mut begin = first + 1;
        if smoothed
            && timestamps[first] < start
            && native_histogram_detect_reset(&points[first].histogram, &points[first + 1].histogram)
        {
            previous = &points[first + 1].histogram;
            begin += 1;
        }
        if begin <= last {
            for point in &points[begin..last] {
                let histogram = &point.histogram;
                if native_histogram_detect_reset(previous, histogram) {
                    combine(&mut result, previous, 1.0, metric)?;
                }
                previous = histogram;
            }
            if native_histogram_detect_reset(previous, &right) {
                combine(&mut result, previous, 1.0, metric)?;
            }
        }
    }
    if matches!(kind, RangeFn::Rate) {
        super::super::histogram::divide_native_histogram_values(&mut result, range.secs_f64());
    }
    result.reset_hint = ResetHint::Gauge;
    result.start_timestamp_ms = None;
    let positive = super::compact_histogram_spans(&result.positive_spans, &result.positive_counts);
    result.positive_spans = positive.spans;
    result.positive_counts = positive.counts;
    let negative = super::compact_histogram_spans(&result.negative_spans, &result.negative_counts);
    result.negative_spans = negative.spans;
    result.negative_counts = negative.counts;
    Some(result)
}
