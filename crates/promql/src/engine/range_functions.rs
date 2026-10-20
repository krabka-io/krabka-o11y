use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_units::prelude::*;
use num_traits::ToPrimitive;

use super::{
    RangeEval, RangeWindow, add_compatible_native_histogram,
    annotations::{
        emit_info, emit_warning, histogram_counter_reset_collision_warning,
        histogram_ignored_in_mixed_range_info, mismatched_custom_buckets_info,
        mixed_exponential_custom_warning, mixed_floats_histograms_warning,
        native_histogram_not_counter_warning, native_histogram_not_gauge_warning,
    },
    histogram::native_histogram_detect_reset,
    native_histograms_are_range_compatible, reconcile_native_histogram_layouts,
    result_utils::quantile_value,
    scale_native_histogram_values,
    selector::timestamp_seconds,
};
#[cfg(feature = "experimental-functions")]
use crate::error::{PromqlError, Result};
use crate::{
    PromqlLabels as Labels,
    planner::ExtendedSelectorModifier,
    result::{FloatSample, InstantSample, RangeSeries, SampleValue},
};
mod apply_outer_range_fn;
mod boundary_value;
mod compact_histogram_spans;
mod count_step_transitions;
mod extended_range_value;
mod extrapolation;
mod fold_over_time_extremum;
mod instant_delta_histogram;
mod instant_delta_sample_from_series;
mod native_histograms_equal;
mod outer_range_fn;
mod outer_range_sample_from_series;
mod over_time_fn;
mod over_time_histogram_sample;
mod over_time_mad;
mod over_time_mean;
mod over_time_sample_from_series;
mod over_time_sum;
mod predict_linear;
mod range_function_sample_from_series;
mod range_histogram_sample;
mod range_samples;
mod regression_slope_and_intercept;
pub(super) use apply_outer_range_fn::apply_outer_range_fn;
use boundary_value::boundary_value;
use compact_histogram_spans::compact_histogram_spans;
use count_step_transitions::count_step_transitions;
pub(super) use extended_range_value::{
    HistogramSample, extended_float_range_value, extended_histogram_range_value,
    interpolate_histogram,
};
use extrapolation::{
    HistogramExtrapolation, extrapolated_histogram_component, extrapolated_rate_with_starts,
};
use fold_over_time_extremum::fold_over_time_extremum;
use instant_delta_histogram::instant_delta_histogram;
use instant_delta_sample_from_series::instant_delta_sample_from_series;
pub(super) use native_histograms_equal::native_histograms_equal;
pub(super) use outer_range_fn::OuterRangeFn;
use outer_range_sample_from_series::{OuterRangeFold, outer_range_sample_from_series};
pub(super) use over_time_fn::OverTimeFn;
use over_time_histogram_sample::over_time_histogram_sample;
use over_time_mad::over_time_mad;
pub(crate) use over_time_mean::over_time_mean;
use over_time_sample_from_series::over_time_sample_from_series;
pub(super) use over_time_sum::over_time_sum;
use predict_linear::predict_linear;
use range_function_sample_from_series::range_function_sample_from_series;
use range_histogram_sample::range_histogram_sample;
pub(super) use range_samples::range_samples;
use regression_slope_and_intercept::regression_slope_and_intercept;

use crate::functions::{extrapolate::start_timestamp_reset, over_time::over_time_variance};

/// Which extremum `fold_over_time_extremum` tracks.
#[derive(Clone, Copy)]
pub(crate) enum ExtremumKind {
    Min,
    Max,
}

impl ExtremumKind {
    pub(crate) fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum IrateFn {
    Irate,
    Idelta,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeFn {
    Rate,
    Increase,
    Delta,
    Changes,
    Resets,
}

pub(crate) fn regression_slope(samples: &[FloatSample], range_end_ms: i64) -> Option<f64> {
    regression_slope_and_intercept(samples, range_end_ms).map(|fit| fit.slope)
}

pub(crate) fn align_subquery_start(start_ms: i64, step: Time) -> i64 {
    let step_ms = step.millis_i64();
    let remainder = start_ms.rem_euclid(step_ms);
    if remainder == 0 {
        start_ms
    } else {
        start_ms.saturating_add(step_ms - remainder)
    }
}

pub(crate) fn histogram_counts_reset(previous: &[f64], current: &[f64]) -> bool {
    let shared_len = previous.len().min(current.len());
    for index in 0..shared_len {
        if current[index] < previous[index] {
            return true;
        }
    }
    false
}

pub(crate) fn histogram_reset_between(
    previous: &NativeHistogram,
    current: &NativeHistogram,
) -> bool {
    current.count < previous.count
        || current.sum < previous.sum
        || current.zero_count < previous.zero_count
        || histogram_counts_reset(&previous.positive_counts, &current.positive_counts)
        || histogram_counts_reset(&previous.negative_counts, &current.negative_counts)
}

pub(crate) fn histogram_reset_indices(histograms: &[NativeHistogram]) -> Vec<usize> {
    let mut indices = Vec::new();
    for index in 0..histograms.len().saturating_sub(1) {
        if histogram_reset_between(&histograms[index], &histograms[index + 1]) {
            indices.push(index + 1);
        }
    }
    indices
}

pub(crate) fn range_has_samples(series: &RangeSeries, range_end_ms: i64, range: Time) -> bool {
    range_sample_count(series, range_end_ms, range) != 0
}

pub(crate) fn range_sample_count(series: &RangeSeries, range_end_ms: i64, range: Time) -> usize {
    range_samples(series, range_end_ms, range).count()
}

#[cfg(feature = "experimental-functions")]
#[cfg(feature = "experimental-functions")]
pub(crate) fn validate_smoothing_factor(name: &str, value: f64) -> Result<()> {
    if value <= 0.0 || value >= 1.0 {
        return Err(PromqlError::Plan(format!(
            "invalid {name}. Expected: 0 < factor < 1, got: {value}"
        )));
    }
    Ok(())
}

pub(crate) fn count_changes(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let changes = values
        .windows(2)
        .filter(|window| window[0].to_bits() != window[1].to_bits())
        .fold(0.0, |count, _| count + 1.0);
    Some(changes)
}

pub(crate) fn count_resets(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let resets = values
        .windows(2)
        .filter(|window| window[1] < window[0])
        .fold(0.0, |count, _| count + 1.0);
    Some(resets)
}

pub(crate) fn deriv_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    regression_slope(&samples, range_end_ms)
}

#[cfg(feature = "experimental-functions")]
#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing(
    samples: &[FloatSample],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let mut previous_smoothed = 0.0;
    let mut smoothed = samples[0].value;
    let mut trend = samples[1].value - samples[0].value;

    for (index, sample) in samples.iter().enumerate().skip(1) {
        if index != 1 {
            trend =
                trend_factor.mul_add(smoothed - previous_smoothed, (1.0 - trend_factor) * trend);
        }
        let scaled_value = smoothing_factor * sample.value;
        let smoothed_with_trend = (1.0 - smoothing_factor) * (smoothed + trend);
        previous_smoothed = smoothed;
        smoothed = scaled_value + smoothed_with_trend;
    }

    Some(smoothed)
}

#[cfg(feature = "experimental-functions")]
#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    double_exponential_smoothing(&samples, smoothing_factor, trend_factor)
}

pub(crate) fn extrapolated_histogram_counts(
    extrapolation: &HistogramExtrapolation<'_>,
    histograms: &[NativeHistogram],
    counts: impl Fn(&NativeHistogram) -> &[f64],
) -> Option<Vec<f64>> {
    let bucket_count = counts(histograms.first()?).len();
    let mut out = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        let values = histograms
            .iter()
            .map(|histogram| counts(histogram).get(index).copied())
            .collect::<Option<Vec<_>>>()?;
        out.push(extrapolated_histogram_component(extrapolation, &values)?);
    }
    Some(out)
}

pub(crate) fn float_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Vec<FloatSample> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    series
        .samples
        .iter()
        .filter_map(|sample| {
            if sample.timestamp_ms <= range_start_ms || sample.timestamp_ms > range_end_ms {
                return None;
            }
            let SampleValue::Float(value) = &sample.value else {
                return None;
            };
            Some(FloatSample {
                timestamp_ms: sample.timestamp_ms,
                value: *value,
            })
        })
        .collect()
}

pub(crate) fn histogram_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
) -> Vec<NativeHistogram> {
    range_samples(series, range_end_ms, range)
        .filter_map(|sample| match sample.value {
            SampleValue::Histogram(histogram) => Some(histogram.clone()),
            SampleValue::Float(_) => None,
        })
        .collect()
}

/// Folds the last two float samples of a window into `irate` or `idelta`.
///
/// `irate` reads a fall in value as a counter reset and takes the newer sample
/// whole, then divides by the interval between the two. `idelta` is meant for
/// gauges, so it reports the signed difference as it stands.
pub(crate) fn instant_delta(previous: f64, last: f64, interval_secs: f64, kind: IrateFn) -> f64 {
    let mut result = last - previous;
    if matches!(kind, IrateFn::Irate) {
        if result < 0.0 {
            result = last;
        }
        result /= interval_secs;
    }
    result
}

pub(crate) fn instant_smoothed_boundary_value(
    timestamps: &[i64],
    values: &[f64],
    target_ms: i64,
) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if target_ms < *timestamps.first()? {
        return None;
    }
    if target_ms == *timestamps.first()? {
        return values.first().copied();
    }
    if target_ms >= *timestamps.last()? {
        return values.last().copied();
    }
    boundary_value(timestamps, values, target_ms)
}

pub(crate) fn interpolate_boundary(
    left_ts: i64,
    left_value: f64,
    right_ts: i64,
    right_value: f64,
    target_ms: i64,
) -> Option<f64> {
    let interval = (right_ts - left_ts).to_f64()?;
    if interval <= 0.0 {
        return None;
    }
    let ratio = (target_ms - left_ts).to_f64()? / interval;
    Some(left_value + (right_value - left_value) * ratio)
}

/// Does one Kahan-compensated incremental sum step.
///
/// This function adds `increment` to the running `sum` and `comp`. It is a direct port of Prometheus' `kahanSumInc`
/// (`promql/engine.go`). The numerically stable mean and variance folds use it,
/// so the operator and interpreter agree bit-for-bit.
pub(crate) fn kahan_sum_inc(increment: f64, sum: &mut f64, comp: &mut f64) {
    let old_sum = *sum;
    let old_comp = *comp;
    let new_sum = old_sum + increment;
    // Recover the rounding error lost when `increment` is small relative to
    // `sum` (or vice versa), matching Prometheus' branch on magnitude.
    //
    // An infinite running sum drops the compensation instead. Without this the
    // very first infinite increment leaves `(inf - inf) + x`, which is NaN, and
    // the NaN rides `comp` all the way to the `sum + comp` at the end -- so
    // `avg_over_time` over a series holding a single +Inf returned NaN where
    // Prometheus returns +Inf. Matches the `IsInf(t, 0)` arm of `kahanSumInc`.
    let new_comp = if new_sum.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        old_comp + ((old_sum - new_sum) + increment)
    } else {
        old_comp + ((increment - new_sum) + old_sum)
    };
    *sum = new_sum;
    *comp = new_comp;
}

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

/// Folds one series' window into its `predict_linear` value.
///
/// The regression is taken over the window that `range_end_ms` closes, but the
/// prediction is anchored at `eval_ms`, the query's own evaluation time:
/// `funcPredictLinear` passes `enh.Ts` as the intercept time. An `@` modifier
/// therefore moves the window without moving what the prediction is relative
/// to, which is why `predict_linear(m[55m] @ 3000, 3600)` answers differently
/// at each evaluation time.
pub(crate) fn predict_linear_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    duration: Time,
    eval_ms: i64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let samples = float_range_samples(series, range_end_ms, range);
    predict_linear(&samples, eval_ms, duration)
}

pub(crate) fn quantile_over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    quantile: f64,
) -> Option<f64> {
    note_histograms_ignored_in_range(series, range_end_ms, range);
    let mut values = float_range_samples(series, range_end_ms, range)
        .into_iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    quantile_value(quantile, &mut values)
}
