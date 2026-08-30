use krabka_blockstore::Labels;
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_units::prelude::*;
use num_traits::ToPrimitive;

use super::{
    RangeEval, add_compatible_native_histogram, labels::labels_without_metric_name,
    native_histograms_are_range_compatible, result_utils::quantile_value,
    scale_native_histogram_values, selector::timestamp_seconds,
};
#[cfg(feature = "experimental-functions")]
use crate::error::{PromqlError, Result};
use crate::{
    planner::ExtendedSelectorModifier,
    result::{InstantSample, RangeSeries, SampleValue},
};

mod align_subquery_start;
mod anchored_float_range_value;
mod apply_outer_range_fn;
mod boundary_value;
mod compact_histogram_spans;
mod count_changes;
mod count_histogram_resets;
mod count_resets;
mod counter_corrected_values;
mod counter_delta;
mod deriv_sample_from_series;
mod double_exponential_smoothing;
mod double_exponential_smoothing_sample_from_series;
mod extrapolate_histogram_delta;
mod extrapolated_histogram_component;
mod extrapolated_histogram_counts;
mod extrapolated_rate;
mod extremum_kind;
mod float_range_samples;
mod fold_over_time_extremum;
mod histogram_counts_reset;
mod histogram_extrapolation;
mod histogram_range_samples;
mod histogram_reset_between;
mod histogram_reset_indices;
mod instant_delta;
mod instant_delta_sample_from_series;
mod instant_smoothed_boundary_value;
mod interpolate_boundary;
mod irate_fn;
mod kahan_sum_inc;
mod outer_range_fn;
mod outer_range_sample_from_series;
mod over_time_fn;
mod over_time_histogram_sample;
mod over_time_mad;
mod over_time_mean;
mod over_time_sample_from_series;
mod over_time_variance;
mod predict_linear;
mod predict_linear_sample_from_series;
mod quantile_over_time_sample_from_series;
mod range_fn;
mod range_function_sample_from_series;
mod range_has_samples;
mod range_histogram_sample;
mod range_sample_count;
mod range_samples;
mod regression_slope;
mod regression_slope_and_intercept;
mod smoothed_float_range_value;
mod validate_smoothing_factor;

pub(super) use align_subquery_start::align_subquery_start;
use anchored_float_range_value::anchored_float_range_value;
pub(super) use apply_outer_range_fn::apply_outer_range_fn;
use boundary_value::boundary_value;
use compact_histogram_spans::compact_histogram_spans;
use count_changes::count_changes;
use count_histogram_resets::count_histogram_resets;
use count_resets::count_resets;
use counter_corrected_values::counter_corrected_values;
use counter_delta::counter_delta;
use deriv_sample_from_series::deriv_sample_from_series;
#[cfg(feature = "experimental-functions")]
use double_exponential_smoothing::double_exponential_smoothing;
#[cfg(feature = "experimental-functions")]
use double_exponential_smoothing_sample_from_series::double_exponential_smoothing_sample_from_series;
use extrapolate_histogram_delta::extrapolate_histogram_delta;
use extrapolated_histogram_component::extrapolated_histogram_component;
use extrapolated_histogram_counts::extrapolated_histogram_counts;
use extrapolated_rate::extrapolated_rate;
use extremum_kind::ExtremumKind;
use float_range_samples::float_range_samples;
use fold_over_time_extremum::fold_over_time_extremum;
use histogram_counts_reset::histogram_counts_reset;
use histogram_extrapolation::HistogramExtrapolation;
use histogram_range_samples::histogram_range_samples;
use histogram_reset_between::histogram_reset_between;
use histogram_reset_indices::histogram_reset_indices;
use instant_delta::instant_delta;
use instant_delta_sample_from_series::instant_delta_sample_from_series;
pub(super) use instant_smoothed_boundary_value::instant_smoothed_boundary_value;
use interpolate_boundary::interpolate_boundary;
pub(super) use irate_fn::IrateFn;
pub(super) use kahan_sum_inc::kahan_sum_inc;
pub(super) use outer_range_fn::OuterRangeFn;
use outer_range_sample_from_series::outer_range_sample_from_series;
pub(super) use over_time_fn::OverTimeFn;
use over_time_histogram_sample::over_time_histogram_sample;
use over_time_mad::over_time_mad;
use over_time_mean::over_time_mean;
use over_time_sample_from_series::over_time_sample_from_series;
use over_time_variance::over_time_variance;
use predict_linear::predict_linear;
use predict_linear_sample_from_series::predict_linear_sample_from_series;
use quantile_over_time_sample_from_series::quantile_over_time_sample_from_series;
pub(super) use range_fn::RangeFn;
use range_function_sample_from_series::range_function_sample_from_series;
pub(super) use range_has_samples::range_has_samples;
use range_histogram_sample::range_histogram_sample;
use range_sample_count::range_sample_count;
use range_samples::range_samples;
use regression_slope::regression_slope;
use regression_slope_and_intercept::regression_slope_and_intercept;
use smoothed_float_range_value::smoothed_float_range_value;
#[cfg(feature = "experimental-functions")]
pub(super) use validate_smoothing_factor::validate_smoothing_factor;
