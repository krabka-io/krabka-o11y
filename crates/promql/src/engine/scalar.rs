#[cfg(feature = "experimental-functions")]
use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use time::OffsetDateTime;

#[cfg(feature = "experimental-functions")]
use super::{QUERY_RANGE_CONTEXT, planned::PlannedInstant};
use super::{histogram::scaled_native_histogram, labels::labels_without_metric_name};
#[cfg(test)]
use crate::planner::label_ops::SortOrder;
use crate::{
    PromqlError,
    error::Result,
    result::{QueryResult, SampleValue},
};

// === split-modules: generated submodules ===
mod calendar_fn;
mod calendar_fn_from_function_name;
mod clamp_float;
mod clamp_kind;
mod days_in_month;
mod duration_helper;
mod is_leap_year;
mod negate_query_result;
mod round_to_nearest;
mod scalar_call_to_planned;
mod scalar_extrema_fn;
mod sort_direction;
mod sort_order;
mod unary_float_fn;

pub(super) use calendar_fn::CalendarFn;
pub(super) use calendar_fn_from_function_name::calendar_fn_from_function_name;
#[cfg(test)]
pub(super) use clamp_float::clamp_float;
#[cfg(test)]
pub(super) use clamp_kind::ClampKind;
use days_in_month::days_in_month;
#[cfg(feature = "experimental-functions")]
pub(super) use duration_helper::DurationHelper;
use is_leap_year::is_leap_year;
pub(super) use negate_query_result::negate_query_result;
#[cfg(test)]
pub(super) use round_to_nearest::round_to_nearest;
#[cfg(feature = "experimental-functions")]
pub(super) use scalar_call_to_planned::scalar_call_to_planned;
#[cfg(feature = "experimental-functions")]
pub(super) use scalar_extrema_fn::ScalarExtremaFn;
#[cfg(test)]
pub(super) use sort_direction::SortDirection;
#[cfg(test)]
pub(super) use unary_float_fn::UnaryFloatFn;
