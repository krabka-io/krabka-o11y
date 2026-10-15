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

mod calendar_fn;
mod negate_query_result;
mod unary_float_fn;

pub(super) use calendar_fn::CalendarFn;
pub(super) use negate_query_result::negate_query_result;
#[cfg(test)]
pub(super) use unary_float_fn::UnaryFloatFn;

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum ClampKind {
    Both,
    Min,
    Max,
}

#[cfg(test)]
impl ClampKind {
    pub(crate) fn argument_count(self) -> usize {
        match self {
            Self::Both => 3,
            Self::Min | Self::Max => 2,
        }
    }
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(super) enum DurationHelper {
    Range,
    Step,
    Start,
    End,
}

#[cfg(feature = "experimental-functions")]
impl DurationHelper {
    pub(crate) fn value_ms(self) -> i64 {
        QUERY_RANGE_CONTEXT
            .try_with(|context| match self {
                Self::Range => context.end_ms.saturating_sub(context.start_ms),
                Self::Step => context.step.millis_i64(),
                Self::Start => context.start_ms,
                Self::End => context.end_ms,
            })
            .unwrap_or(0)
    }
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(super) enum ScalarExtremaFn {
    Max,
    Min,
}

#[cfg(feature = "experimental-functions")]
impl ScalarExtremaFn {
    pub(crate) fn apply(self, left: f64, right: f64) -> f64 {
        if left.is_nan() || right.is_nan() {
            return f64::NAN;
        }
        match self {
            Self::Max => left.max(right),
            Self::Min => left.min(right),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum SortDirection {
    Ascending,
    Descending,
}

#[cfg(test)]
pub(super) use crate::functions::scalar_math::clamp_float;

pub(crate) fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Rounds `value` to the nearest multiple of `to_nearest`, ties going up.
///
/// Prometheus INVERTS the step and multiplies, rather than dividing by it:
/// `math.Floor(f*toNearestInverse+0.5) / toNearestInverse`. The two spellings
/// disagree in the last bits for a step such as `0.1`, whose inverse is exact
/// while the step itself is not, so the inversion is part of the answer.
#[cfg(test)]
pub(crate) fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    let inverse = 1.0 / to_nearest;
    (value * inverse + 0.5).floor() / inverse
}

#[cfg(test)]
#[cfg(test)]
impl From<SortDirection> for SortOrder {
    fn from(direction: SortDirection) -> Self {
        match direction {
            SortDirection::Ascending => Self::Ascending,
            SortDirection::Descending => Self::Descending,
        }
    }
}

/// Maps a `PromQL` calendar-function name to its `CalendarFn` variant.
///
/// The mapping mirrors the calendar arms of `PromqlEngine::eval_instant_call`.
/// This function returns `None` for any other function, so the planner dispatch
/// falls through.
pub(crate) fn calendar_fn_from_function_name(name: &str) -> Option<CalendarFn> {
    Some(match name {
        "year" => CalendarFn::Year,
        "month" => CalendarFn::Month,
        "day_of_month" => CalendarFn::DayOfMonth,
        "day_of_week" => CalendarFn::DayOfWeek,
        "day_of_year" => CalendarFn::DayOfYear,
        "days_in_month" => CalendarFn::DaysInMonth,
        "hour" => CalendarFn::Hour,
        "minute" => CalendarFn::Minute,
        _ => return None,
    })
}

#[cfg(feature = "experimental-functions")]
/// Wraps a scalar `QueryResult` from a delegated interpreter call.
///
/// The result becomes a `PlannedInstant::PrecomputedScalar`. A non-scalar result
/// is impossible for these callers. This function still maps such a result to a
/// canonical error instead of a panic.
#[cfg(feature = "experimental-functions")]
pub(crate) fn scalar_call_to_planned(result: &QueryResult) -> Result<PlannedInstant> {
    match *result {
        QueryResult::Scalar { ts_ms, value } => {
            Ok(PlannedInstant::PrecomputedScalar { ts_ms, value })
        }
        _ => Err(PromqlError::Plan(
            "expected a scalar result from an experimental scalar call".to_string(),
        )),
    }
}
