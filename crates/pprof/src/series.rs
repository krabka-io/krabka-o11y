//! Select-series result types and step-bucketing helpers.

use krabka_units::{Time, convert::TimeExt as _, millis};

use crate::ProfileError;

/// Select-series aggregation mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesAgg {
    Sum,
    Average,
}

/// One time series returned by `select_series`.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub labels: Vec<ProfileLabel>,
    pub points: Vec<SamplePoint>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::secs;

    use super::*;

    fn takes_copy(_agg: SeriesAgg) {}

    #[test]
    fn series_holds_points_and_agg_is_copy() {
        let series = Series {
            labels: vec![ProfileLabel::new("service_name", "checkout")],
            points: vec![
                SamplePoint {
                    timestamp_ms: 1000,
                    value: 1.5,
                },
                SamplePoint {
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
        };
        assert!(
            series.points[1]
                == SamplePoint {
                    timestamp_ms: 2000,
                    value: 2.0
                }
        );
        let agg = SeriesAgg::Sum;
        takes_copy(agg);
        takes_copy(agg);
    }

    #[test]
    fn step_from_secs_reads_fractional_seconds_and_rejects_nonpositive() {
        assert!(step_from_secs(15.0).unwrap() == secs(15));
        assert!(step_from_secs(0.5).unwrap() == millis(500));
        let zero = step_from_secs(0.0).unwrap_err();
        assert!(matches!(zero, ProfileError::Plan(message) if message.contains("positive finite")));
        assert!(step_from_secs(-1.0).is_err());
        let infinity = step_from_secs(f64::INFINITY).unwrap_err();
        assert!(
            matches!(infinity, ProfileError::Plan(message) if message.contains("positive finite"))
        );
    }

    #[yare::parameterized(
        below_one_millisecond = { 0.0001, None },
        half_millisecond = { 0.0005, None },
        fractional_millisecond = { 0.000_999_9, None },
        exactly_one_millisecond = { 0.001, Some(millis(1)) },
    )]
    fn step_secs_rejects_sub_millisecond_values(step_secs: f64, want: Option<Time>) {
        assert2::assert!(step_from_secs(step_secs).ok() == want, "{step_secs}");
    }

    #[test]
    fn step_secs_rejects_steps_beyond_i64_milliseconds() {
        let too_large = step_from_secs(1e18).unwrap_err();
        assert!(matches!(too_large, ProfileError::Plan(message) if message.contains("too large")));
        assert!(step_from_secs(1e15).is_ok());
    }

    #[yare::parameterized(
        later_in_step = { 17_000, 15_000 },
        exact_step = { 15_000, 15_000 },
        previous_step = { 14_999, 0 },
        negative_step = { -1, -15_000 },
    )]
    fn bucket_start_is_step_floored(timestamp_ms: i64, want: i64) {
        assert2::assert!(
            step_bucket_ms(timestamp_ms, secs(15)) == want,
            "{timestamp_ms}"
        );
    }

    #[test]
    fn fold_sum_vs_average() {
        assert!((fold_bucket(SeriesAgg::Sum, &[2, 3, 5]) - 10.0).abs() < f64::EPSILON);
        assert!((fold_bucket(SeriesAgg::Average, &[2, 3, 5]) - 10.0 / 3.0).abs() < 1e-12);
    }
}
mod series_bucket_ms;

/// A profile label name and its value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileLabel {
    pub name: String,
    pub value: String,
}

impl ProfileLabel {
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// A numeric value at one timestamp in a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SamplePoint {
    pub timestamp_ms: i64,
    pub value: f64,
}
pub use series_bucket_ms::series_bucket_ms;

pub(crate) fn decimal_i64_to_f64(value: i64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("i64 decimal representation parses as f64")
}

pub(crate) fn decimal_usize_to_f64(value: usize) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("usize decimal representation parses as f64")
}

#[must_use]
pub fn fold_bucket(agg: SeriesAgg, values: &[i64]) -> f64 {
    let sum: i64 = values.iter().sum();
    match agg {
        SeriesAgg::Sum => decimal_i64_to_f64(sum),
        SeriesAgg::Average => decimal_i64_to_f64(sum) / decimal_usize_to_f64(values.len()),
    }
}

/// Returns the epoch-millisecond start of the bucket that holds `ts_ms`.
///
/// The bucket size is the step of the query. The timestamp is an instant, and
/// the bucket start is an instant too, so both stay epoch milliseconds. Only
/// the step is an extent. This function floors with Euclidean division, so a
/// timestamp before the epoch goes into the bucket below it. Euclidean division
/// does not truncate toward zero.
#[must_use]
pub fn step_bucket_ms(ts_ms: i64, step: Time) -> i64 {
    let step_ms = step.millis_i64();
    ts_ms.div_euclid(step_ms) * step_ms
}

/// Returns the step of a select-series query.
///
/// The Pyroscope `step` query parameter carries the step as fractional seconds.
///
/// # Errors
/// Returns [`ProfileError::Plan`] when the step is not a positive finite number
/// of seconds, is shorter than a millisecond, or is too long to express as whole
/// milliseconds in an `i64`.
pub fn step_from_secs(step_secs: f64) -> Result<Time, ProfileError> {
    validated_step(Time::from_secs_f64(step_secs))
}

/// Checks the same bounds as [`step_from_secs`] on an already-typed step.
///
/// A query then cannot reach the bucket arithmetic with a step of zero.
pub(crate) fn validated_step(step: Time) -> Result<Time, ProfileError> {
    let step_secs = step.secs_f64();
    if !(step_secs.is_finite() && step_secs > 0.0) {
        return Err(ProfileError::Plan(format!(
            "step must be a positive finite number of seconds, got {step_secs}"
        )));
    }
    if step < millis(1) {
        return Err(ProfileError::Plan("step must be >= 1ms".to_string()));
    }
    // `millis_i64` saturates, so a step beyond `i64::MAX` milliseconds would
    // silently bucket at the saturated value rather than fail.
    if step >= Time::from_millis(i64::MAX) {
        return Err(ProfileError::Plan(format!(
            "step is too large: {step_secs}"
        )));
    }
    Ok(step)
}
