//! Select-series result types and step-bucketing helpers.

use krabka_units::{Time, convert::TimeExt as _, millis};

use crate::ProfileError;

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

mod decimal_i64_to_f64;
mod decimal_usize_to_f64;
mod fold_bucket;
mod series_agg;
mod series_bucket_ms;
mod series_type;
mod step_bucket_ms;
mod step_from_secs;
mod validated_step;

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

use decimal_i64_to_f64::decimal_i64_to_f64;
use decimal_usize_to_f64::decimal_usize_to_f64;
pub use fold_bucket::fold_bucket;
pub use series_agg::SeriesAgg;
pub use series_bucket_ms::series_bucket_ms;
pub use series_type::Series;
pub use step_bucket_ms::step_bucket_ms;
pub use step_from_secs::step_from_secs;
pub(crate) use validated_step::validated_step;
