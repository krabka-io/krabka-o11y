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
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(1000, 1.5), (2000, 2.0)],
        };
        assert!(series.points[1] == (2000, 2.0));
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

    #[test]
    fn step_secs_rejects_sub_millisecond_values() {
        for (step_secs, want) in [
            (0.0001, None),
            (0.0005, None),
            (0.000_999_9, None),
            (0.001, Some(millis(1))),
        ] {
            assert!(step_from_secs(step_secs).ok() == want, "{step_secs}");
        }
    }

    #[test]
    fn step_secs_rejects_steps_beyond_i64_milliseconds() {
        let too_large = step_from_secs(1e18).unwrap_err();
        assert!(matches!(too_large, ProfileError::Plan(message) if message.contains("too large")));
        assert!(step_from_secs(1e15).is_ok());
    }

    #[test]
    fn bucket_start_is_step_floored() {
        for (timestamp_ms, want) in [
            (17_000, 15_000),
            (15_000, 15_000),
            (14_999, 0),
            (-1, -15_000),
        ] {
            assert!(
                step_bucket_ms(timestamp_ms, secs(15)) == want,
                "{timestamp_ms}"
            );
        }
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
mod series_type;
mod step_bucket_ms;
mod step_from_secs;
mod validated_step;

use decimal_i64_to_f64::decimal_i64_to_f64;
use decimal_usize_to_f64::decimal_usize_to_f64;
pub use fold_bucket::fold_bucket;
pub use series_agg::SeriesAgg;
pub use series_type::Series;
pub use step_bucket_ms::step_bucket_ms;
pub use step_from_secs::step_from_secs;
pub(crate) use validated_step::validated_step;
