//! Pure extrapolation math for the rate-family `PromQL` functions.
//!
//! These free functions are a byte-for-byte port of the interpreter's
//! counter-reset and extrapolation algorithm. See `engine.rs`'s
//! `extrapolated_rate` and `instant_delta`. The math lives here so that the
//! `ScalarUDF`s in [`super::rate`] can reuse the exact arithmetic the
//! tree-walking engine already validates against the conformance corpus, and so
//! that tests can check the numbers directly.
//!
//! All inputs are decoded `&[f64]` values paired 1:1 with `&[i64]` millisecond
//! timestamps, as produced by `RangeManipulate`'s `<value>_range` and
//! `<time>_range` columns. `range` is the range-selector window width.
//! `range_end_ms` is the eval instant `t` the window closes on. `range_start_ms`
//! is `t - range`. Every function returns `None` where Prometheus yields no
//! sample, for example fewer than two points or a zero-width sampled interval.
//! The UDF layer renders that `None` as a NULL cell.

use krabka_units::prelude::*;
use num_traits::ToPrimitive;

#[cfg(test)]
mod tests {

    use super::*;

    /// Mirrors the engine's `approx_eq` tolerance for f64 sample comparisons.
    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    /// Pins `rate` to `engine.rs::instant_rate_extrapolates_counter_window`:
    /// samples at 0..240s that step by 1.0, `rate(...[5m])` at t=300s == 5/300.
    #[test]
    fn rate_extrapolates_counter_window_like_engine() {
        let timestamps = [0_i64, 60_000, 120_000, 180_000, 240_000];
        let values = [0.0, 1.0, 2.0, 3.0, 4.0];
        // range_end = 300_000, range = 300_000 (5m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            300_000,
            millis(300_000),
            RangeKind::Rate,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 5.0 / 300.0));
    }

    /// Pins `increase` reset correction to
    /// `engine.rs::instant_increase_corrects_counter_resets`: 1,2,1 over [2m]
    /// at t=120s => increase == 2.0. The drop 2->1 adds back the pre-reset 2.
    #[test]
    fn increase_corrects_counter_resets_like_engine() {
        let timestamps = [0_i64, 60_000, 120_000];
        let values = [1.0, 2.0, 1.0];
        // range_end = 120_000, range = 120_000 (2m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            120_000,
            millis(120_000),
            RangeKind::Increase,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 2.0));
    }

    /// Pins `delta` gauge mode to
    /// `engine.rs::instant_delta_is_gauge_delta_without_reset_correction`:
    /// 4,3 over [1m] at t=60s => delta == -2.0. There is no reset correction, so
    /// the drop stays. The first sample is at 30s and the second at 60s.
    #[test]
    fn delta_is_gauge_delta_without_reset_correction_like_engine() {
        let timestamps = [30_000_i64, 60_000];
        let values = [4.0, 3.0];
        // range_end = 60_000, range = 60_000 (1m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            60_000,
            millis(60_000),
            RangeKind::Delta,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, -2.0));
    }

    /// The function caps a duration slightly beyond 110% of the average sample
    /// interval to half an interval. This matches Prometheus' extrapolation
    /// threshold.
    #[test]
    fn extrapolation_threshold_uses_ten_percent_slack() {
        let timestamps = [11_050_i64, 21_050];
        let values = [2.0, 12.0];
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            21_050,
            millis(21_050),
            RangeKind::Delta,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 15.0));
    }

    /// Counter extrapolation clamps the start duration to the extrapolated zero
    /// point when the counter would otherwise project below zero.
    #[test]
    fn counter_zero_anchor_limits_start_extrapolation() {
        let timestamps = [5_000_i64, 15_000];
        let values = [1.0, 4.0];
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            15_000,
            millis(15_000),
            RangeKind::Increase,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 4.0));
    }

    /// A single sample cannot form a rate: Prometheus yields no value.
    #[test]
    fn single_sample_yields_none() {
        let timestamps = [60_000_i64];
        let values = [1.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
        assert2::assert!(instant_delta(&timestamps, &values, InstantKind::Irate).is_none());
    }

    /// Timestamp/value range arrays must be paired 1:1 before any arithmetic.
    #[test]
    fn mismatched_range_lengths_yield_none() {
        let timestamps = [0_i64, 60_000];
        let values = [1.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
    }

    /// Two coincident timestamps make a zero-width sampled interval, which
    /// yields no value.
    #[test]
    fn zero_width_sampled_interval_yields_none() {
        let timestamps = [60_000_i64, 60_000];
        let values = [1.0, 2.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
    }

    /// Pins `irate` to `engine.rs::instant_irate_uses_last_two_samples_per_second`:
    /// 0,1,3 at 0/60/90s, `irate(...[2m])` at t=90s == (3-1)/((90-60)/1000) == 2/30.
    #[test]
    fn irate_uses_last_two_samples_per_second_like_engine() {
        let timestamps = [0_i64, 60_000, 90_000];
        let values = [0.0, 1.0, 3.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 2.0 / 30.0));
    }

    /// Pins `idelta` to
    /// `engine.rs::instant_idelta_uses_last_two_samples_without_per_second_division`:
    /// 0,1,3 at 0/60/90s, `idelta(...[2m])` at t=90s == 3-1 == 2.0, with no
    /// division.
    #[test]
    fn idelta_uses_last_two_samples_without_division_like_engine() {
        let timestamps = [0_i64, 60_000, 90_000];
        let values = [0.0, 1.0, 3.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Idelta).unwrap();
        assert2::assert!(approx_eq(got, 2.0));
    }

    /// `irate` clamps a negative last-pair delta, which is a counter reset, to
    /// the last value. This matches the engine's `instant_delta` reset branch.
    #[test]
    fn irate_clamps_counter_reset_to_last_value() {
        // last pair drops 5 -> 2 over 1s: reset, so result = last (2) / 1s = 2.
        let timestamps = [0_i64, 1_000];
        let values = [5.0, 2.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 2.0));
        // idelta preserves the negative delta (gauge): 2 - 5 = -3.
        let idelta = instant_delta(&timestamps, &values, InstantKind::Idelta).unwrap();
        assert2::assert!(approx_eq(idelta, -3.0));
    }

    /// Equal adjacent counter samples are a zero rate, not a reset.
    #[test]
    fn irate_equal_samples_yield_zero_without_reset_clamp() {
        let timestamps = [0_i64, 1_000];
        let values = [5.0, 5.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 0.0));
    }
}

// === split-modules: generated submodules ===
mod extrapolated_rate;
mod instant_delta;
mod instant_kind;
mod range_kind;

pub use extrapolated_rate::extrapolated_rate;
pub use instant_delta::instant_delta;
pub use instant_kind::InstantKind;
pub use range_kind::RangeKind;
