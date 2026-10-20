//! `*_over_time` `PromQL` functions as `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF reads the windowed columns that `RangeManipulate` emits per eval
//! step, and returns a `Float64Array` with one value per step. The per-window
//! reductions are a byte-for-byte port of the tree-walking engine's
//! `over_time_sample_from_series` (float path) and `quantile_value`. These UDFs
//! and the interpreter agree on every number.
//!
//! # Call convention
//!
//! Every non-quantile `*_over_time` UDF takes three positional arguments in
//! this order:
//!
//! 1. `eval_timestamp` (`Int64`): the eval instant `t` each window closes on.
//!    This is `RangeManipulate`'s scalar `timestamp` column.
//! 2. `timestamp_range` (`Dictionary<Int64, List<Int64>>`): the windowed sample
//!    timestamps. This is `RangeManipulate`'s `<time>_range` column.
//! 3. `value_range` (`Dictionary<Int64, List<Float64>>`): the windowed sample
//!    values. This is `RangeManipulate`'s `<value>_range` column, 1:1 with (2).
//!
//! `quantile_over_time` takes a fourth argument, the quantile `phi`
//! (`Float64` scalar). `phi` comes before the three windowed columns:
//! `prom_quantile_over_time(phi, eval_timestamp, timestamp_range, value_range)`.

//!
//! Every UDF accepts the `eval_timestamp` and `timestamp_range` columns, but
//! only `last_over_time` reads the timestamps, to pick the latest sample. One
//! uniform shape keeps the planner lowering simple. An empty window gives NULL,
//! not a NaN sentinel, because Prometheus emits no sample there. The assembler
//! drops that cell and downstream aggregates skip it, the same as the
//! interpreter, which omits no-value series before it aggregates. A computed
//! reduction stays a non-null float and propagates, even when its value is NaN.

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::{
    common::Result as DfResult,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use num_traits::ToPrimitive;

use crate::engine::kahan_sum_inc;

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Float64Array, Int64Array},
        datatypes::Field,
    };
    use assert2::check;
    use datafusion::common::ScalarValue;

    use super::*;
    use crate::{
        functions::udf_test_support::{WindowStep, approx_eq, nullable_floats, window_columns},
        range_array::RangeArray,
    };

    /// Runs an `OverTimeUdf` directly over a multi-step window set.
    ///
    /// This function builds the invoke arguments and returns each step's value,
    /// or `None` for a no-value NULL cell. `phi` is supplied only for the
    /// quantile family.
    fn run_udf_nullable(
        family: OverTimeFamily,
        steps: &[WindowStep<'_>],
        phi: f64,
    ) -> Vec<Option<f64>> {
        let udf = OverTimeUdf::new(family);
        let [eval_col, ts_dict, val_dict] = window_columns(steps);

        let rows = steps.len();
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let mut arg_fields = Vec::new();
        let mut call_args = Vec::new();
        if family.takes_quantile_param() {
            arg_fields.push(Arc::new(Field::new("phi", DataType::Float64, false)));
            call_args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(phi))));
        }
        arg_fields.push(Arc::new(Field::new(
            "eval_timestamp",
            DataType::Int64,
            false,
        )));
        arg_fields.push(Arc::new(Field::new(
            "timestamp_range",
            ts_dict.data_type().clone(),
            false,
        )));
        arg_fields.push(Arc::new(Field::new(
            "value_range",
            val_dict.data_type().clone(),
            false,
        )));
        call_args.push(ColumnarValue::Array(eval_col));
        call_args.push(ColumnarValue::Array(ts_dict));
        call_args.push(ColumnarValue::Array(val_dict));

        let args = ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        nullable_floats(udf.invoke_with_args(args).unwrap(), rows)
    }

    /// Wrapper that asserts every step produced a non-null value.
    ///
    /// This function returns the unwrapped floats. Tests for the no-value NULL
    /// case call [`run_udf_nullable`] directly.
    fn run_udf(family: OverTimeFamily, steps: &[WindowStep<'_>], phi: f64) -> Vec<f64> {
        run_udf_nullable(family, steps, phi)
            .into_iter()
            .map(|value| value.expect("expected a non-null value cell"))
            .collect()
    }

    /// One window with 3,5 reproduces the engine's basic reductions.
    ///
    /// The engine test is
    /// `instant_basic_over_time_functions_reduce_range_samples`.
    #[yare::parameterized(
        sum = { OverTimeFamily::Sum, 8.0 },
        average = { OverTimeFamily::Avg, 4.0 },
        count = { OverTimeFamily::Count, 2.0 },
        minimum = { OverTimeFamily::Min, 3.0 },
        maximum = { OverTimeFamily::Max, 5.0 },
        last = { OverTimeFamily::Last, 5.0 },
        present = { OverTimeFamily::Present, 1.0 },
    )]
    fn basic_reductions_match_engine(family: OverTimeFamily, want: f64) {
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 120_000,
            timestamps: &[60_000, 120_000],
            sample_values: &[3.0, 5.0],
        }];
        let got = run_udf(family, window, 0.0)[0];
        assert2::assert!(approx_eq(got, want));
    }

    /// Population stddev and stdvar over 2,4,4,4,5,5,7,9 match the engine.
    ///
    /// The engine test
    /// `instant_statistical_over_time_functions_reduce_range_samples` gives
    /// stdvar == 4 and stddev == 2. The median quantile (0.5) == 4.5.
    #[yare::parameterized(
        variance = { OverTimeFamily::Stdvar, 0.0, 4.0 },
        standard_deviation = { OverTimeFamily::Stddev, 0.0, 2.0 },
        median = { OverTimeFamily::Quantile, 0.5, 4.5 },
    )]
    fn statistical_reductions_match_engine(family: OverTimeFamily, phi: f64, want: f64) {
        let vals: &[f64] = &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let ts: Vec<i64> = (0..i64::try_from(vals.len()).unwrap())
            .map(|i| (i + 1) * 60_000)
            .collect();
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 480_000,
            timestamps: &ts,
            sample_values: vals,
        }];
        let got = run_udf(family, window, phi)[0];
        assert2::assert!(approx_eq(got, want));
    }

    #[test]
    fn extremum_ties_preserve_first_signed_zero() {
        let min = fold_extremum(&[0.0, -0.0], Extremum::Min);
        assert2::assert!(min.to_bits() == 0.0_f64.to_bits());

        let max = fold_extremum(&[-0.0, 0.0], Extremum::Max);
        assert2::assert!(max.to_bits() == (-0.0_f64).to_bits());
    }

    #[test]
    fn variance_uses_compensated_welford_terms() {
        let small = over_time_variance([1.0, 1e-16, 1e-16, 1e-16]);
        assert2::assert!(small.to_bits() == 0x3fc7_ffff_ffff_fffe);

        let large = over_time_variance([1e-16, 1e16, 1e16, 5.0, 1e8, -1e8]);
        assert2::assert!(large.to_bits() == 0x4671_87bd_f63d_b730);
    }

    /// The direct Kahan mean keeps the low bits that cancel against the large
    /// ones: `1e16 + 1e-16 + 1e-16 - 1e16` really is `2e-16`, so the mean of
    /// those four samples is `5e-17` and not zero.
    #[test]
    fn mean_uses_compensated_updates() {
        let mean = over_time_mean(&[1e16, 1e-16, 1e-16, -1e16]);
        assert2::assert!(mean.to_bits() == 5e-17_f64.to_bits());
    }

    /// The running sum switches the fold to an incremental mean, so a window
    /// that overflows `f64` still averages to the infinity Prometheus reports.
    #[test]
    fn mean_saturates_the_way_prometheus_does() {
        struct MeanCase {
            values: Vec<f64>,
            expected: f64,
        }
        for case in [
            MeanCase {
                values: vec![f64::INFINITY],
                expected: f64::INFINITY,
            },
            MeanCase {
                values: vec![f64::INFINITY, 1.0],
                expected: f64::INFINITY,
            },
            MeanCase {
                values: vec![f64::NEG_INFINITY, -1.0],
                expected: f64::NEG_INFINITY,
            },
            MeanCase {
                values: vec![f64::MAX, f64::MAX],
                expected: f64::MAX,
            },
        ] {
            assert2::assert!(
                over_time_mean(&case.values).to_bits() == case.expected.to_bits(),
                "{:?}",
                case.values
            );
        }
        assert2::assert!(over_time_mean(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
    }

    /// `sum_over_time` compensates, so a small term is not lost against a large
    /// one that later cancels.
    #[test]
    fn sum_is_kahan_compensated() {
        assert2::assert!(over_time_sum(&[1.0, 1e-16, -1.0]).to_bits() == 1e-16_f64.to_bits());
        assert2::assert!(over_time_sum(&[f64::INFINITY, 1.0]).to_bits() == f64::INFINITY.to_bits());
    }

    #[test]
    fn kahan_sum_inc_recovers_lost_low_bits() {
        // Both operand orders (|sum| >= |increment| and the swapped branch)
        // recover the low bits into the compensation term.
        struct KahanInputCase {
            increment: f64,
            initial_sum: f64,
        }
        for case in [
            KahanInputCase {
                increment: 1e-16,
                initial_sum: 1.0,
            },
            KahanInputCase {
                increment: 1.0,
                initial_sum: 1e-16,
            },
        ] {
            let mut sum = case.initial_sum;
            let mut comp = 0.0;
            kahan_sum_inc(case.increment, &mut sum, &mut comp);
            assert2::assert!(sum.to_bits() == 1.0_f64.to_bits());
            assert2::assert!(comp.to_bits() == 1e-16_f64.to_bits());
        }
    }

    #[yare::parameterized(
        below = { -0.1, f64::NEG_INFINITY },
        lower = { 0.0, 1.0 },
        upper = { 1.0, 3.0 },
        above = { 1.1, f64::INFINITY },
    )]
    fn quantile_boundaries_match_prometheus(phi: f64, want: f64) {
        let values = [3.0, 1.0, 2.0];
        let got = quantile_value(phi, &values).unwrap();
        // Out-of-range phi yields an exact signed infinity; in-range keeps
        // the epsilon comparison.
        let matches = if want.is_infinite() {
            got.to_bits() == want.to_bits()
        } else {
            approx_eq(got, want)
        };
        assert2::assert!(matches);
    }

    fn dict_i64(
        values: Vec<i64>,
        ranges: impl IntoIterator<Item = crate::RangeWindow>,
    ) -> ArrayRef {
        Arc::new(
            RangeArray::from_ranges(Arc::new(Int64Array::from(values)) as ArrayRef, ranges)
                .unwrap()
                .into_dict_array()
                .unwrap(),
        )
    }

    fn dict_f64(
        values: Vec<f64>,
        ranges: impl IntoIterator<Item = crate::RangeWindow>,
    ) -> ArrayRef {
        Arc::new(
            RangeArray::from_ranges(Arc::new(Float64Array::from(values)) as ArrayRef, ranges)
                .unwrap()
                .into_dict_array()
                .unwrap(),
        )
    }

    fn invoke_sum_with_columns(
        rows: usize,
        eval_col: ArrayRef,
        ts_dict: ArrayRef,
        val_dict: ArrayRef,
    ) -> DfResult<ColumnarValue> {
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = crate::functions::udf_test_support::range_argument_fields(
            &eval_col, &ts_dict, &val_dict,
        );
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(eval_col),
                ColumnarValue::Array(ts_dict),
                ColumnarValue::Array(val_dict),
            ],
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        OverTimeUdf::new(OverTimeFamily::Sum).invoke_with_args(args)
    }

    #[yare::parameterized(
        eval_timestamp = { false },
        timestamp_range = { true },
    )]
    fn invoke_rejects_row_count_mismatch(timestamp_range_has_wrong_rows: bool) {
        let eval_col = if timestamp_range_has_wrong_rows {
            Arc::new(Int64Array::from(vec![60_000])) as ArrayRef
        } else {
            Arc::new(Int64Array::from(vec![60_000, 120_000])) as ArrayRef
        };
        let ts_dict = if timestamp_range_has_wrong_rows {
            dict_i64(
                vec![60_000, 120_000],
                [crate::RangeWindow::new(0, 1), crate::RangeWindow::new(1, 1)],
            )
        } else {
            dict_i64(vec![60_000], [crate::RangeWindow::new(0, 1)])
        };
        let values = dict_f64(
            vec![1.0, 2.0],
            [crate::RangeWindow::new(0, 1), crate::RangeWindow::new(1, 1)],
        );

        let err = invoke_sum_with_columns(2, eval_col, ts_dict, values)
            .unwrap_err()
            .to_string();
        assert2::assert!(err.contains("row-count mismatch"));
    }

    #[test]
    fn scalar_f64_rejects_empty_or_null_array_fallback() {
        let empty = ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new())));
        assert2::assert!(scalar_f64(&empty, "phi", "prom_quantile_over_time").is_err());

        let null = ColumnarValue::Array(Arc::new(Float64Array::from(vec![None])));
        assert2::assert!(scalar_f64(&null, "phi", "prom_quantile_over_time").is_err());
    }

    /// `last_over_time` returns the latest sample's value from an unordered window.
    #[test]
    fn last_uses_max_timestamp() {
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 300_000,
            timestamps: &[60_000, 300_000, 120_000],
            sample_values: &[1.0, 9.0, 2.0],
        }];
        assert2::assert!(approx_eq(
            run_udf(OverTimeFamily::Last, window, 0.0)[0],
            9.0
        ));
    }

    /// An empty window gives NULL, not a NaN sentinel.
    ///
    /// Prometheus has no value there, so the assembler drops the series and
    /// aggregates skip it.
    #[yare::parameterized(
        sum = { OverTimeFamily::Sum, 0.0 },
        count = { OverTimeFamily::Count, 0.0 },
        present = { OverTimeFamily::Present, 0.0 },
        quantile = { OverTimeFamily::Quantile, 0.5 },
    )]
    fn empty_window_yields_null(family: OverTimeFamily, phi: f64) {
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 60_000,
            timestamps: &[],
            sample_values: &[],
        }];
        assert2::assert!(run_udf_nullable(family, window, phi)[0].is_none());
    }

    /// A computed reduction stays a non-null float even when its value is NaN.
    ///
    /// A window that holds a NaN sample still gives a non-null cell, so
    /// downstream aggregates propagate it and do not skip it.
    #[test]
    fn genuine_nan_reduction_is_kept_non_null() {
        // sum over [NaN, 1.0] is a genuine NaN value (the window is non-empty, so
        // this is not the no-value case).
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 120_000,
            timestamps: &[60_000, 120_000],
            sample_values: &[f64::NAN, 1.0],
        }];
        let out = run_udf_nullable(OverTimeFamily::Sum, window, 0.0);
        assert2::assert!(out[0].is_some());
        assert2::assert!(out[0].unwrap().is_nan());
    }

    /// H9: `min_over_time` and `max_over_time` ignore NaN.
    ///
    /// A NaN sample never displaces a non-NaN extremum, at any position. A
    /// window's extremum is over its non-NaN samples. The extremum is NaN only
    /// when every sample is NaN.
    #[test]
    fn min_max_over_time_ignore_nan() {
        struct ExtremumCase {
            values: &'static [f64],
            expected_min: f64,
            expected_max: f64,
        }

        let cases = [
            // {NaN, 1, 2}: min=1, max=2 (the leading NaN is folded out).
            ExtremumCase {
                values: &[f64::NAN, 1.0, 2.0],
                expected_min: 1.0,
                expected_max: 2.0,
            },
            // {1, NaN}: a trailing NaN never displaces the running extremum.
            ExtremumCase {
                values: &[1.0, f64::NAN],
                expected_min: 1.0,
                expected_max: 1.0,
            },
            // {NaN, NaN}: an all-NaN window stays NaN.
            ExtremumCase {
                values: &[f64::NAN, f64::NAN],
                expected_min: f64::NAN,
                expected_max: f64::NAN,
            },
        ];
        for case in cases {
            let vals = case.values;
            let ts: Vec<i64> = (1..=i64::try_from(vals.len()).unwrap())
                .map(|i| i * 60_000)
                .collect();
            let window: &[WindowStep<'_>] = &[WindowStep {
                eval_ts_ms: *ts.last().unwrap(),
                timestamps: &ts,
                sample_values: vals,
            }];
            struct ExtremumExpectation {
                family: OverTimeFamily,
                expected: f64,
            }
            for expectation in [
                ExtremumExpectation {
                    family: OverTimeFamily::Min,
                    expected: case.expected_min,
                },
                ExtremumExpectation {
                    family: OverTimeFamily::Max,
                    expected: case.expected_max,
                },
            ] {
                let got = run_udf(expectation.family, window, 0.0)[0];
                let matches = if expectation.expected.is_nan() {
                    got.is_nan()
                } else {
                    approx_eq(got, expectation.expected)
                };
                assert2::assert!(matches);
            }
        }
    }

    /// M16: a close-valued window at a large offset must not cancel to a negative variance.
    ///
    /// A negative variance has a NaN `sqrt`. For `stdvar_over_time` and
    /// `stddev_over_time`, Welford gives the small positive population variance
    /// and stddev.
    #[test]
    fn over_time_variance_is_stable_for_large_offset_window() {
        let vals: &[f64] = &[1e8, 1e8 + 1.0, 1e8 + 2.0];
        let ts: &[i64] = &[60_000, 120_000, 180_000];
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 180_000,
            timestamps: ts,
            sample_values: vals,
        }];
        // population variance of {0,1,2} == 2/3; stddev == sqrt(2/3). Pinning
        // the exact positive value also rules out the cancellation failure
        // (a negative variance whose sqrt is NaN).
        let stdvar = run_udf(OverTimeFamily::Stdvar, window, 0.0)[0];
        assert2::assert!(approx_eq(stdvar, 2.0 / 3.0));
        let stddev = run_udf(OverTimeFamily::Stddev, window, 0.0)[0];
        assert2::assert!(approx_eq(stddev, (2.0_f64 / 3.0).sqrt()));
    }

    /// M17: `avg_over_time` must not overflow the running sum to +/-Inf.
    ///
    /// The samples have a very large magnitude. The incremental Kahan mean
    /// stays finite.
    #[test]
    fn avg_over_time_does_not_overflow() {
        let vals: &[f64] = &[f64::MAX, f64::MAX];
        let window: &[WindowStep<'_>] = &[WindowStep {
            eval_ts_ms: 120_000,
            timestamps: &[60_000, 120_000],
            sample_values: vals,
        }];
        let avg = run_udf(OverTimeFamily::Avg, window, 0.0)[0];
        // The naive `(MAX + MAX) / 2` overflows to +Inf; the mean of two equal
        // values is the value itself.
        assert2::assert!(avg.is_finite());
        assert2::assert!(approx_eq(avg, f64::MAX));
    }

    /// A multi-step window set gives one reduction per step.
    #[test]
    fn produces_per_step_vector() {
        let out = run_udf(
            OverTimeFamily::Sum,
            &[
                WindowStep {
                    eval_ts_ms: 120_000,
                    timestamps: &[60_000, 120_000],
                    sample_values: &[1.0, 2.0],
                },
                WindowStep {
                    eval_ts_ms: 240_000,
                    timestamps: &[180_000, 240_000],
                    sample_values: &[3.0, 4.0],
                },
            ],
            0.0,
        );
        check!(out.len() == 2);
        check!(approx_eq(out[0], 3.0));
        check!(approx_eq(out[1], 7.0));
    }

    /// The UDFs register on a `SessionContext` under their Prometheus-prefixed
    /// names, so a planner can resolve them.
    #[test]
    fn register_installs_named_udfs() {
        use datafusion::execution::FunctionRegistry;

        let ctx = SessionContext::new();
        register_over_time_udfs(&ctx);
        for name in [
            "prom_sum_over_time",
            "prom_avg_over_time",
            "prom_count_over_time",
            "prom_min_over_time",
            "prom_max_over_time",
            "prom_stddev_over_time",
            "prom_stdvar_over_time",
            "prom_last_over_time",
            "prom_present_over_time",
            "prom_quantile_over_time",
        ] {
            assert2::assert!(ctx.udf(name).is_ok());
        }
    }
}

mod extremum;
mod over_time_family;
mod over_time_udf;
mod over_time_variance;
mod quantile_value;

use extremum::Extremum;
pub use over_time_family::OverTimeFamily;
#[cfg(test)]
use over_time_udf::OverTimeUdf;
pub use over_time_udf::over_time_udf;
pub(crate) use over_time_variance::over_time_variance;
use quantile_value::quantile_value;

use super::{
    impl_window_udf_identity,
    udf_args::{WindowColumns, append_window_values, scalar_f64},
};

/// Returns the arithmetic mean of a non-empty `values` window.
///
/// This is the engine's `avg_over_time` fold, so the UDF and the engine agree
/// bit-for-bit. An empty window returns NaN.
pub(crate) fn over_time_mean(values: &[f64]) -> f64 {
    crate::engine::over_time_mean(values.iter().copied())
}

/// Registers every `*_over_time` UDF on `ctx`, so a planner can lower onto them.
pub fn register_over_time_udfs(ctx: &SessionContext) {
    for udf in over_time_family_udfs() {
        ctx.register_udf(udf);
    }
}

/// Folds a non-empty window to its `min` or `max` and ignores NaN.
///
/// The fold seeds with the first sample, NaN included, then replaces the
/// running value under [`Extremum::should_replace`]. The result is NaN only
/// when every sample is NaN. This matches Prometheus, the engine's
/// `over_time_sample_from_series`, and the `prom_min`/`prom_max` aggregate UDAF.
pub(crate) fn fold_extremum(values: &[f64], extremum: Extremum) -> f64 {
    let mut running = values[0];
    for &candidate in &values[1..] {
        if extremum.should_replace(running, candidate) {
            running = candidate;
        }
    }
    running
}

/// Returns the value of the sample with the greatest timestamp.
///
/// A tie selects the later element, the same as `max_by_key(timestamp)` over a
/// time-sorted window. This function returns `None` only for an empty window.
pub(crate) fn last_value_by_timestamp(timestamps: &[i64], values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // The window is time-sorted ascending (SeriesNormalize), so the engine's
    // `max_by_key(timestamp)` is the last element. Fall back to a scan if the
    // timestamps and values disagree in length (defensive; should not happen).
    if timestamps.len() == values.len() {
        let mut best_idx = 0;
        for entry in timestamps.iter().enumerate() {
            let idx = entry.0;
            let ts = entry.1;
            // `max_by_key` keeps the *last* maximum on ties.
            if *ts >= timestamps[best_idx] {
                best_idx = idx;
            }
        }
        return Some(values[best_idx]);
    }
    values.last().copied()
}

/// Returns every non-experimental `*_over_time` UDF, ready to register on a
/// [`SessionContext`].
#[must_use]
pub fn over_time_family_udfs() -> Vec<ScalarUDF> {
    [
        OverTimeFamily::Sum,
        OverTimeFamily::Avg,
        OverTimeFamily::Count,
        OverTimeFamily::Min,
        OverTimeFamily::Max,
        OverTimeFamily::Stddev,
        OverTimeFamily::Stdvar,
        OverTimeFamily::Last,
        OverTimeFamily::Present,
        OverTimeFamily::Quantile,
    ]
    .into_iter()
    .map(over_time_udf)
    .collect()
}

/// Returns the Kahan-compensated sum of a float window.
///
/// Prometheus' `sum_over_time` compensates the running sum and adds the
/// compensation back at the end, so a window whose terms differ by many orders
/// of magnitude does not lose the small ones. A sum that saturates to an
/// infinity drops the compensation, which would otherwise be a NaN.
pub(crate) fn over_time_sum(values: &[f64]) -> f64 {
    let mut sum = 0.0_f64;
    let mut comp = 0.0_f64;
    for &value in values {
        kahan_sum_inc(value, &mut sum, &mut comp);
    }
    if sum.is_infinite() {
        return sum;
    }
    sum + comp
}
