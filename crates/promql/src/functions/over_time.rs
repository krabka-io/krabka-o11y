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

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use num_traits::ToPrimitive;

use crate::range_array::RangeArray;

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use assert2::check;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    /// Runs an `OverTimeUdf` directly over a multi-step window set.
    ///
    /// This function builds the invoke arguments and returns each step's value,
    /// or `None` for a no-value NULL cell. `phi` is supplied only for the
    /// quantile family.
    fn run_udf_nullable(
        family: OverTimeFamily,
        steps: &[(i64, &[i64], &[f64])],
        phi: f64,
    ) -> Vec<Option<f64>> {
        let udf = OverTimeUdf::new(family);
        let mut all_ts = Vec::new();
        let mut all_val = Vec::new();
        let mut ranges = Vec::new();
        let mut eval = Vec::new();
        let mut offset = 0_u32;
        for (eval_ts, ts, val) in steps {
            assert2::assert!(ts.len() == val.len());
            let len = u32::try_from(ts.len()).unwrap();
            all_ts.extend_from_slice(ts);
            all_val.extend_from_slice(val);
            ranges.push((offset, len));
            offset += len;
            eval.push(*eval_ts);
        }
        let (value_ra, ts_ra) = RangeArray::from_paired_ranges(
            Float64Array::from(all_val),
            Int64Array::from(all_ts),
            ranges,
        )
        .unwrap();
        let ts_dict: ArrayRef = Arc::new(ts_ra.into_dict_array().unwrap());
        let val_dict: ArrayRef = Arc::new(value_ra.into_dict_array().unwrap());
        let eval_col: ArrayRef = Arc::new(Int64Array::from(eval.clone()));

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
        let out = udf.invoke_with_args(args).unwrap();
        let array = out.into_array(rows).unwrap();
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..floats.len())
            .map(|i| {
                if floats.is_null(i) {
                    None
                } else {
                    Some(floats.value(i))
                }
            })
            .collect()
    }

    /// Wrapper that asserts every step produced a non-null value.
    ///
    /// This function returns the unwrapped floats. Tests for the no-value NULL
    /// case call [`run_udf_nullable`] directly.
    fn run_udf(family: OverTimeFamily, steps: &[(i64, &[i64], &[f64])], phi: f64) -> Vec<f64> {
        run_udf_nullable(family, steps, phi)
            .into_iter()
            .map(|value| value.expect("expected a non-null value cell"))
            .collect()
    }

    /// One window with 3,5 reproduces the engine's basic reductions.
    ///
    /// The engine test is
    /// `instant_basic_over_time_functions_reduce_range_samples`.
    #[test]
    fn basic_reductions_match_engine() {
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], &[3.0, 5.0])];
        for (family, want) in [
            (OverTimeFamily::Sum, 8.0),
            (OverTimeFamily::Avg, 4.0),
            (OverTimeFamily::Count, 2.0),
            (OverTimeFamily::Min, 3.0),
            (OverTimeFamily::Max, 5.0),
            (OverTimeFamily::Last, 5.0),
            (OverTimeFamily::Present, 1.0),
        ] {
            let got = run_udf(family, window, 0.0)[0];
            assert2::assert!(approx_eq(got, want));
        }
    }

    /// Population stddev and stdvar over 2,4,4,4,5,5,7,9 match the engine.
    ///
    /// The engine test
    /// `instant_statistical_over_time_functions_reduce_range_samples` gives
    /// stdvar == 4 and stddev == 2. The median quantile (0.5) == 4.5.
    #[test]
    fn statistical_reductions_match_engine() {
        let vals: &[f64] = &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let ts: Vec<i64> = (0..i64::try_from(vals.len()).unwrap())
            .map(|i| (i + 1) * 60_000)
            .collect();
        let window: &[(i64, &[i64], &[f64])] = &[(480_000, &ts, vals)];
        for (family, phi, want) in [
            (OverTimeFamily::Stdvar, 0.0, 4.0),
            (OverTimeFamily::Stddev, 0.0, 2.0),
            (OverTimeFamily::Quantile, 0.5, 4.5),
        ] {
            let got = run_udf(family, window, phi)[0];
            assert2::assert!(approx_eq(got, want));
        }
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
        let small = over_time_variance(&[1.0, 1e-16, 1e-16, 1e-16]);
        assert2::assert!(small.to_bits() == 0x3fc7_ffff_ffff_fffe);

        let large = over_time_variance(&[1e-16, 1e16, 1e16, 5.0, 1e8, -1e8]);
        assert2::assert!(large.to_bits() == 0x4671_87bd_f63d_b730);
    }

    #[test]
    fn mean_uses_compensated_updates() {
        let mean = over_time_mean(&[1e16, 1e-16, 1e-16, -1e16]);
        assert2::assert!(mean.to_bits() == 0.25_f64.to_bits());
    }

    #[test]
    fn infinite_mean_guard_matches_prometheus_cases() {
        for (mean, value, want) in [
            (f64::INFINITY, f64::INFINITY, true),
            (f64::INFINITY, 1.0, true),
            (f64::NEG_INFINITY, f64::NEG_INFINITY, true),
            (f64::NEG_INFINITY, -1.0, true),
            (f64::INFINITY, f64::NEG_INFINITY, false),
            (f64::NEG_INFINITY, f64::INFINITY, false),
            (f64::INFINITY, f64::NAN, false),
            (1.0, 1.0, false),
        ] {
            assert2::assert!(keep_infinite_mean(mean, value) == want);
        }
    }

    #[test]
    fn kahan_sum_inc_recovers_lost_low_bits() {
        // Both operand orders (|sum| >= |increment| and the swapped branch)
        // recover the low bits into the compensation term.
        for (increment, initial_sum) in [(1e-16, 1.0), (1.0, 1e-16)] {
            let (sum, comp) = kahan_sum_inc(increment, initial_sum, 0.0);
            assert2::assert!(sum.to_bits() == 1.0_f64.to_bits());
            assert2::assert!(comp.to_bits() == 1e-16_f64.to_bits());
        }
    }

    #[test]
    fn quantile_boundaries_match_prometheus() {
        let values = [3.0, 1.0, 2.0];
        for (phi, want) in [
            (-0.1, f64::NEG_INFINITY),
            (0.0, 1.0),
            (1.0, 3.0),
            (1.1, f64::INFINITY),
        ] {
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
    }

    fn dict_i64(values: Vec<i64>, ranges: impl IntoIterator<Item = (u32, u32)>) -> ArrayRef {
        Arc::new(
            RangeArray::from_ranges(Arc::new(Int64Array::from(values)) as ArrayRef, ranges)
                .unwrap()
                .into_dict_array()
                .unwrap(),
        )
    }

    fn dict_f64(values: Vec<f64>, ranges: impl IntoIterator<Item = (u32, u32)>) -> ArrayRef {
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
        let arg_fields = vec![
            Arc::new(Field::new("eval_timestamp", DataType::Int64, false)),
            Arc::new(Field::new(
                "timestamp_range",
                ts_dict.data_type().clone(),
                false,
            )),
            Arc::new(Field::new(
                "value_range",
                val_dict.data_type().clone(),
                false,
            )),
        ];
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

    #[test]
    fn invoke_rejects_each_row_count_mismatch() {
        let err = invoke_sum_with_columns(
            2,
            Arc::new(Int64Array::from(vec![60_000, 120_000])) as ArrayRef,
            dict_i64(vec![60_000], [(0, 1)]),
            dict_f64(vec![1.0, 2.0], [(0, 1), (1, 1)]),
        )
        .unwrap_err()
        .to_string();
        assert2::assert!(err.contains("row-count mismatch"));

        let err = invoke_sum_with_columns(
            2,
            Arc::new(Int64Array::from(vec![60_000])) as ArrayRef,
            dict_i64(vec![60_000, 120_000], [(0, 1), (1, 1)]),
            dict_f64(vec![1.0, 2.0], [(0, 1), (1, 1)]),
        )
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
        let window: &[(i64, &[i64], &[f64])] =
            &[(300_000, &[60_000, 300_000, 120_000], &[1.0, 9.0, 2.0])];
        assert2::assert!(approx_eq(
            run_udf(OverTimeFamily::Last, window, 0.0)[0],
            9.0
        ));
    }

    /// An empty window gives NULL, not a NaN sentinel.
    ///
    /// Prometheus has no value there, so the assembler drops the series and
    /// aggregates skip it.
    #[test]
    fn empty_window_yields_null() {
        let window: &[(i64, &[i64], &[f64])] = &[(60_000, &[], &[])];
        for (family, phi) in [
            (OverTimeFamily::Sum, 0.0),
            (OverTimeFamily::Count, 0.0),
            (OverTimeFamily::Present, 0.0),
            (OverTimeFamily::Quantile, 0.5),
        ] {
            assert2::assert!(run_udf_nullable(family, window, phi)[0].is_none());
        }
    }

    /// A computed reduction stays a non-null float even when its value is NaN.
    ///
    /// A window that holds a NaN sample still gives a non-null cell, so
    /// downstream aggregates propagate it and do not skip it.
    #[test]
    fn genuine_nan_reduction_is_kept_non_null() {
        // sum over [NaN, 1.0] is a genuine NaN value (the window is non-empty, so
        // this is not the no-value case).
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], &[f64::NAN, 1.0])];
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
        let cases: &[(&[f64], f64, f64)] = &[
            // {NaN, 1, 2}: min=1, max=2 (the leading NaN is folded out).
            (&[f64::NAN, 1.0, 2.0], 1.0, 2.0),
            // {1, NaN}: a trailing NaN never displaces the running extremum.
            (&[1.0, f64::NAN], 1.0, 1.0),
            // {NaN, NaN}: an all-NaN window stays NaN.
            (&[f64::NAN, f64::NAN], f64::NAN, f64::NAN),
        ];
        for &(vals, want_min, want_max) in cases {
            let ts: Vec<i64> = (1..=i64::try_from(vals.len()).unwrap())
                .map(|i| i * 60_000)
                .collect();
            let window: &[(i64, &[i64], &[f64])] = &[(*ts.last().unwrap(), &ts, vals)];
            for (family, want) in [
                (OverTimeFamily::Min, want_min),
                (OverTimeFamily::Max, want_max),
            ] {
                let got = run_udf(family, window, 0.0)[0];
                let matches = if want.is_nan() {
                    got.is_nan()
                } else {
                    approx_eq(got, want)
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
        let window: &[(i64, &[i64], &[f64])] = &[(180_000, ts, vals)];
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
        let window: &[(i64, &[i64], &[f64])] = &[(120_000, &[60_000, 120_000], vals)];
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
                (120_000, &[60_000, 120_000], &[1.0, 2.0]),
                (240_000, &[180_000, 240_000], &[3.0, 4.0]),
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

    /// Confirms the helper round-trips a `DictionaryArray` back into a `RangeArray`.
    #[test]
    fn decode_range_column_round_trips() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let range = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 1)]).unwrap();
        let dict: ArrayRef = Arc::new(range.into_dict_array().unwrap());
        let back = decode_range_column(&dict, "value_range", "prom_sum_over_time").unwrap();
        check!(back.len() == 2);
        check!(back.value_slice(0).unwrap() == [1.0, 2.0]);

        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        check!(decode_range_column(&plain, "value_range", "prom_sum_over_time").is_err());
    }
}

// === split-modules: generated submodules ===
mod decode_range_column;
mod extremum;
mod fold_extremum;
mod kahan_sum_inc;
mod keep_infinite_mean;
mod last_value_by_timestamp;
mod over_time_family;
mod over_time_family_udfs;
mod over_time_mean;
mod over_time_udf;
mod over_time_variance;
mod quantile_value;
mod register_over_time_udfs;
mod scalar_f64;

use decode_range_column::decode_range_column;
use extremum::Extremum;
use fold_extremum::fold_extremum;
use kahan_sum_inc::kahan_sum_inc;
use keep_infinite_mean::keep_infinite_mean;
use last_value_by_timestamp::last_value_by_timestamp;
pub use over_time_family::OverTimeFamily;
pub use over_time_family_udfs::over_time_family_udfs;
use over_time_mean::over_time_mean;
pub use over_time_udf::over_time_udf;
use over_time_variance::over_time_variance;
use quantile_value::quantile_value;
pub use register_over_time_udfs::register_over_time_udfs;
use scalar_f64::scalar_f64;
