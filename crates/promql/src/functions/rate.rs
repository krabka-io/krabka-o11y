//! Rate-family `PromQL` functions as `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF reads the windowed columns that `RangeManipulate` emits per eval
//! step and returns a `Float64Array` with one value per step. The shared
//! extrapolation and instant math lives in [`super::extrapolate`], a
//! byte-for-byte port of the tree-walking engine, so these UDFs and the
//! interpreter agree on every number.
//!
//! # Call convention
//!
//! The planner calls every rate-family UDF with four positional arguments, in
//! this order:
//!
//! 1. `eval_timestamp` (`Int64`): the eval instant `t` that each window closes
//!    on. This is `RangeManipulate`'s scalar `timestamp` column, `range_end_ms`.
//! 2. `timestamp_range` (`Dictionary<Int64, List<Int64>>`): the windowed sample
//!    timestamps, `RangeManipulate`'s `<time>_range` column.
//! 3. `value_range` (`Dictionary<Int64, List<Float64>>`): the windowed sample
//!    values, `RangeManipulate`'s `<value>_range` column, 1:1 with (2).
//! 4. `range_ms` (`Int64`, scalar): the range-selector width in milliseconds.
//!    `range_start_ms = eval_timestamp - range_ms`.
//!
//! All five functions take the same arity even though `irate` and `idelta`
//! ignore `range_ms` and use only the last two samples. One uniform shape keeps
//! the planner lowering simple.
//!
//! A cell that Prometheus has no value for renders
//! as NULL, not as a NaN sentinel. Such a cell has fewer than two samples or a
//! zero-width interval. The assembler then drops the series and downstream
//! aggregates skip it, as in the interpreter, which omits no-value series before
//! it aggregates. A value that a UDF computes stays a non-null float and
//! propagates, even when that value is NaN.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Float64Builder, Int64Array},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use krabka_units::prelude::*;

use super::extrapolate::{InstantKind, RangeKind, extrapolated_rate, instant_delta};
use crate::range_array::RangeArray;

#[cfg(test)]
mod tests {
    use arrow::{
        array::Float64Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use assert2::check;
    use datafusion::common::ScalarValue;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    fn timestamp_range(windows: &[&[i64]]) -> ArrayRef {
        let mut values = Vec::new();
        let mut ranges = Vec::new();
        let mut offset = 0_u32;
        for window in windows {
            let len = u32::try_from(window.len()).unwrap();
            values.extend_from_slice(window);
            ranges.push((offset, len));
            offset += len;
        }
        let range = RangeArray::from_ranges(Arc::new(Int64Array::from(values)) as ArrayRef, ranges)
            .unwrap();
        Arc::new(range.into_dict_array().unwrap())
    }

    fn value_range(windows: &[&[f64]]) -> ArrayRef {
        let mut values = Vec::new();
        let mut ranges = Vec::new();
        let mut offset = 0_u32;
        for window in windows {
            let len = u32::try_from(window.len()).unwrap();
            values.extend_from_slice(window);
            ranges.push((offset, len));
            offset += len;
        }
        let range =
            RangeArray::from_ranges(Arc::new(Float64Array::from(values)) as ArrayRef, ranges)
                .unwrap();
        Arc::new(range.into_dict_array().unwrap())
    }

    fn invoke_args(
        eval_col: ArrayRef,
        ts_dict: ArrayRef,
        val_dict: ArrayRef,
        range_ms: ColumnarValue,
        rows: usize,
    ) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new(
                "eval_timestamp",
                eval_col.data_type().clone(),
                false,
            )),
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
            Arc::new(Field::new("range_ms", DataType::Int64, false)),
        ];
        ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(eval_col),
                ColumnarValue::Array(ts_dict),
                ColumnarValue::Array(val_dict),
                range_ms,
            ],
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    /// Builds the four invoke args for a window set and runs a `RateUdf`.
    ///
    /// The four args are `eval_ts`, `timestamp_range`, `value_range`, and
    /// `range_ms`. This function returns each step's value, or `None` for a
    /// no-value NULL cell.
    fn run_udf_nullable(
        udf: &RateUdf,
        steps: &[(i64, &[i64], &[f64])],
        range_ms: i64,
    ) -> Vec<Option<f64>> {
        // Flatten the per-step windows into paired backing arrays + ranges.
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
        let args = invoke_args(
            eval_col,
            ts_dict,
            val_dict,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(range_ms))),
            rows,
        );

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

    /// Runs the UDF and asserts that every step made a non-null value.
    ///
    /// This function returns the unwrapped floats. Tests for the no-value NULL
    /// case call [`run_udf_nullable`] directly.
    fn run_udf(udf: &RateUdf, steps: &[(i64, &[i64], &[f64])], range_ms: i64) -> Vec<f64> {
        run_udf_nullable(udf, steps, range_ms)
            .into_iter()
            .map(|value| value.expect("expected a non-null value cell"))
            .collect()
    }

    #[test]
    fn rate_udf_rejects_each_row_count_mismatch_independently() {
        let udf = RateUdf::new(RateFamily::Rate);
        let eval: ArrayRef = Arc::new(Int64Array::from(vec![60_000_i64]));
        let timestamps = timestamp_range(&[&[0, 60_000]]);
        let values = value_range(&[&[1.0, 2.0]]);
        let range_ms = ColumnarValue::Scalar(ScalarValue::Int64(Some(60_000)));

        for (_case, args) in [
            (
                "timestamp_range has extra rows",
                invoke_args(
                    Arc::clone(&eval),
                    timestamp_range(&[&[0, 60_000], &[0, 60_000]]),
                    Arc::clone(&values),
                    range_ms.clone(),
                    1,
                ),
            ),
            (
                "value_range has extra rows",
                invoke_args(
                    Arc::clone(&eval),
                    Arc::clone(&timestamps),
                    value_range(&[&[1.0, 2.0], &[1.0, 2.0]]),
                    range_ms.clone(),
                    1,
                ),
            ),
            (
                "eval_timestamp has extra rows",
                invoke_args(
                    Arc::new(Int64Array::from(vec![60_000_i64, 120_000])),
                    timestamps,
                    values,
                    range_ms,
                    1,
                ),
            ),
        ] {
            assert2::assert!(udf.invoke_with_args(args).is_err());
        }
    }

    #[test]
    fn rate_udf_rejects_empty_or_null_range_ms_array() {
        let udf = RateUdf::new(RateFamily::Rate);
        let eval: ArrayRef = Arc::new(Int64Array::from(vec![60_000_i64]));
        let timestamps = timestamp_range(&[&[0, 60_000]]);
        let values = value_range(&[&[1.0, 2.0]]);

        assert2::assert!(
            udf.invoke_with_args(invoke_args(
                Arc::clone(&eval),
                Arc::clone(&timestamps),
                Arc::clone(&values),
                ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
                1,
            ))
            .is_err()
        );
        assert2::assert!(
            udf.invoke_with_args(invoke_args(
                eval,
                timestamps,
                values,
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>]))),
                1,
            ))
            .is_err()
        );
    }

    /// `prom_rate` over the engine's counter window reproduces 5/300, the same
    /// number that `engine.rs::instant_rate_extrapolates_counter_window` asserts.
    #[test]
    fn rate_udf_matches_engine_counter_window() {
        let udf = RateUdf::new(RateFamily::Rate);
        // Single eval step at t=300s, window (0, 300s] holds 0..240s.
        let out = run_udf(
            &udf,
            &[(
                300_000,
                &[0, 60_000, 120_000, 180_000, 240_000],
                &[0.0, 1.0, 2.0, 3.0, 4.0],
            )],
            300_000,
        );
        assert2::assert!(out.len() == 1);
        assert2::assert!(approx_eq(out[0], 5.0 / 300.0));
    }

    /// `prom_rate` across multiple eval steps returns a per-step rate vector.
    ///
    /// The vector matches `engine.rs::range_rate_uses_each_step_as_window_end`:
    /// 4/300 and 5/300 for the two steps at t=240s and t=300s.
    #[test]
    fn rate_udf_produces_per_step_vector() {
        let udf = RateUdf::new(RateFamily::Rate);
        let out = run_udf(
            &udf,
            &[
                // t=240s, window (-60s, 240s] -> 0..240s.
                (
                    240_000,
                    &[0, 60_000, 120_000, 180_000, 240_000],
                    &[0.0, 1.0, 2.0, 3.0, 4.0],
                ),
                // t=300s, window (0, 300s] -> 60..300s.
                (
                    300_000,
                    &[60_000, 120_000, 180_000, 240_000, 300_000],
                    &[1.0, 2.0, 3.0, 4.0, 5.0],
                ),
            ],
            300_000,
        );
        assert2::assert!(out.len() == 2);
        for (step, want) in [(0_usize, 4.0 / 300.0), (1, 5.0 / 300.0)] {
            assert2::assert!(approx_eq(out[step], want));
        }
    }

    /// `prom_increase` reproduces the engine's reset correction: 1,2,1 -> 2.0.
    #[test]
    fn increase_udf_corrects_counter_reset() {
        let udf = RateUdf::new(RateFamily::Increase);
        let out = run_udf(
            &udf,
            &[(120_000, &[0, 60_000, 120_000], &[1.0, 2.0, 1.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0));
    }

    /// `prom_delta` is gauge mode: 4,3 -> -2.0, which matches the engine.
    #[test]
    fn delta_udf_is_gauge_delta() {
        let udf = RateUdf::new(RateFamily::Delta);
        let out = run_udf(&udf, &[(60_000, &[30_000, 60_000], &[4.0, 3.0])], 60_000);
        assert2::assert!(approx_eq(out[0], -2.0));
    }

    /// `prom_irate` reproduces 2/30 from the last two samples (engine number).
    #[test]
    fn irate_udf_uses_last_two_samples() {
        let udf = RateUdf::new(RateFamily::Irate);
        let out = run_udf(
            &udf,
            &[(90_000, &[0, 60_000, 90_000], &[0.0, 1.0, 3.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0 / 30.0));
    }

    /// `prom_idelta` reproduces 2.0 from the last two samples (engine number).
    #[test]
    fn idelta_udf_uses_last_two_samples() {
        let udf = RateUdf::new(RateFamily::Idelta);
        let out = run_udf(
            &udf,
            &[(90_000, &[0, 60_000, 90_000], &[0.0, 1.0, 3.0])],
            120_000,
        );
        assert2::assert!(approx_eq(out[0], 2.0));
    }

    /// A window with fewer than two samples renders as NULL, not a NaN sentinel.
    ///
    /// Prometheus has no value for such a window. The assembler drops the series
    /// and aggregates skip it.
    #[test]
    fn under_two_samples_yields_null() {
        let udf = RateUdf::new(RateFamily::Rate);
        let out = run_udf_nullable(&udf, &[(60_000, &[60_000], &[1.0])], 60_000);
        assert2::assert!(out[0].is_none());
    }

    /// A computed value stays a non-null float even when the value is NaN.
    ///
    /// One example is a delta over a window that holds a NaN sample. The cell is
    /// non-null, so downstream aggregates propagate it and do not skip it.
    #[test]
    fn genuine_nan_value_is_kept_non_null() {
        let udf = RateUdf::new(RateFamily::Delta);
        // Two in-window samples (NaN, 1.0): the gauge delta is computed (not a
        // no-value case), and the arithmetic yields NaN. It must be a non-null
        // NaN cell, not a NULL.
        let out = run_udf_nullable(
            &udf,
            &[(120_000, &[60_000, 120_000], &[f64::NAN, 1.0])],
            120_000,
        );
        assert2::assert!(out[0].is_some());
        assert2::assert!(out[0].unwrap().is_nan());
    }

    /// The UDF installs onto a `SessionContext` under its Prometheus-prefixed
    /// names, so a planner can resolve them.
    #[test]
    fn register_installs_named_udfs() {
        use datafusion::execution::FunctionRegistry;

        let ctx = SessionContext::new();
        register_rate_udfs(&ctx);
        for name in [
            "prom_rate",
            "prom_increase",
            "prom_delta",
            "prom_irate",
            "prom_idelta",
        ] {
            assert2::assert!(ctx.udf(name).is_ok());
        }
    }

    /// End-to-end test: registers the UDF on a context, then invokes it through
    /// a SQL projection over a `RecordBatch` that holds the `RangeManipulate`
    /// columns.
    #[tokio::test]
    async fn rate_udf_runs_through_sql_projection() {
        use datafusion::datasource::MemTable;

        // One eval step: t=300s, window holds 0..240s stepping by 1.0.
        let (value_ra, ts_ra) = RangeArray::from_paired_ranges(
            Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
            Int64Array::from(vec![0_i64, 60_000, 120_000, 180_000, 240_000]),
            [(0_u32, 5_u32)],
        )
        .unwrap();
        let ts_dict: ArrayRef = Arc::new(ts_ra.into_dict_array().unwrap());
        let val_dict: ArrayRef = Arc::new(value_ra.into_dict_array().unwrap());
        let eval_col: ArrayRef = Arc::new(Int64Array::from(vec![300_000_i64]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("timestamp_range", ts_dict.data_type().clone(), false),
            Field::new("value_range", val_dict.data_type().clone(), false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![eval_col, ts_dict, val_dict]).unwrap();

        let ctx = SessionContext::new();
        register_rate_udfs(&ctx);
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("series", Arc::new(table)).unwrap();

        let df = ctx
            .sql(
                "SELECT prom_rate(timestamp, timestamp_range, value_range, CAST(300000 AS BIGINT)) AS r FROM series",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let column = results[0]
            .column_by_name("r")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(column.len() == 1);
        assert2::assert!(approx_eq(column.value(0), 5.0 / 300.0));
    }

    /// Confirms that the helper round-trips a `DictionaryArray` into a `RangeArray`.
    #[test]
    fn decode_range_column_round_trips() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let range = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 1)]).unwrap();
        let dict: ArrayRef = Arc::new(range.into_dict_array().unwrap());
        let back = decode_range_column(&dict, "value_range", "prom_rate").unwrap();
        check!(back.len() == 2);
        check!(back.value_slice(0).unwrap() == [1.0, 2.0]);

        // A non-dictionary column is rejected.
        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        check!(decode_range_column(&plain, "value_range", "prom_rate").is_err());
    }
}

// === split-modules: generated submodules ===
mod decode_range_column;
mod delta_udf;
mod idelta_udf;
mod increase_udf;
mod irate_udf;
mod rate_family;
mod rate_family_udfs;
mod rate_udf;
mod register_rate_udfs;
mod scalar_i64;

use decode_range_column::decode_range_column;
pub use delta_udf::delta_udf;
pub use idelta_udf::idelta_udf;
pub use increase_udf::increase_udf;
pub use irate_udf::irate_udf;
use rate_family::RateFamily;
pub use rate_family_udfs::rate_family_udfs;
use rate_udf::RateUdf;
pub use rate_udf::rate_udf;
pub use register_rate_udfs::register_rate_udfs;
use scalar_i64::scalar_i64;
