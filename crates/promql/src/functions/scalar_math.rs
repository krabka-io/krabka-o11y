//! Per-row scalar math / trig / clamp / round / sgn `PromQL` functions as
//! `DataFusion` [`ScalarUDF`]s.
//!
//! Each UDF reads a single `Float64` `value` column and returns a `Float64`
//! result, one value per row. That column holds the per-series value of the
//! inner instant vector. `clamp`, `clamp_min`, and `clamp_max` thread their
//! bounds, and `round` threads its optional `to_nearest`, as more leading
//! `Float64` scalar columns.
//!
//! The math is a byte-for-byte port of the tree-walking interpreter's
//! `UnaryFloatFn::apply`, `clamp_float`, and `round_to_nearest`. The operator
//! path and the interpreter therefore agree on every number, including the edge
//! values that a `DataFusion` built-in math expression can round differently.
//! Those edge values are `ln(0)`, `sqrt(-1)`, `sgn(NaN)`, `sgn(-0.0)`, and the
//! `.5` rounding direction. A UDF also removes the need to audit each built-in
//! against Prometheus.
//!
//! # Call convention
//!
//! - Unary families (`abs`, `ceil`, …, `deg`, `rad`): `prom_<fn>(value)`.
//! - `clamp_min`/`clamp_max`: `prom_clamp_min(bound, value)` /
//!   `prom_clamp_max(bound, value)`, where the bound leads.
//! - `clamp`: `prom_clamp(min, max, value)`, where both bounds lead.
//! - `round`: `prom_round(to_nearest, value)`, where `to_nearest` leads.
//!
//! A UDF keeps every NaN and never drops one. `f(NaN)` and `sqrt(-1)` render as
//! `NaN`, as in the interpreter, which keeps every float sample.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Float64Array, Float64Builder},
    datatypes::DataType,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Float64Array, datatypes::Field};

    use super::*;

    /// Invokes a `ScalarMathUdf` over a one-batch `value` column and the given
    /// leading scalar bounds.
    ///
    /// This function returns the result column.
    fn run(op: ScalarMathOp, bounds: &[f64], values: &[f64]) -> Vec<f64> {
        let udf = ScalarMathUdf::new(op);
        let rows = values.len();
        let mut call_args = Vec::new();
        let mut arg_fields = Vec::new();
        for bound in bounds {
            call_args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(*bound))));
            arg_fields.push(Arc::new(Field::new("bound", DataType::Float64, false)));
        }
        let value_col: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        call_args.push(ColumnarValue::Array(value_col));
        arg_fields.push(Arc::new(Field::new("value", DataType::Float64, true)));

        let args = ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let array = out.into_array(rows).unwrap();
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..floats.len()).map(|i| floats.value(i)).collect()
    }

    fn invoke_with_columns(
        op: ScalarMathOp,
        call_args: Vec<ColumnarValue>,
        rows: usize,
    ) -> DfResult<ColumnarValue> {
        let udf = ScalarMathUdf::new(op);
        let arg_fields = call_args
            .iter()
            .enumerate()
            .map(|(index, _)| Arc::new(Field::new(format!("arg_{index}"), DataType::Float64, true)))
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
    }

    fn bits_eq(left: f64, right: f64) -> bool {
        left.to_bits() == right.to_bits() || (left.is_nan() && right.is_nan())
    }

    #[test]
    fn unary_matches_rust_f64() {
        // `bits_eq` treats any-NaN == any-NaN, so NaN expectations
        // (sqrt/ln of a negative, preserved rather than dropped) sit in the
        // same table as the exact cases; ln(0) -> -inf.
        for (op, input, want) in [
            (ScalarMathOp::Abs, -3.0, 3.0),
            (ScalarMathOp::Sqrt, 4.0, 2.0),
            (ScalarMathOp::Sqrt, -1.0, f64::NAN),
            (ScalarMathOp::Ln, 0.0, f64::NEG_INFINITY),
            (ScalarMathOp::Ln, -1.0, f64::NAN),
            (ScalarMathOp::Log2, 8.0, 3.0),
        ] {
            let got = run(op, &[], &[input])[0];
            assert2::assert!(bits_eq(got, want));
        }
    }

    #[test]
    fn sgn_handles_nan_and_signed_zero() {
        // -0.0 is neither > 0 nor < 0, so sgn(-0.0) = 0.0 (positive zero,
        // pinned by the bit comparison); sgn(NaN) stays NaN.
        for (input, want) in [
            (5.0, 1.0),
            (-5.0, -1.0),
            (0.0, 0.0),
            (-0.0, 0.0),
            (f64::NAN, f64::NAN),
        ] {
            let got = run(ScalarMathOp::Sgn, &[], &[input])[0];
            assert2::assert!(bits_eq(got, want));
        }
    }

    #[test]
    fn round_matches_interpreter_half_up() {
        // .5 rounds up (toward +inf), matching `round_to_nearest`.
        for (to_nearest, value, want) in [
            (1.0, 2.5, 3.0),
            (1.0, -2.5, -2.0),
            (5.0, 12.0, 10.0),
            (5.0, 13.0, 15.0),
        ] {
            let got = run(ScalarMathOp::Round, &[to_nearest], &[value])[0];
            assert2::assert!(bits_eq(got, want));
        }
    }

    #[test]
    fn clamp_family_bounds_values() {
        // Signed zeros pass through unclamped (pinned by the bit comparison);
        // a NaN bound yields NaN.
        let cases: &[(ScalarMathOp, &[f64], f64, f64)] = &[
            (ScalarMathOp::ClampMin, &[0.0], -3.0, 0.0),
            (ScalarMathOp::ClampMin, &[0.0], 3.0, 3.0),
            (ScalarMathOp::ClampMax, &[10.0], 42.0, 10.0),
            (ScalarMathOp::Clamp, &[0.0, 100.0], 150.0, 100.0),
            (ScalarMathOp::Clamp, &[0.0, 100.0], -5.0, 0.0),
            (ScalarMathOp::ClampMin, &[0.0], -0.0, -0.0),
            (ScalarMathOp::ClampMax, &[-0.0], 0.0, 0.0),
            (ScalarMathOp::ClampMin, &[f64::NAN], 3.0, f64::NAN),
        ];
        for &(op, bounds, value, want) in cases {
            let got = run(op, bounds, &[value])[0];
            assert2::assert!(bits_eq(got, want));
        }
    }

    #[test]
    fn scalar_bound_array_must_have_non_null_first_value() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));

        assert2::assert!(
            invoke_with_columns(
                ScalarMathOp::ClampMin,
                vec![
                    ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new()))),
                    ColumnarValue::Array(Arc::clone(&values)),
                ],
                1,
            )
            .is_err()
        );
        assert2::assert!(
            invoke_with_columns(
                ScalarMathOp::ClampMin,
                vec![
                    ColumnarValue::Array(Arc::new(Float64Array::from(vec![None::<f64>]))),
                    ColumnarValue::Array(values),
                ],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn nan_value_flows_through() {
        for op in [ScalarMathOp::Sin, ScalarMathOp::Abs, ScalarMathOp::Ceil] {
            assert2::assert!(run(op, &[], &[f64::NAN])[0].is_nan());
        }
    }
}

// === split-modules: generated submodules ===
mod clamp_float;
mod register_scalar_math_udfs;
mod round_to_nearest;
mod scalar_f64;
mod scalar_math_op;
mod scalar_math_udf;
mod scalar_math_udfs;

use clamp_float::clamp_float;
pub use register_scalar_math_udfs::register_scalar_math_udfs;
use round_to_nearest::round_to_nearest;
use scalar_f64::scalar_f64;
pub use scalar_math_op::ScalarMathOp;
pub use scalar_math_udf::scalar_math_udf;
pub use scalar_math_udfs::scalar_math_udfs;
