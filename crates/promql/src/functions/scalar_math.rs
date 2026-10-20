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
    common::{DataFusionError, Result as DfResult},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Float64Array, datatypes::Field};
    use datafusion::common::ScalarValue;

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

        let args = scalar_math_args(call_args, arg_fields, rows);
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
        let arg_fields = (0..call_args.len())
            .map(|index| Arc::new(Field::new(format!("arg_{index}"), DataType::Float64, true)))
            .collect();
        udf.invoke_with_args(scalar_math_args(call_args, arg_fields, rows))
    }

    fn scalar_math_args(
        args: Vec<ColumnarValue>,
        arg_fields: Vec<Arc<Field>>,
        rows: usize,
    ) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    fn bits_eq(left: f64, right: f64) -> bool {
        left.to_bits() == right.to_bits() || (left.is_nan() && right.is_nan())
    }

    #[yare::parameterized(
        abs = { ScalarMathOp::Abs, -3.0, 3.0 },
        sqrt = { ScalarMathOp::Sqrt, 4.0, 2.0 },
        sqrt_negative = { ScalarMathOp::Sqrt, -1.0, f64::NAN },
        ln_zero = { ScalarMathOp::Ln, 0.0, f64::NEG_INFINITY },
        ln_negative = { ScalarMathOp::Ln, -1.0, f64::NAN },
        log2 = { ScalarMathOp::Log2, 8.0, 3.0 },
    )]
    fn unary_matches_rust_f64(op: ScalarMathOp, input: f64, want: f64) {
        let got = run(op, &[], &[input])[0];
        assert2::assert!(bits_eq(got, want));
    }

    #[yare::parameterized(
        positive = { 5.0, 1.0 },
        negative = { -5.0, -1.0 },
        zero = { 0.0, 0.0 },
        negative_zero = { -0.0, 0.0 },
        nan = { f64::NAN, f64::NAN },
    )]
    fn sgn_handles_nan_and_signed_zero(input: f64, want: f64) {
        let got = run(ScalarMathOp::Sgn, &[], &[input])[0];
        assert2::assert!(bits_eq(got, want));
    }

    #[yare::parameterized(
        positive_half = { 1.0, 2.5, 3.0 },
        negative_half = { 1.0, -2.5, -2.0 },
        nearest_ten_low = { 5.0, 12.0, 10.0 },
        nearest_ten_high = { 5.0, 13.0, 15.0 },
    )]
    fn round_matches_interpreter_half_up(to_nearest: f64, value: f64, want: f64) {
        let got = run(ScalarMathOp::Round, &[to_nearest], &[value])[0];
        assert2::assert!(bits_eq(got, want));
    }

    #[test]
    fn clamp_family_bounds_values() {
        // Signed zeros pass through unclamped (pinned by the bit comparison);
        // a NaN bound yields NaN.
        struct ClampCase {
            op: ScalarMathOp,
            bounds: &'static [f64],
            value: f64,
            expected: f64,
        }
        let cases = [
            ClampCase {
                op: ScalarMathOp::ClampMin,
                bounds: &[0.0],
                value: -3.0,
                expected: 0.0,
            },
            ClampCase {
                op: ScalarMathOp::ClampMin,
                bounds: &[0.0],
                value: 3.0,
                expected: 3.0,
            },
            ClampCase {
                op: ScalarMathOp::ClampMax,
                bounds: &[10.0],
                value: 42.0,
                expected: 10.0,
            },
            ClampCase {
                op: ScalarMathOp::Clamp,
                bounds: &[0.0, 100.0],
                value: 150.0,
                expected: 100.0,
            },
            ClampCase {
                op: ScalarMathOp::Clamp,
                bounds: &[0.0, 100.0],
                value: -5.0,
                expected: 0.0,
            },
            ClampCase {
                op: ScalarMathOp::ClampMin,
                bounds: &[0.0],
                value: -0.0,
                expected: -0.0,
            },
            ClampCase {
                op: ScalarMathOp::ClampMax,
                bounds: &[-0.0],
                value: 0.0,
                expected: 0.0,
            },
            ClampCase {
                op: ScalarMathOp::ClampMin,
                bounds: &[f64::NAN],
                value: 3.0,
                expected: f64::NAN,
            },
        ];
        for case in cases {
            let got = run(case.op, case.bounds, &[case.value])[0];
            assert2::assert!(bits_eq(got, case.expected));
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
mod scalar_math_op;
mod scalar_math_udf;
mod scalar_math_udfs;
pub use scalar_math_op::ScalarMathOp;
#[cfg(test)]
use scalar_math_udf::ScalarMathUdf;
pub use scalar_math_udf::scalar_math_udf;
pub use scalar_math_udfs::scalar_math_udfs;

use super::udf_args::scalar_f64;

/// Registers every scalar-math UDF on `ctx` so a planner can lower onto them.
pub fn register_scalar_math_udfs(ctx: &SessionContext) {
    for udf in scalar_math_udfs() {
        ctx.register_udf(udf);
    }
}

/// Rounds `value` to the nearest multiple of `to_nearest`, ties going up.
///
/// Prometheus INVERTS the step and multiplies, rather than dividing by it:
/// `math.Floor(f*toNearestInverse+0.5) / toNearestInverse`. The two spellings
/// disagree in the last bits for a step such as `0.1`, whose inverse is exact
/// while the step itself is not, so the inversion is part of the answer.
pub(crate) fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    let inverse = 1.0 / to_nearest;
    (value * inverse + 0.5).floor() / inverse
}

/// Port of the interpreter's `clamp_float`.
pub(crate) fn clamp_float(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
    if min.is_some_and(f64::is_nan) || max.is_some_and(f64::is_nan) {
        return f64::NAN;
    }
    if let Some(min) = min
        && value < min
    {
        return min;
    }
    if let Some(max) = max
        && value > max
    {
        return max;
    }
    value
}

/// Returns the sign of `value` as the `PromQL` `sgn` function does: `1`, `-1` or `0`, and
/// NaN for NaN.
pub(crate) fn prometheus_sgn(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value > 0.0 {
        1.0
    } else if value < 0.0 {
        -1.0
    } else {
        0.0
    }
}
