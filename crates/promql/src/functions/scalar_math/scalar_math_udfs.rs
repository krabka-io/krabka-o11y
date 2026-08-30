use super::*;

/// Every scalar-math UDF, ready to register on a [`SessionContext`].
#[must_use]
pub fn scalar_math_udfs() -> Vec<ScalarUDF> {
    [
        ScalarMathOp::Abs,
        ScalarMathOp::Ceil,
        ScalarMathOp::Floor,
        ScalarMathOp::Sqrt,
        ScalarMathOp::Exp,
        ScalarMathOp::Ln,
        ScalarMathOp::Log2,
        ScalarMathOp::Log10,
        ScalarMathOp::Sgn,
        ScalarMathOp::Sin,
        ScalarMathOp::Cos,
        ScalarMathOp::Tan,
        ScalarMathOp::Asin,
        ScalarMathOp::Acos,
        ScalarMathOp::Atan,
        ScalarMathOp::Sinh,
        ScalarMathOp::Cosh,
        ScalarMathOp::Tanh,
        ScalarMathOp::Asinh,
        ScalarMathOp::Acosh,
        ScalarMathOp::Atanh,
        ScalarMathOp::Deg,
        ScalarMathOp::Rad,
        ScalarMathOp::Round,
        ScalarMathOp::ClampMin,
        ScalarMathOp::ClampMax,
        ScalarMathOp::Clamp,
    ]
    .into_iter()
    .map(scalar_math_udf)
    .collect()
}
