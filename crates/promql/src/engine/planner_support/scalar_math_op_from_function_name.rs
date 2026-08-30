use super::ScalarMathOp;

/// Maps a `PromQL` function name to its per-row scalar-math op.
///
/// Returns `None` for any function outside the scalar-math set, which stays on
/// the interpreter. `pi` is a 0-arg literal, not a per-row op, so this map
/// excludes it.
pub(crate) fn scalar_math_op_from_function_name(name: &str) -> Option<ScalarMathOp> {
    Some(match name {
        "abs" => ScalarMathOp::Abs,
        "ceil" => ScalarMathOp::Ceil,
        "floor" => ScalarMathOp::Floor,
        "sqrt" => ScalarMathOp::Sqrt,
        "exp" => ScalarMathOp::Exp,
        "ln" => ScalarMathOp::Ln,
        "log2" => ScalarMathOp::Log2,
        "log10" => ScalarMathOp::Log10,
        "sgn" => ScalarMathOp::Sgn,
        "sin" => ScalarMathOp::Sin,
        "cos" => ScalarMathOp::Cos,
        "tan" => ScalarMathOp::Tan,
        "asin" => ScalarMathOp::Asin,
        "acos" => ScalarMathOp::Acos,
        "atan" => ScalarMathOp::Atan,
        "sinh" => ScalarMathOp::Sinh,
        "cosh" => ScalarMathOp::Cosh,
        "tanh" => ScalarMathOp::Tanh,
        "asinh" => ScalarMathOp::Asinh,
        "acosh" => ScalarMathOp::Acosh,
        "atanh" => ScalarMathOp::Atanh,
        "deg" => ScalarMathOp::Deg,
        "rad" => ScalarMathOp::Rad,
        "round" => ScalarMathOp::Round,
        "clamp_min" => ScalarMathOp::ClampMin,
        "clamp_max" => ScalarMathOp::ClampMax,
        "clamp" => ScalarMathOp::Clamp,
        _ => return None,
    })
}
