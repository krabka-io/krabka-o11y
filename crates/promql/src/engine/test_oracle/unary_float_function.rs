use super::*;

pub(crate) fn unary_float_function(name: &str) -> Option<UnaryFloatFn> {
    Some(match name {
        "ceil" => UnaryFloatFn::Ceil,
        "floor" => UnaryFloatFn::Floor,
        "sgn" => UnaryFloatFn::Sgn,
        "abs" => UnaryFloatFn::Abs,
        "sqrt" => UnaryFloatFn::Sqrt,
        "exp" => UnaryFloatFn::Exp,
        "ln" => UnaryFloatFn::Ln,
        "log2" => UnaryFloatFn::Log2,
        "log10" => UnaryFloatFn::Log10,
        "sin" => UnaryFloatFn::Sin,
        "sinh" => UnaryFloatFn::Sinh,
        "cos" => UnaryFloatFn::Cos,
        "cosh" => UnaryFloatFn::Cosh,
        "tan" => UnaryFloatFn::Tan,
        "tanh" => UnaryFloatFn::Tanh,
        "asin" => UnaryFloatFn::Asin,
        "asinh" => UnaryFloatFn::Asinh,
        "acos" => UnaryFloatFn::Acos,
        "acosh" => UnaryFloatFn::Acosh,
        "atan" => UnaryFloatFn::Atan,
        "atanh" => UnaryFloatFn::Atanh,
        "deg" => UnaryFloatFn::Deg,
        "rad" => UnaryFloatFn::Rad,
        _ => return None,
    })
}
