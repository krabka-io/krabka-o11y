#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum UnaryFloatFn {
    Ceil,
    Floor,
    Sgn,
    Abs,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sin,
    Sinh,
    Cos,
    Cosh,
    Tan,
    Tanh,
    Asin,
    Asinh,
    Acos,
    Acosh,
    Atan,
    Atanh,
    Deg,
    Rad,
}

#[cfg(test)]
use crate::functions::scalar_math::ScalarMathOp;

#[cfg(test)]
impl UnaryFloatFn {
    pub(crate) fn apply(self, value: f64) -> f64 {
        self.scalar_math_op().apply(value, &[])
    }

    /// The operator-path op that evaluates the same function. None of these
    /// ops takes a leading scalar parameter.
    fn scalar_math_op(self) -> ScalarMathOp {
        match self {
            Self::Ceil => ScalarMathOp::Ceil,
            Self::Floor => ScalarMathOp::Floor,
            Self::Sgn => ScalarMathOp::Sgn,
            Self::Abs => ScalarMathOp::Abs,
            Self::Sqrt => ScalarMathOp::Sqrt,
            Self::Exp => ScalarMathOp::Exp,
            Self::Ln => ScalarMathOp::Ln,
            Self::Log2 => ScalarMathOp::Log2,
            Self::Log10 => ScalarMathOp::Log10,
            Self::Sin => ScalarMathOp::Sin,
            Self::Sinh => ScalarMathOp::Sinh,
            Self::Cos => ScalarMathOp::Cos,
            Self::Cosh => ScalarMathOp::Cosh,
            Self::Tan => ScalarMathOp::Tan,
            Self::Tanh => ScalarMathOp::Tanh,
            Self::Asin => ScalarMathOp::Asin,
            Self::Asinh => ScalarMathOp::Asinh,
            Self::Acos => ScalarMathOp::Acos,
            Self::Acosh => ScalarMathOp::Acosh,
            Self::Atan => ScalarMathOp::Atan,
            Self::Atanh => ScalarMathOp::Atanh,
            Self::Deg => ScalarMathOp::Deg,
            Self::Rad => ScalarMathOp::Rad,
        }
    }
}
