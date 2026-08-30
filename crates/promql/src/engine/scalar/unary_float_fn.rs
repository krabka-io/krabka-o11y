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
impl UnaryFloatFn {
    pub(crate) fn apply(self, value: f64) -> f64 {
        match self {
            Self::Ceil => value.ceil(),
            Self::Floor => value.floor(),
            Self::Abs => value.abs(),
            Self::Sqrt => value.sqrt(),
            Self::Exp => value.exp(),
            Self::Ln => value.ln(),
            Self::Log2 => value.log2(),
            Self::Log10 => value.log10(),
            Self::Sin => value.sin(),
            Self::Sinh => value.sinh(),
            Self::Cos => value.cos(),
            Self::Cosh => value.cosh(),
            Self::Tan => value.tan(),
            Self::Tanh => value.tanh(),
            Self::Asin => value.asin(),
            Self::Asinh => value.asinh(),
            Self::Acos => value.acos(),
            Self::Acosh => value.acosh(),
            Self::Atan => value.atan(),
            Self::Atanh => value.atanh(),
            Self::Deg => value.to_degrees(),
            Self::Rad => value.to_radians(),
            Self::Sgn => {
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
        }
    }
}
