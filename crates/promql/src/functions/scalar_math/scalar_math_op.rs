use super::{round_to_nearest, clamp_float};

/// Which per-row scalar function a [`ScalarMathUdf`] evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ScalarMathOp {
    Abs,
    Ceil,
    Floor,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sgn,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    /// `round(v, to_nearest?)`: `to_nearest` is the leading scalar column.
    Round,
    /// `clamp_min(v, min)`: `min` is the leading scalar column.
    ClampMin,
    /// `clamp_max(v, max)`: `max` is the leading scalar column.
    ClampMax,
    /// `clamp(v, min, max)`: `min` and `max` are the two leading scalar columns.
    Clamp,
}

impl ScalarMathOp {
    /// Returns the registered UDF name for this op.
    #[must_use]
    pub fn udf_name(self) -> &'static str {
        match self {
            Self::Abs => "prom_abs",
            Self::Ceil => "prom_ceil",
            Self::Floor => "prom_floor",
            Self::Sqrt => "prom_sqrt",
            Self::Exp => "prom_exp",
            Self::Ln => "prom_ln",
            Self::Log2 => "prom_log2",
            Self::Log10 => "prom_log10",
            Self::Sgn => "prom_sgn",
            Self::Sin => "prom_sin",
            Self::Cos => "prom_cos",
            Self::Tan => "prom_tan",
            Self::Asin => "prom_asin",
            Self::Acos => "prom_acos",
            Self::Atan => "prom_atan",
            Self::Sinh => "prom_sinh",
            Self::Cosh => "prom_cosh",
            Self::Tanh => "prom_tanh",
            Self::Asinh => "prom_asinh",
            Self::Acosh => "prom_acosh",
            Self::Atanh => "prom_atanh",
            Self::Deg => "prom_deg",
            Self::Rad => "prom_rad",
            Self::Round => "prom_round",
            Self::ClampMin => "prom_clamp_min",
            Self::ClampMax => "prom_clamp_max",
            Self::Clamp => "prom_clamp",
        }
    }

    /// Returns the count of leading `Float64` scalar columns this op threads
    /// ahead of the `value` column.
    ///
    /// `round` and `clamp_*` take bound args. Unary functions take none.
    pub(crate) fn scalar_param_count(self) -> usize {
        match self {
            Self::Round | Self::ClampMin | Self::ClampMax => 1,
            Self::Clamp => 2,
            _ => 0,
        }
    }

    /// Returns the total positional-argument count: `value` plus the leading
    /// scalars.
    pub(crate) fn arity(self) -> usize {
        self.scalar_param_count() + 1
    }

    /// Applies the op to one row.
    ///
    /// `params` holds the leading scalar args in call order: `[to_nearest]` for
    /// `round`, `[min]` or `[max]` for `clamp_min` and `clamp_max`, and
    /// `[min, max]` for `clamp`. `value` is the per-row instant-vector value.
    ///
    /// This is a direct port of the interpreter's `UnaryFloatFn::apply`,
    /// `clamp_float`, and `round_to_nearest`, and it evaluates bit-for-bit.
    pub(crate) fn apply(self, value: f64, params: &[f64]) -> f64 {
        match self {
            Self::Abs => value.abs(),
            Self::Ceil => value.ceil(),
            Self::Floor => value.floor(),
            Self::Sqrt => value.sqrt(),
            Self::Exp => value.exp(),
            Self::Ln => value.ln(),
            Self::Log2 => value.log2(),
            Self::Log10 => value.log10(),
            Self::Sin => value.sin(),
            Self::Cos => value.cos(),
            Self::Tan => value.tan(),
            Self::Asin => value.asin(),
            Self::Acos => value.acos(),
            Self::Atan => value.atan(),
            Self::Sinh => value.sinh(),
            Self::Cosh => value.cosh(),
            Self::Tanh => value.tanh(),
            Self::Asinh => value.asinh(),
            Self::Acosh => value.acosh(),
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
            // `round(v / to_nearest + 0.5).floor() * to_nearest`, matching
            // `round_to_nearest` (the `.5`-rounds-up direction included).
            Self::Round => round_to_nearest(value, params[0]),
            Self::ClampMin => clamp_float(value, Some(params[0]), None),
            Self::ClampMax => clamp_float(value, None, Some(params[0])),
            Self::Clamp => clamp_float(value, Some(params[0]), Some(params[1])),
        }
    }
}
