use krabka_domain_macros::EnumName;

use super::{clamp_float, round_to_nearest};

/// Which per-row scalar function a [`ScalarMathUdf`] evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, EnumName)]
#[enum_name(accessor = "udf_name")]
pub enum ScalarMathOp {
    #[name(value = "prom_abs")]
    Abs,
    #[name(value = "prom_ceil")]
    Ceil,
    #[name(value = "prom_floor")]
    Floor,
    #[name(value = "prom_sqrt")]
    Sqrt,
    #[name(value = "prom_exp")]
    Exp,
    #[name(value = "prom_ln")]
    Ln,
    #[name(value = "prom_log2")]
    Log2,
    #[name(value = "prom_log10")]
    Log10,
    #[name(value = "prom_sgn")]
    Sgn,
    #[name(value = "prom_sin")]
    Sin,
    #[name(value = "prom_cos")]
    Cos,
    #[name(value = "prom_tan")]
    Tan,
    #[name(value = "prom_asin")]
    Asin,
    #[name(value = "prom_acos")]
    Acos,
    #[name(value = "prom_atan")]
    Atan,
    #[name(value = "prom_sinh")]
    Sinh,
    #[name(value = "prom_cosh")]
    Cosh,
    #[name(value = "prom_tanh")]
    Tanh,
    #[name(value = "prom_asinh")]
    Asinh,
    #[name(value = "prom_acosh")]
    Acosh,
    #[name(value = "prom_atanh")]
    Atanh,
    #[name(value = "prom_deg")]
    Deg,
    #[name(value = "prom_rad")]
    Rad,
    /// `round(v, to_nearest?)`: `to_nearest` is the leading scalar column.
    #[name(value = "prom_round")]
    Round,
    /// `clamp_min(v, min)`: `min` is the leading scalar column.
    #[name(value = "prom_clamp_min")]
    ClampMin,
    /// `clamp_max(v, max)`: `max` is the leading scalar column.
    #[name(value = "prom_clamp_max")]
    ClampMax,
    /// `clamp(v, min, max)`: `min` and `max` are the two leading scalar columns.
    #[name(value = "prom_clamp")]
    Clamp,
}

impl ScalarMathOp {
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
