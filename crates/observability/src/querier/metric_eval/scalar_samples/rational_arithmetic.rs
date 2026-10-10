//! Arithmetic on the `(numerator, denominator)` pairs that `MetricValue` and
//! `ScalarSample` both store, with the sign on the numerator.

use num_traits::{FromPrimitive as _, ToPrimitive as _};

use super::METRIC_DECIMAL_SCALE;

/// A signed rational, with the sign on the numerator.
#[derive(Clone, Copy)]
pub(crate) struct Rational {
    pub(crate) numerator: i128,
    pub(crate) denominator: u128,
}

impl Rational {
    /// `self / divisor`, or `None` when `divisor` is zero or a product
    /// overflows.
    pub(crate) fn checked_quotient(self, divisor: Self) -> Option<Self> {
        if divisor.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(divisor.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(divisor.numerator)?;
        // `< 0` against `<= 0` is a permanent survivor: a zero denominator is
        // normalised to one, and the divisor's numerator was rejected above,
        // so this product is never zero.
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self {
            numerator,
            denominator: u128::try_from(denominator).ok()?,
        })
    }
}

/// The value of a rational as a finite `f64`.
pub(crate) fn rational_to_f64(numerator: i128, denominator: u128) -> Option<f64> {
    let quotient = numerator.to_f64()? / denominator.to_f64()?;
    quotient.is_finite().then_some(quotient)
}

/// The numerator of `decimal` over [`METRIC_DECIMAL_SCALE`], rounded, or `None`
/// for a value that is not finite or does not fit.
pub(crate) fn decimal_scaled_numerator(decimal: f64) -> Option<i128> {
    if !decimal.is_finite() {
        return None;
    }

    let scaled = (decimal * METRIC_DECIMAL_SCALE.to_f64()?).round();
    i128::from_f64(scaled)
}

/// Implements `divide`, `modulo` and `power` for a rational type with
/// `numerator` and `denominator` fields, a normalising `new`, and `to_f64` and
/// `from_f64`.
///
/// A macro, not a trait, so the methods stay inherent and every caller keeps
/// calling them without a trait import.
macro_rules! impl_rational_division_ops {
    ($rational:ty) => {
        impl $rational {
            pub(crate) fn divide(self, other: Self) -> Option<Self> {
                let Rational {
                    numerator,
                    denominator,
                } = Rational {
                    numerator: self.numerator,
                    denominator: self.denominator,
                }
                .checked_quotient(Rational {
                    numerator: other.numerator,
                    denominator: other.denominator,
                })?;
                Some(Self::new(numerator, denominator))
            }

            pub(crate) fn modulo(self, other: Self) -> Option<Self> {
                if other.numerator == 0 {
                    return None;
                }

                Self::from_f64(self.to_f64()? % other.to_f64()?)
            }

            pub(crate) fn power(self, other: Self) -> Option<Self> {
                Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
            }
        }
    };
}

pub(crate) use impl_rational_division_ops;
