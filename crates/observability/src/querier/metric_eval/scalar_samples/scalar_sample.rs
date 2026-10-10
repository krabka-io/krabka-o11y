use super::{
    METRIC_DECIMAL_SCALE, Rational, ScalarComparisonOp, decimal_scaled_numerator, gcd_signed,
    impl_rational_division_ops, rational_to_f64,
};
use crate::{MetricValue, format_metric_value};

impl_rational_division_ops!(ScalarSample);

#[derive(Clone, Copy)]
pub(crate) struct ScalarSample {
    pub(crate) numerator: i128,
    pub(crate) denominator: u128,
}

impl ScalarSample {
    pub(crate) fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self {
                numerator: 0,
                denominator: 1,
            };
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).unwrap_or(i128::MAX),
            denominator: denominator / divisor,
        }
    }

    /// The numerator rescaled by `other_denominator`, so that two samples
    /// share the denominator `self.denominator * other_denominator`.
    fn scaled_numerator(self, other_denominator: u128) -> Option<i128> {
        self.numerator
            .checked_mul(i128::try_from(other_denominator).ok()?)
    }

    pub(crate) fn add(self, other: Self) -> Option<Self> {
        let left = self.scaled_numerator(other.denominator);
        let right = other.scaled_numerator(self.denominator);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_add(right?)?, denominator))
    }

    pub(crate) fn subtract(self, other: Self) -> Option<Self> {
        let left = self.scaled_numerator(other.denominator);
        let right = other.scaled_numerator(self.denominator);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_sub(right?)?, denominator))
    }

    pub(crate) fn multiply(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator.checked_mul(other.numerator)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }

    pub(crate) fn compare(self, operator: ScalarComparisonOp, other: Self) -> Option<bool> {
        let left = self.scaled_numerator(other.denominator)?;
        let right = other.scaled_numerator(self.denominator)?;
        Some(match operator {
            ScalarComparisonOp::Equal => left == right,
            ScalarComparisonOp::NotEqual => left != right,
            ScalarComparisonOp::Greater => left > right,
            ScalarComparisonOp::GreaterOrEqual => left >= right,
            ScalarComparisonOp::Less => left < right,
            ScalarComparisonOp::LessOrEqual => left <= right,
        })
    }

    pub(crate) fn to_f64(self) -> Option<f64> {
        rational_to_f64(self.numerator, self.denominator)
    }

    pub(crate) fn from_f64(value: f64) -> Option<Self> {
        Some(Self::new(
            decimal_scaled_numerator(value)?,
            METRIC_DECIMAL_SCALE,
        ))
    }

    pub(crate) fn format(self) -> String {
        format_metric_value(MetricValue {
            numerator: self.numerator,
            denominator: self.denominator,
        })
    }

    pub(crate) fn format_fixed_six(self) -> String {
        format!("{:.6}", self.to_f64().unwrap_or_default())
    }
}
