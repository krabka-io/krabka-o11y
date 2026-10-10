use super::{
    METRIC_DECIMAL_SCALE, Rational, ScalarComparisonOp, decimal_scaled_numerator, gcd_signed,
    impl_rational_division_ops, rational_to_f64,
};

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

    pub(crate) fn add(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_add(right?)?, denominator))
    }

    pub(crate) fn subtract(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
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
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?)?;
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
        let negative = self.numerator < 0;
        let numerator = self.numerator.unsigned_abs();
        let whole = numerator / self.denominator;
        let mut remainder = numerator % self.denominator;
        let sign = if negative { "-" } else { "" };
        if remainder == 0 {
            return format!("{sign}{whole}");
        }

        let mut decimals = String::new();
        while remainder != 0 && decimals.len() < 9 {
            remainder *= 10;
            let digit =
                u8::try_from(remainder / self.denominator).expect("decimal digit is less than 10");
            decimals.push(char::from(b'0' + digit));
            remainder %= self.denominator;
        }
        while decimals.ends_with('0') {
            decimals.pop();
        }
        format!("{sign}{whole}.{decimals}")
    }

    pub(crate) fn format_fixed_six(self) -> String {
        format!("{:.6}", self.to_f64().unwrap_or_default())
    }
}
