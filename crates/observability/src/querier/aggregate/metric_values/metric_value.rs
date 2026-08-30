use super::*;

impl MetricValue {
    pub(crate) fn zero() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    pub(crate) fn integer(value: u64) -> Self {
        Self::new(i128::from(value), 1)
    }

    pub(crate) fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self::zero();
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).expect("gcd fits in i128"),
            denominator: denominator / divisor,
        }
    }

    pub(crate) fn add(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                + other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn subtract(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                - other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn multiply(self, other: Self) -> Self {
        Self::new(
            self.numerator * other.numerator,
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
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

    pub(crate) fn saturating_sub(self, other: Self) -> Self {
        if self.cmp_value(other) == Ordering::Less {
            Self::zero()
        } else {
            Self::new(
                self.numerator
                    * i128::try_from(other.denominator).expect("denominator fits in i128")
                    - other.numerator
                        * i128::try_from(self.denominator).expect("denominator fits in i128"),
                self.denominator * other.denominator,
            )
        }
    }

    pub(crate) fn divide_by(self, divisor: u64) -> Self {
        if divisor == 0 {
            Self::zero()
        } else {
            Self::new(self.numerator, self.denominator * u128::from(divisor))
        }
    }

    pub(crate) fn sqrt(self) -> Self {
        let value = self.to_f64().unwrap_or_default().sqrt();
        if !value.is_finite() || value <= 0.0 {
            return Self::zero();
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64().unwrap_or_default()).floor();
        Self::new(
            i128::from_f64(scaled).unwrap_or_default(),
            METRIC_DECIMAL_SCALE,
        )
    }

    pub(crate) fn cmp_value(self, other: Self) -> Ordering {
        (self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")).cmp(
            &(other.numerator
                * i128::try_from(self.denominator).expect("denominator fits in i128")),
        )
    }

    pub(crate) fn to_f64(self) -> Option<f64> {
        let value = self.numerator.to_f64()? / self.denominator.to_f64()?;
        value.is_finite().then_some(value)
    }

    pub(crate) fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64()?).round();
        Some(Self::new(i128::from_f64(scaled)?, METRIC_DECIMAL_SCALE))
    }
}

impl Default for MetricValue {
    fn default() -> Self {
        Self::zero()
    }
}
