use super::*;

impl_rational_division_ops!(MetricValue);

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

    /// `self` when it is less than `other`, and `other` otherwise.
    pub(crate) fn lesser_of(self, other: Self) -> Self {
        if self.cmp_value(other) == Ordering::Less {
            self
        } else {
            other
        }
    }

    /// `self` when it is greater than `other`, and `other` otherwise.
    pub(crate) fn greater_of(self, other: Self) -> Self {
        if self.cmp_value(other) == Ordering::Greater {
            self
        } else {
            other
        }
    }

    pub(crate) fn cmp_value(self, other: Self) -> Ordering {
        (self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")).cmp(
            &(other.numerator
                * i128::try_from(self.denominator).expect("denominator fits in i128")),
        )
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
}

impl Default for MetricValue {
    fn default() -> Self {
        Self::zero()
    }
}
