use super::MetricValue;

/// Which side of a binary operator a scalar literal was written on.
///
/// `1 > x` and `x > 1` disagree for the ordered operators, and `1 - x` and
/// `x - 1` disagree for the non-commutative arithmetic operators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarSide {
    /// The scalar is the left operand, as in `1 > x`.
    Left,
    /// The scalar is the right operand, as in `x > 1`.
    Right,
}

impl ScalarSide {
    /// The side `krabka_logql`'s `scalar_on_left` flag names.
    #[cfg(test)]
    pub(crate) fn from_scalar_on_left(scalar_on_left: bool) -> Self {
        if scalar_on_left {
            Self::Left
        } else {
            Self::Right
        }
    }
}

/// A sample and the scalar it is combined with, in the order they were
/// written.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarOperands {
    pub(crate) sample: MetricValue,
    pub(crate) scalar: MetricValue,
    pub(crate) scalar_side: ScalarSide,
}

impl ScalarOperands {
    /// The left and the right operand.
    pub(crate) fn left_and_right(self) -> (MetricValue, MetricValue) {
        match self.scalar_side {
            ScalarSide::Left => (self.scalar, self.sample),
            ScalarSide::Right => (self.sample, self.scalar),
        }
    }
}
