use super::{QuantileNumerator, QuantileDenominator};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantile {
    pub numerator: QuantileNumerator,
    pub denominator: QuantileDenominator,
}
